import random
from logging import Logger
from collections import defaultdict
from time import struct_time, gmtime, mktime, asctime
from pymongo.collection import Collection

from Utilities.Logging import displayTime
from Utilities.DataTools import sortByWakeUpPriority
from Utilities.IteratorTools import filterKeys
from Cache.DataCacheLoader import DataCacheLoader
from Databases.Mongo.MongoClient import MongoClient
from Services.Trello.TrelloClient import TrelloClient
from Services.WMStats.WMStatsReader import WMStatsReader
from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Services.ReqMgr.ReqMgrWriter import ReqMgrWriter

from typing import Optional


class AgentController(MongoClient, TrelloClient):
    """
    __AgentController__
    General API for controlling the agents status
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__(logger=logger)

            self.busyFraction = kwargs.get("busyFraction") or 0.8
            self.idleFraction = kwargs.get("idleFraction") or 0.1
            self.speedDrainingFraction = kwargs.get("speedDrainingFraction") or 0.05
            self.openDrainingThreshold = kwargs.get("openDrainingThreshold") or 100
            self.maxPendingCpus = kwargs.get("maxPendingCpus") or 10000000
            self.wakeUpDraining = kwargs.get("wakeUpDraining") or False
            self.silent = kwargs.get("silent") or False

            self.reqmgr = {"reader": ReqMgrReader(logger=logger), "writer": ReqMgrWriter(logger=logger)}
            self.wmstatsReader = WMStatsReader()
            self.dataCache = DataCacheLoader()

            self.agentsByStatus = defaultdict(set)
            self.agentsByRelease = defaultdict(set)
            self.agentsByMajorRelease = defaultdict(set)
            self.agentsWithDownComponents = set()
            self.isSync = self.syncToProduction()

            if not self.silent:
                if not self.agentsByStatus.get("standby"):
                    self.logger.critical("There is no agent in standby")
                if not self.isSync:
                    self.logger.warning("Failed to properly initialize the agent info")

        except Exception as error:
            raise Exception(f"Error initializing AgentController\n{str(error)}")

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.agentInfo

    def _buildMongoDocument(self, name: str, data: dict, now: struct_time = gmtime()) -> dict:
        document = self.get(name) or {}
        document.update({"name": name, "update": int(mktime(now)), "date": asctime(now)})
        document.update(data)
        return document

    def set(self, name: str, **data) -> None:
        """
        The function to set new data in agent info
        :param name: agent name
        :param data: data arguments
        """
        try:
            super()._set(name, data, name=name)

        except Exception as error:
            self.logger.error("Failed to set agent info")
            self.logger.error(str(error))

    def get(self, name: str) -> dict:
        """
        The function to get all info for a given agent
        :param name: agent name
        :return: agent info
        """
        try:
            return super()._getOne(name=name) or {}

        except Exception as error:
            self.logger.error("Failed to get agent %s info", name)
            self.logger.error(str(error))

    def getAgents(self, **query) -> list:
        """
        The function to get all the agents names
        :param query: optional query params
        :return: agent names
        """
        try:
            return super()._get("name", **query)

        except Exception as error:
            self.logger.error("Failed to get the agents names")
            self.logger.error(str(error))

    def syncToProduction(self) -> bool:
        """
        The function to sync the agent info with production
        :return: True if it is properly sync, False o/w
        """
        try:
            prodAgents = self.wmstatsReader.getProductionAgents()
            if not prodAgents:
                raise Exception("Failed to get production agents")

            self.agentsWithDownComponents.update(
                name for name, agent in prodAgents.items() if agent.get("down_components")
            )

            agentsNames = set(self.getAgents() + prodAgents.keys())
            for name in sorted(agentsNames):
                oldInfo = self.get(name) or {}
                lastestInfo = prodAgents.get(name, {})

                release = lastestInfo.get("agent_version")
                if release:
                    self.set(name, version=release)
                    self.agentsByRelease[release].add(name)
                    self.agentsByMajorRelease[".".join(release.split(".")[:3])].add(name)

                status = self._syncAgentStatus(oldInfo, lastestInfo)
                self.set(name, status=status)
                self.agentsByStatus[status].add(name)

            return True

        except Exception as error:
            self.logger.error("Failed to sync agents info to production")
            self.logger.error(str(error))
            return False

    def syncToTrello(self, acting: bool = False) -> None:
        """
        The function to sync the agent info with Trello board
        :param acting: if True boot/drain/retire agent if needed, do nothing o/w
        """
        try:
            for name in self.getAgents():
                status = self.get(name).get("status")
                trelloCard = self.getCard(name)
                trelloListId = self.getListId(status)

                if trelloListId and trelloCard.get("idList") and trelloListId != trelloCard.get("idList"):
                    if not self.silent:
                        self.logger.info("Mismatch for agent %s", name)

                    drainAgent, newStatus = False, None
                    if trelloCard.get("idList") == self.getListId("draining"):
                        drainAgent, newStatus = True, "draining"
                    elif trelloCard.get("idList") == self.getListId("running"):
                        drainAgent, newStatus = False, "running"
                    elif trelloCard.get("idList") == self.getListId("standby"):
                        drainAgent, newStatus = True, "standby"
                    elif trelloCard.get("idList") == self.getListId("offline"):
                        drainAgent, newStatus = True, "offline"
                    else:
                        if not self.silent:
                            self.logger.info("Cannot reconize Trello status %s", trelloCard.get("idList"))
                        continue

                    if acting:
                        updateSucceeded = self._updateAgentConfig(name, {"UserDrainMode": drainAgent})
                        if updateSucceeded:
                            self.set(name, status=newStatus)
                            if not self.silent:
                                self.logger.critical("Putting agent %s in %s", name, newStatus)

        except Exception as error:
            self.logger.error("Failed to sync agents info to Trello")
            self.logger.error(str(error))

    def updateTrelloBoard(self, acting: bool = False) -> None:
        """
        The function to update the agent info in Trello board
        :param acting: if True boot/drain/retire agent if needed, do nothing o/w
        """
        try:
            for name in self.manipulated():
                status = self.get(name).get("status")
                trelloCard = self.getCard(name)
                trelloListId = self.getListId(status)

                if trelloListId and trelloCard.get("idList") and trelloListId != trelloCard.get("idList"):
                    if not self.silent:
                        self.logger.info("Mismatch for agent %s", name)

                    if acting:
                        if not self.silent:
                            self.logger.info("Setting agent %s into list %s", name, status)
                        self.setList(name, status)

        except Exception as error:
            self.logger.error("Failed to update agents info in Trello board")
            self.logger.error(str(error))

    def poll(self, acting: bool = False) -> bool:
        """
        The function to poll the agent info
        :param acting: if True boot/drain/retire agent if needed, do nothing o/w
        :return: True if succeeded, False o/w
        """
        try:
            if not self.isSync:
                raise Exception("Cannot poll the agents without fresh info about them")

            agentsPool = self.dataCache.get("gwmsmon_pool")
            if not agentsPool:
                raise Exception("Agents pool is empty")

            self._initializePollParams()
            self._countJobsAndCpus("running", agentsPool)
            self._countJobsAndCpus("idle", agentsPool)

            self._setCandidatesForWakingUp(agentsPool)
            self._setCandidatesForDraining(agentsPool)
            self._setCandidatesForOpenDraining(agentsPool)
            self._setCandidatesForSpeedDraining(agentsPool)

            self._tell(agentsPool)
            if not acting:
                if not self.silent:
                    self.logger.info("The polling is not proactive")
                return True

            if self._areRunningOverThreshold(agentsPool):
                self._bootAgent(agentsPool)
            elif self._newRelease():
                self._drainAgent()
            elif self.cpus["idle"] > self.maxPendingCpus:
                self._retireAgent()
            elif not self.silent:
                self.logger.info("Everything is fine. No need to retire or add an agent")
            self._setSpeedDrainPriority()

            self.updateTrelloBoard(acting)
            self.syncToTrello(acting)
            self.syncToProduction()

        except Exception as error:
            self.logger.error("Failed to poll agents")
            self.logger.error(str(error))
            return False

    def _syncAgentStatus(self, oldInfo: dict, lastestInfo: dict) -> str:
        """
        The function to sync the agent status from its given info
        :param oldInfo: info from last agent update
        :param lastestInfo: info from production
        :return: status, i. e. standby, draining, offline or running
        """
        if oldInfo and lastestInfo:
            if lastestInfo["drain_mode"]:
                if oldInfo["status"] == "standby" or (
                    oldInfo.get("version") and oldInfo.get("version") != lastestInfo.get("agent_version")
                ):
                    return "standby"
                return "draining"
            return "running"

        if oldInfo:
            return "offline"

        if lastestInfo["drain_mode"]:
            if not self.silent:
                self.logger.info("There is a new agent in the pool: %s, setting to standby", oldInfo.get("name"))
            return "standby"

        return "running"

    def _initializePollParams(self, now: struct_time = gmtime()) -> None:
        """
        The function to initialize params for polling
        :param now: time now
        """
        self._setLastestAndOldestReleases()
        self._setLastestAndOldestReleases(major=True)

        self.releaseStatus = defaultdict(dict)
        self._countAgentsWithReleaseStatus("running", "lastest")
        self._countAgentsWithReleaseStatus("running", "oldest")
        self._countAgentsWithReleaseStatus("standby", "lastest")

        self.recentAgents = defaultdict(set)
        self.timeoutAgent = defaultdict(float)
        self._setRecentAgents("running", now)
        self._setRecentAgents("standby", now)
        self._setRecentAgents("draining", now)

        self.jobs = defaultdict(int)
        self.cpus = defaultdict(int)

        self.candidates = defaultdict(set)
        self.manipulated = set()

    def _setLastestAndOldestReleases(self, major: bool = False) -> None:
        """
        The function to set the agents in lastest and oldest releases
        :param major: if True use major releases dict, releases dict o/w
        """
        agentsByRelease = self.agentsByMajorRelease if major else self.agentsByRelease
        releases = [(r, map(lambda x: int(x.replace("patch", "")), r.split("."))) for r in agentsByRelease]
        releases = sorted(releases, key=lambda r: r[1], reverse=True)

        agentsByRelease["lastest"] = agentsByRelease[releases[0]]
        agentsByRelease["oldest"] = agentsByRelease[releases[-1]] if releases[0] != releases[-1] else {}

    def _countAgentsWithReleaseStatus(self, status: str, release: str) -> None:
        """
        The function to count agents with a given status in a given release version
        :param status: status name
        :param release: release version
        """
        agents = set(self.agentsByStatus.get(status, []))
        agents &= self.agentsByRelease[release]
        self.releaseStatus[release][status] = len(agents)

    def _setRecentAgents(self, status: str, now: struct_time) -> None:
        """
        The function to set the agents recently updated to a given status
        :param status: status name
        :param now: time now
        """
        lastActionTimeout = 18000
        lastActionStatic = 72000
        for name in self.agentsByStatus.get(status, []):
            lastUpdate = self.get(name).get("update")
            timeSinceLastUpdate = mktime(now) - lastUpdate

            if timeSinceLastUpdate < lastActionStatic:
                self.recentAgents[status].add(name)

            if timeSinceLastUpdate < lastActionTimeout:
                timeout = lastActionTimeout - timeSinceLastUpdate
                if self.timeoutAgent[status] < timeout:
                    self.timeoutAgent[status] = timeout

    def _countJobsAndCpus(self, status: str, agents: dict) -> None:
        """
        The function to count jobs and cpus being run by agents with a given status
        :param agents: agents data
        :para status: status name
        """
        for agent in agents.values():
            self.jobs[status] += agent.get(f"Total{status.title()}Jobs", 0)
            self.cpus[status] += agent.get(f"Total{status.title()}Cpus", 0)

    def _setCandidatesForWakingUp(self, agents: dict) -> None:
        """
        The function to set agent candidates for waking up
        :param agents: agents info
        """
        candidates = set(self.agentsByStatus.get("draining", []))
        candidates &= set(agents.keys())
        candidates -= set(self.recentAgents.get("draining", []))
        candidates -= set(self.recentAgents.get("standby", []))
        candidates -= self.agentsWithDownComponents
        for name, agent in filterKeys(candidates, agents).items():
            if name not in self.agentsByRelease["oldest"] and not self._isStuffed(agent) and not self._isLight(agent):
                self.candidates["wakeUp"].add(name)

    def _setCandidatesForDraining(self, agents: dict) -> None:
        """
        The function to set agent candidates for draining
        :param agents: agents info
        """
        candidates = set(self.agentsByStatus.get("running", []))
        candidates &= set(agents.keys())
        candidates -= set(self.recentAgents.get("running", []))
        for name in candidates:
            if name not in self.agentsByRelease["lastest"] and name not in self.agentsByMajorRelease["lastest"]:
                self.candidates["drain"].add(name)

    def _setCandidatesForOpenDraining(self, agents: dict) -> None:
        """
        The function to set agent candidates for open draining
        :param agents: agents info
        """
        if len(self.agentsByStatus.get("standby", [])) <= 1:
            candidates = set(self.agentsByStatus.get("draining", []))
            candidates &= set(agents.keys())
            for name, agent in filterKeys(candidates, agents).items():
                if agent.get("TotalRunningJobs", 0) + agent.get("MaxJobsRunning", 0) <= self.openDrainingThreshold:
                    self.candidates["openDrain"].add(name)

    def _setCandidatesForSpeedDraining(self, agents: dict) -> None:
        """
        The function to set agent candidates for speed draining
        :param agents: agents info
        """
        runningThreshold = self.cpus["running"] * self.speedDrainingFraction

        candidates = set(self.agentsByStatus.get("draining", []))
        candidates &= set(agents.keys())
        for name, agent in filterKeys(candidates, agents).items():
            if (
                agent.get("TotalIdleCpus", 0) <= runningThreshold
                and agent.get("TotalRunningCpus", 0) <= runningThreshold
                and agent.get("TotalRunningJobs", 0) <= agent.get("MaxJobsRunning", 0) * self.speedDrainingFraction
            ):
                self.candidates["speedDrain"].add(name)

    def _isStuffed(self, agent: dict) -> bool:
        """
        The function to check if an agent is stuffed, i. e. running more jobs than it should
        :param agents: agents info
        :return: True if stuffed, False o/w
        """
        runningThreshold = agent.get("MaxJobsRunning", 0) * self.busyFraction
        return agent["TotalRunningJobs"] >= runningThreshold

    def _isLight(self, agent: dict) -> bool:
        """
        The function to check if an agent is light, i. e. running less jobs than it should
        :param agents: agents info
        :return: True if light, False o/w
        """
        idleThreshold = agent.get("MaxJobsRunning", 0) * self.idleFraction
        return agent.get("TotalRunningJobs", 0) <= idleThreshold

    def _tell(self, agents: dict) -> None:
        """
        The function to print information about the agents
        :param agents: agents info
        """
        if self.silent:
            return

        self.logger.info("Agent releases: %s", sorted(self.agentsByRelease.keys()))
        self.logger.info("Agent major releases: %s", sorted(self.agentsByMajorRelease.keys()))
        self.logger.info("Running lastest release: %s", self.releaseStatus["lastest"].get("running"))
        self.logger.info("Standby in lastest release: %s", self.releaseStatus["lastest"].get("standby"))
        self.logger.info("Running in oldest release: %s", self.releaseStatus["oldest"].get("running"))
        self.logger.info("Running capacity: %s", self._sumRunningCapacity(agents))
        self.logger.info("Running jobs: %s", self.jobs["running"])
        self.logger.info("Running cpus: %s", self.cpus["running"])
        self.logger.info("Pending jobs: %s", self.jobs["idle"])
        self.logger.info("Pending cpus: %s", self.cpus["idle"])
        self.logger.info("These are candidates for draining: %s", sorted(self.candidates["drain"]))
        self.logger.info("These are good for speed draining: %s", sorted(self.candidates["speedDrain"]))
        self.logger.info("These are good for open draining: %s", sorted(self.candidates["openDrain"]))

        if not any(self.timeoutAgent.values()):
            if self._areRunningOverThreshold(agents):
                self.logger.critical("All agents are maxing out. New agent is needed")
            if self.cpus["idle"] > self.maxPendingCpus:
                self.logger.critical(
                    "There is more than %s cpus pending (%s). An agent needs to be set aside",
                    self.maxPendingCpus,
                    self.cpus["idle"],
                )
            if self._newRelease():
                self.logger.critical(
                    "There is a new release. Starting to drain other agents from %s",
                    sorted(self.candidates["drain"]),
                )
        else:
            self.logger.info(
                "An agent was recently put in running/draining/standby. Cannot do any furthuer acting for another %s last running or %s last standby",
                displayTime(max(self.timeoutAgent.values())),
                displayTime(self.timeoutAgent["standby"]),
            )

    def _sumRunningCapacity(self, agents: dict) -> int:
        """
        The function to get the running capacity of the given agents
        :param agents: agents info
        :running: number of jobs
        """
        running = self.agentsByStatus.get("running", [])
        running &= set(agents.keys())
        return sum([agent.get("MaxJobsRunning", 0) for agent in filterKeys(running, agents).values()])

    def _areRunningOverThreshold(self, agents: dict) -> bool:
        """
        The function to check if the agents are running over the threshold
        :param agents: agents info
        :return: True if running over threshold, False o/w
        """
        running = self.agentsByStatus.get("running", [])
        running &= set(agents.keys())
        return all(self._isStuffed(agent) for agent in filterKeys(running, agents).values())

    def _newRelease(self) -> bool:
        """
        The function to check if there was a new production release or not
        """
        return any(
            self.releaseStatus["lastest"].get(status, 0) > 1 for status in ["running", "standby"]
        ) and self.releaseStatus["oldest"].get("running", 0)

    def _bootAgent(self, agents: dict) -> None:
        """
        The function to boot an agent from candidates
        :param agents: agents info
        """
        candidates = self.agentsByStatus.get("standby", [])
        if not candidates and self.wakeUpDraining:
            candidates = self.candidates["wakeUp"] or self.agentsByStatus.get("draining", [])
            candidates = sortByWakeUpPriority(filterKeys(candidates, agents))[:1]

        if candidates:
            wakeUp = random.choice(candidates)
            self.candidates["speedDrain"].discard(wakeUp)
            self.candidates["openDrain"].discard(wakeUp)
            bootSucceeded = self._updateAgentConfig(wakeUp, {"UserDrainMode": False})
            if bootSucceeded:
                self.manipulated.add(wakeUp)
                self.set(wakeUp, status="running")
                if not self.silent:
                    self.logger.critical("Putting agent %s in production", wakeUp)
        elif not self.silent:
            self.logger.critical("A new agent is needed in the pool, but none is available")

    def _drainAgent(self) -> None:
        """
        The function to drain an agent from candidates
        """
        if self.candidates["drain"]:
            sleepUp = random.choice(list(self.candidates.get("drain", [])))
            drainSucceeded = self._updateAgentConfig(sleepUp, {"UserDrainMode": True})
            if drainSucceeded:
                self.manipulated.add(sleepUp)
                self.set(sleepUp, status="draining")
                if not self.silent:
                    self.logger.critical("Putting agent %s in drain mode", sleepUp)
        elif not self.silent:
            self.logger.critical("Agents need to be set in drain mode, but none is available")

    def _retireAgent(self) -> None:
        """
        The function to retire an agent from candidates
        """
        sleepUp = random.choice(list(self.agentsByStatus.get("running", [])))
        retireSucceeded = self._updateAgentConfig(sleepUp, {"UserDrainMode": True})
        if retireSucceeded:
            self.manipulated.add(sleepUp)
            self.set(sleepUp, status="standby")
            if not self.silent:
                self.logger.critical("Putting agent %s in standby", sleepUp)

    def _updateAgentConfig(self, agent: str, config: dict) -> bool:
        """
        The function to update an agent configuration when booting/draining/retiring the agent
        :param agent: agent name
        :param config: new agent config params
        :return: True if succeeded, False o/w
        """
        return self.reqmgr["writer"].setAgentConfig(agent, {**self.reqmgr["reader"].getAgentConfig(agent), **config})

    def _setSpeedDrainPriority(self) -> None:
        """
        The function to set the speed drain priority of agents
        """
        priorityDrain = set(self.getAgents(speeddrain=True))
        if priorityDrain & set(self.candidates.get("speedDrain", [])):
            priorityDrain &= set(self.candidates.get("speedDrain", []))
        else:
            priorityDrain.update(random.shuffle(list(self.candidates.get("speedDrain", [])))[:1])

        priorityDrain.update(self.candidates.get("openDrain", []))
        for name in self.getAgents():
            self.set(name, speeddrain=name in priorityDrain)
