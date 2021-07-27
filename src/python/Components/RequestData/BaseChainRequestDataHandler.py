import re
from logging import Logger
from collections import defaultdict

from typing import Optional, Union, Any, Tuple, Callable

from Utilities.IteratorTools import filterKeys
from Components.RequestData.BaseRequestDataHandler import BaseRequestDataHandler


class BaseChainRequestDataHandler(BaseRequestDataHandler):
    """
    __BaseChainRequestDataHandler__
    General Abstract Base Class for building the concrete request data handlers based on the chainrequest type
    """

    def __init__(self, wfSchema: dict, logger: Optional[Logger]) -> None:
        try:
            super().__init__(wfSchema, logger=logger)
            self.base = self.wfSchema["RequestType"].replace("Chain", "")
            self.chainKeys = filter(re.compile(f"^{self.base}\d+$").search, self.wfSchema)

        except Exception as error:
            raise Exception(f"Error initializing {self.__class__.__name__}\n{str(error)}")

    def getChainValues(self, key: str, f: Optional[Callable] = None) -> dict:
        """
        The function to get the values in the chain
        :param key: key name
        :param f: optional function to apply on the value
        :return: a dict of value of the chain items
        """
        try:
            values = {}
            for _, task in filterKeys(self.chainKeys, self.wfSchema).items():
                if key in task:
                    value = task[key]
                    values[task[f"{self.base}Name"]] = f(value) if f else value

            return values

        except Exception as error:
            self.logger.error("Failed to get values for %s", key)
            self.logger.error(str(error))

    def getAcquisitionEra(self) -> dict:
        return self.getChainValues("AcquisitionEra")

    def getProcessingString(self) -> dict:
        return self.getChainValues("ProcessingString")

    def getMemory(self) -> float:
        try:
            memory = list(self.getChainValues("Memory").values())
            return max(map(float, filter(None, memory))) if memory else None

        except Exception as error:
            self.logger.error("Failed to get the workflow memory")
            self.logger.error(str(error))

    def getIO(self) -> Tuple[bool, list, list, list]:
        try:
            lhe, primaries, parents, secondaries = False, set(), set(), set()
            for _, task in filterKeys(self.chainKeys, self.wfSchema).items():
                taskLhe, taskPrimaries, taskParents, taskSecondaries = self._getTaskIO(task)
                lhe |= taskLhe
                primaries.update(taskPrimaries)
                parents.update(taskParents)
                secondaries.update(taskSecondaries)

            return lhe, primaries, parents, secondaries

        except Exception as error:
            self.logger.error("Failed to get I/O")
            self.logger.error(str(error))

    def getMulticore(self, details: bool = False) -> Union[int, list]:
        try:
            multicores = list(self.getChainValues("Multicore").values())

            if details:
                return multicores
            return max(map(int, filter(None, multicores)))

        except Exception as error:
            self.logger.error("Failed to get the workflow multicore")
            self.logger.error(str(error))

    def getCampaigns(self, details: bool = True) -> Union[dict, list]:
        try:
            campaigns = self.getChainValues("Campaign" if self.isRelval() else "AcquisitionEra")

            if details:
                return campaigns
            return list(set(campaigns.values()))

        except Exception as error:
            self.logger.error("Failed to get workflow campaigns")
            self.logger.error(str(error))

    def getCampaignsAndLabels(self) -> list:
        try:
            campaigns = self.getCampaigns()
            processingStrings = self.getProcessingString()

            return [(campaigns[k], v) for k, v in processingStrings.items()]

        except Exception as error:
            self.logger.error("Failed to get workflow campaigns and labels")
            self.logger.error("Available processing string: %s", processingStrings)
            self.logger.error("Available campaigns: %s", campaigns)
            self.logger.error(str(error))

    def getParamList(self, key: str) -> list:
        return list(set(item for lst in self.getChainValues(key).values() for item in lst))

    def getParamByTask(self, key: str, task: str) -> Any:
        return self.getChainValues(key).get(task, self.get(key))

    def getExpectedEventsByTask(self) -> dict:
        try:
            eventsExpectedByTask = {}
            eventsExpected = self.get("TotalInputEvents", 0)

            values = {}
            for _, task in filterKeys(self.chainKeys, self.wfSchema).items():
                values[task[f"{self.base}Name"]] = task
                if f"Input{self.base}" not in task and "RequestNumEvents" in task:
                    eventsExpected = task["RequestNumEvents"]

            for _, task in filterKeys(self.chainKeys, self.wfSchema).items():
                taskName = task[f"{self.base}Name"]
                eventsExpectedByTask[taskName] = eventsExpected
                nestedTask = task
                while f"Input{self.base}" in nestedTask:
                    eventsExpectedByTask[taskName] *= nestedTask.get("FilterEfficiency", 1.0)
                    nestedTask = self.get(values[nestedTask[f"Input{self.base}"]])

            return eventsExpectedByTask

        except Exception as error:
            self.logger.error("Failed to get expected events")
            self.logger.error(str(error))

    def getOutputDatasetsByTask(self, _) -> dict:
        try:
            outputByTask = defaultdict(set)
            for task, data in self.get("ChainParentageMap", {}).items():
                outputByTask[task].update(data.get("ChildDsets", []))

            return dict(outputByTask)

        except Exception as error:
            self.logger.error("Failed to get output datasets by task")
            self.logger.error(str(error))

    def getComputingTime(self) -> int:
        try:
            cpuTime, cacheTime = 0, {}
            for _, task in filterKeys(self.chainKeys, self.wfSchema).items():
                if task.get("BlockWhiteList"):
                    events, _ = self.dbsReader.getBlocksEventsAndLumis(task["BlockWhiteList"])
                elif task.get("InputDataset"):
                    events, _ = self.dbsReader.getDatasetEventsAndLumis(task["InputDataset"])
                elif task.get(f"Input{self.base}") and task[f"Input{self.base}"] in cacheTime:
                    events = cacheTime[task[f"Input{self.base}"]]
                elif task.get("RequestNumEvents"):
                    events = float(task["RequestNumEvents"])
                else:
                    self.logger.info("Cannot get the number of events, considering cpu time as zero")
                    continue

                cacheTime[task[f"{self.base}Name"]] = events
                if task.get("FilterEfficiency"):
                    cacheTime[task[f"{self.base}Name"]] *= task["FilterEfficiency"]

                cpuTime += events * task.get("TimePerEvent", 1)

            return cpuTime

        except Exception as error:
            self.logger.error("Failed to get the computing time")
            self.logger.error(str(error))
