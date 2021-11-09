import re
from logging import Logger
from collections import defaultdict

from Utilities.IteratorTools import filterKeys, mapValues, sortByKeys
from WorkflowMgmt.WorkflowSchemaHandlers.BaseWfSchemaHandler import BaseWfSchemaHandler

from typing import Optional, Union, Any, Tuple, Callable


class StepChainWfSchemaHandler(BaseWfSchemaHandler):
    """
    __StepChainWfSchemaHandler__
    General API for handling the request data of step chain request type
    """

    def __init__(self, wfSchema: dict, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__(wfSchema, logger=logger)
            self.includeHEPCloudInSiteWhiteList = True

            self.base = self.wfSchema["RequestType"].replace("Chain", "")
            self.chainKeys = sorted(filter(re.compile(f"^{self.base}\d+$").search, self.wfSchema))

            self.logMsg = {
                "reduceLargeOutput": "The output size of task is expected to be too large : %d x %.2f kB = %.2f GB > %f GB. Reducing to %d"
            }

        except Exception as error:
            raise Exception(f"Error initializing StepChainWfSchemaHandler\n{str(error)}")

    def getChainValues(self, key: str, f: Optional[Callable] = None) -> dict:
        """
        The function to get the values in the chain for a given key
        :param key: key name
        :param f: optional function to apply on the value
        :return: values of the chain items
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
        """
        The function to get the workflow acquisition era
        :return: acquisition era
        """
        return self.getChainValues("AcquisitionEra")

    def getProcessingString(self) -> dict:
        """
        The function to get the workflow processing string
        :return: processing string
        """
        return self.getChainValues("ProcessingString")

    def getMemory(self) -> Optional[float]:
        """
        The function to get the workflow memory
        :return: memory value if any, None o/w
        """
        try:
            memory = list(self.getChainValues("Memory").values())
            return max(map(float, filter(None, memory))) if memory else float(self.get("Memory"))

        except Exception as error:
            self.logger.error("Failed to get the workflow memory")
            self.logger.error(str(error))

    def getIO(self) -> Tuple[bool, set, set, set]:
        """
        The function to get the inputs/outputs
        :return: if any lhe input file, primaries, parents and secondaries
        """
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

    def getMulticore(self, maxOnly: bool = True) -> Union[int, list]:
        """
        The function to get the workflow multicore
        :param maxOnly: if True return max multicore, o/w return list of multicore values
        :return: multicore
        """
        try:
            multicores = list(self.getChainValues("Multicore").values())

            if maxOnly:
                return max(map(int, filter(None, multicores))) if multicores else self.get("Multicore")
            return multicores if multicores else [self.get("Multicore")]

        except Exception as error:
            self.logger.error("Failed to get the workflow multicore")
            self.logger.error(str(error))

    def getCampaigns(self, details: bool = True) -> Union[dict, list]:
        """
        The function to get the workflow campaigns
        :param details: if True and if the request type is a chain it returns details of campaigns, o/w just campaigns names
        :return: campaigns
        """
        try:
            campaigns = self.getChainValues("Campaign" if self.isRelVal() else "AcquisitionEra")

            if details:
                return campaigns

            return list(set(campaigns.values()))

        except Exception as error:
            self.logger.error("Failed to get workflow campaigns")
            self.logger.error(str(error))

    def getCampaignsAndLabels(self) -> list:
        """
        The function to get a list of campaigns and labels
        :return: a list of tuples containing campaign name and processing string
        """
        try:
            campaigns = self.getCampaigns()
            processingStrings = self.getProcessingString()

            return [(campaigns[k], v) for k, v in processingStrings.items()]

        except Exception as error:
            self.logger.error("Failed to get workflow campaigns and labels")
            self.logger.error("Available processing string: %s", processingStrings)
            self.logger.error("Available campaigns: %s", campaigns)
            self.logger.error(str(error))

    def getLumiWhiteList(self) -> dict:
        """
        The function to get the workflow's lumi white list
        :return: lumi white list
        """
        try:
            lumiList = {}
            values = list(self.getChainValues("LumiList").values())
            for value in values:
                lumiList.update(value)

            return lumiList

        except Exception as error:
            self.logger.error("Failed to get Lumi White List")
            self.logger.error(str(error))

    def getParamList(self, key: str) -> list:
        """
        The function to get the workflow's param list for a given key
        :param key: key name
        :return: values list
        """
        try:
            values = list(self.getChainValues(key).values())
            if values and isinstance(values[0], str):
                return list(set(values))
            return list(set(item for lst in values for item in lst))

        except Exception as error:
            self.logger.error("Failed to get %s", key)
            self.logger.error(str(error))

    def getParamByTask(self, key: str, task: str) -> Any:
        """
        The function to get a param value for a given task
        :param key: key name
        :param task: task name
        :return: value
        """
        return self.getChainValues(key).get(task, self.get(key))

    def getExpectedEventsPerTask(self) -> dict:
        """
        The function to get the number of expected events by task
        :return: expected events by task
        """
        try:
            eventsExpectedPerTask = {}
            eventsExpected = self.get("TotalInputEvents", 0)

            values = {}
            for _, task in filterKeys(self.chainKeys, self.wfSchema).items():
                values[task[f"{self.base}Name"]] = task
                if f"Input{self.base}" not in task and "RequestNumEvents" in task:
                    eventsExpected = task["RequestNumEvents"]

            for _, task in filterKeys(self.chainKeys, self.wfSchema).items():
                taskName = task[f"{self.base}Name"]
                eventsExpectedPerTask[taskName] = eventsExpected
                nestedTask = task
                while f"Input{self.base}" in nestedTask:
                    eventsExpectedPerTask[taskName] *= nestedTask.get("FilterEfficiency", 1.0)
                    nestedTask = values.get(nestedTask[f"Input{self.base}"])

            return eventsExpectedPerTask

        except Exception as error:
            self.logger.error("Failed to get expected events")
            self.logger.error(str(error))

    def getOutputDatasetsPerTask(self, workTasks) -> dict:
        """
        The function to get the output datasets by task
        :param workTasks: work tasks
        :return: a dict of dataset names by task names
        """
        try:
            outputPerTask = defaultdict(set)
            for task, data in self.get("ChainParentageMap", {}).items():
                if data.get("ChildDsets"):
                    outputPerTask[task].update(data.get("ChildDsets"))

            return mapValues(list, outputPerTask)

        except Exception as error:
            self.logger.error("Failed to get output datasets by task")
            self.logger.error(str(error))

    def getComputingTime(self) -> int:
        """
        The function to get the computing time (in seconds)
        :return: computing time
        """
        try:
            cpuTime, cacheTime = 0, {}

            for _, task in sortByKeys(filterKeys(self.chainKeys, self.wfSchema)).items():
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

    def checkSplittings(self, splittings: dict) -> Tuple[bool, list]:
        """
        The function to check the splittings sizes and if any action is required
        :param splittings: splittings schema
        :return: if to hold and a list of modified splittings
        """
        try:
            modifiedSplittings = []

            nCores = self.getMulticore()
            totalGbSpaceLimit = nCores * self.unifiedConfiguration.get("GB_space_limit")

            sizePerEvent = self.wfSchema.get("SizePerEvent", 0) / (1024.0 ** 2.0)
            for splitting in splittings:
                params = splitting.get("splitParams", {})

                eventsPerJob = params.get("events_per_job", 0)
                size = eventsPerJob * sizePerEvent

                if size > totalGbSpaceLimit:
                    maxTaskEventsPerJob = int(totalGbSpaceLimit / sizePerEvent)
                    splitting["splitParams"]["events_per_job"] = maxTaskEventsPerJob
                    modifiedSplittings.append(splitting)

                    self.logger.info(
                        self.logMsg["reduceLargeOutput"],
                        eventsPerJob,
                        sizePerEvent,
                        size,
                        totalGbSpaceLimit,
                        maxTaskEventsPerJob,
                    )

            return False, modifiedSplittings

        except Exception as error:
            self.logger.error("Failed to check splittings")
            self.logger.error(str(error))
