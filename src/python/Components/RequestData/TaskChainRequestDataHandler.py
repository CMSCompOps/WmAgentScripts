import re
import copy
from collections import defaultdict

from typing import Optional, Tuple

from Utilities.IteratorTools import filterKeys
from Components.RequestData.BaseChainRequestDataHandler import BaseChainRequestDataHandler


class TaskChainRequestDataHandler(BaseChainRequestDataHandler):
    """
    __TaskChainRequestDataHandler__
    General API for handling the request data of task chain request type
    """

    def _hasAcceptableEfficiency(self) -> bool:
        """
        The function to check if TaskChain has acceptable efficiency
        :return: True if acceptable efficiency, False o/w
        """
        maxCores = self.unifiedConfiguration.get("max_nCores_for_stepchain")

        time = self._getTimeInfo(self)
        totalTimePerEvent, efficiency = 0, 0
        for _, info in time.items():
            totalTimePerEvent += info["timePerEvent"]
            efficiency += info["timePerEvent"] * min(info["cores"], maxCores)

        self.logger.debug("Total time per event for TaskChain: %0.1f", totalTimePerEvent)

        if totalTimePerEvent:
            efficiency /= totalTimePerEvent * maxCores
            self.logger.debug("CPU efficiency of StepChain with %u cores: %0.1f%%", maxCores, efficiency * 100)
            return efficiency > self.unifiedConfiguration.get("efficiency_threshold_for_stepchain")

        return False

    def _getTimeInfo(self) -> dict:
        """
        The function to get the time info for a chain request
        :return: time per event and multicore by task
        """
        time = defaultdict(dict)
        for name, task in filterKeys(self.chainKeys, self.wfSchema).items():
            time[name] = {
                "timePerEvent": task.get("TimePerEvent") / task.get("FilterEfficiency", 1.0),
                "cores": task.get("Multicore", self.get("Multicore")),
            }

        return dict(time)

    def _getTaskSchema(self, task: str) -> dict:
        """
        The function to get the schema for a given task
        :param task: task name
        :return: task schema
        """
        for _, taskValue in filterKeys(self.chainKeys, self.wfSchema).items():
            if taskValue[f"{self.base}Name"] == task:
                return copy.deepcopy(taskValue)

    def isGoodToConvertToStepChain(self, keywords: Optional[list]) -> bool:
        try:
            taskKeys = filter(re.compile(f"^Task").search, self.wfSchema)
            for _, task in filterKeys(taskKeys, self.wfSchema).items():
                if isinstance(task, dict) and task.get("EventStreams", 0) != 0:
                    self.logger.info("Convertion is supported only when EventStreams are zero")
                    return False

            moreThanOneTask = self.get("TaskChain", 0) > 1
            if not moreThanOneTask:
                self.logger.info("There is only one task, not good to convert")
                return False

            allSameTiers = len(set(map(lambda x: x.split("/")[-1], self.get("OutputDatasets", [])))) == 1
            if not allSameTiers:
                self.logger.info("There is more than one tier, not good to convert")
                return False

            allSameArches = len(set(map(lambda x: x[:4], self.getParamList("ScramArch")))) == 1
            if not allSameArches:
                self.logger.info("There is more than one scram arch, not good to convert")
                return False

            allSameCores = len(set(self.getMulticore(details=True))) == 1
            if not allSameCores and not self._hasAcceptableEfficiency():
                self.logger.info("There is more than one core and not acceptable efficiency, not good to convert")
                return False

            processingString = "".join(f"{k}-{v}" for k, v in self.getProcessingString().items())
            foundInTransformKeywords = (
                any(keyword in processingString + self.wf for keyword in keywords) if keywords else True
            )
            if not foundInTransformKeywords:
                self.logger.info("Not found in transform keywords, not good to convert")
                return False

            return True

        except Exception as error:
            self.logger.error("Failed to check if good to convert to step chain")
            self.logger.error(str(error))

    def getEvents(self) -> int:
        return int(self.get("Task1").get("RequestNumEvents"))

    def getBlowupFactors(self, splittings: list) -> Tuple[float, float, float]:
        try:
            maxBlowUp, minChildrenByEvent, rootJobByEvent = 0, 0, 0
            eventsKeys = ["events_per_job", "avg_events_per_job"]

            for task in splittings:
                childrenSize, parentsSize = 0, 0
                for k in eventsKeys:
                    childrenSize = task.get(k, childrenSize)

                parents = [
                    parent
                    for parent in splittings
                    if task["splittingTask"].startswith(parent["splittingTask"])
                    and task["splittingTask"] != parent["splittingTask"]
                ]
                if parents:
                    if not minChildrenByEvent or minChildrenByEvent > childrenSize:
                        minChildrenByEvent = childrenSize

                    for parent in parents:
                        for k in eventsKeys:
                            parentsSize = parent.get(k, parentsSize)

                else:
                    rootJobByEvent = childrenSize

                if childrenSize and parents:
                    blowUp = float(parentsSize) / childrenSize
                    if blowUp > maxBlowUp:
                        maxBlowUp = blowUp

            return minChildrenByEvent, rootJobByEvent, maxBlowUp

        except Exception as error:
            self.logger.error("Failed to get blowup factors")
            self.logger.error(str(error))

    def checkSplittings(self, splittings: dict) -> Tuple[bool, list]:
        # TODO: simplify
        try:
            hold, modifiedSplittings = False, []

            gbSpaceLimit = self.unifiedConfiguration.get("GB_space_limit")
            outputSizeCorrection = self.unifiedConfiguration.get("output_size_correction")

            maxEventsPerLumi = []
            eventsPerLumi, eventsPerLumiInputs = None, None

            for task in splittings:
                taskName = task["taskName"].split("/")[-1]
                taskSchema = self._getTaskSchema(taskName)
                sizePerEvent = taskSchema.get("SizePerEvent", 0)
                for keyword, factor in outputSizeCorrection.items():
                    if keyword in task["taskName"]:
                        sizePerEvent *= factor
                        break

                dataset = taskSchema.get("InputDataset")
                if dataset:
                    eventsPerLumiInputs = self.dbsReader.getDatasetEventsPerLumi(dataset)

                eventsPerLumi = eventsPerLumiInputs or eventsPerLumi
                eventsPerLumi = taskSchema.get("events_per_lumi") or eventsPerLumi

                taskFilterEfficiency = 1.0
                if eventsPerLumi and "events_per_job" in dataset:
                    taskFilterEfficiency = dataset.get("FilterEfficiency")

                    taskEfficiencyFactor = 1.0
                    while "InputTask" in dataset:
                        dataset = self._getTaskSchema(dataset["InputTask"])
                        taskEfficiencyFactor *= dataset.get("FilterEfficiency", 1.0)

                    taskEventsPerLumi = eventsPerLumi * taskEfficiencyFactor

                    timePerEvent = task.get("TimePerEvent")
                    if timePerEvent:
                        timeoutHours = 45.0
                        targetHours = 8.0

                        timePerInputLumi = taskEventsPerLumi * timePerEvent
                        if timePerInputLumi > timeoutHours * 3600:
                            taskMaxEventsPerLumi = int(targetHours * 3600 / timePerEvent)
                            maxEventsPerLumi.append(taskMaxEventsPerLumi / taskEfficiencyFactor)

                            self.logger.info(
                                "The running time of tsk %s if expected to be too large even for one lumi section: %s x %.2f s = %.2f h > %s h. Should go as low as %s.",
                                taskName,
                                taskEventsPerLumi,
                                timePerEvent,
                                timePerInputLumi / 3600.0,
                                timeoutHours,
                                taskMaxEventsPerLumi,
                            )

                    if sizePerEvent:
                        sizePerInputLumi = taskEventsPerLumi * sizePerEvent * taskFilterEfficiency

                        taskMaxEventsPerLumi = int(
                            gbSpaceLimit * (1024 ** 2) / sizePerEvent / taskEfficiencyFactor / taskFilterEfficiency
                        )

                        taskMaxEventsPerJob = int(gbSpaceLimit * (1024 ** 2) / sizePerEvent / taskFilterEfficiency)

                        if sizePerInputLumi > gbSpaceLimit * (1024 ** 2):
                            self.logger.info(
                                "The output size task %s is expected to be too large: %.2f GB > %f GB even for one lumi (effective lumi size is ~%d). Should go as low as %d",
                                taskName,
                                sizePerInputLumi / (1024 ** 2),
                                gbSpaceLimit,
                                taskEventsPerLumi,
                                taskMaxEventsPerLumi,
                            )
                            task["splitParams"]["events_per_job"] = taskMaxEventsPerJob
                            modifiedSplittings.append(task)
                            maxEventsPerLumi.append(taskMaxEventsPerLumi)

            if maxEventsPerLumi:
                if eventsPerLumiInputs:
                    if min(maxEventsPerLumi) < eventsPerLumiInputs:
                        self.logger.critical(
                            "Possible events per lumi of this wf (min(%s)) is smaller than %s evt/lumi of the input dataset",
                            maxEventsPerLumi,
                            eventsPerLumiInputs,
                        )
                        self.logger.critical(
                            "Workflow URL: https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=task_%s",
                            self.getParamList("PrepID")[0],
                        )
                        hold = True
                    else:
                        self.logger.info(
                            "The smallest value of %s is ok compared to %s evt/lumi in the input",
                            maxEventsPerLumi,
                            eventsPerLumiInputs,
                        )
                else:
                    rootSplitting = splittings[0]
                    currentSplitting = rootSplitting.get("splitParams", {}).get("events_per_lumi", None)
                    if currentSplitting and currentSplitting > min(maxEventsPerLumi):
                        rootSplitting["splitParams"]["events_per_lumi"] = min(maxEventsPerLumi)
                        modifiedSplittings.append(rootSplitting)

            return hold, modifiedSplittings

        except Exception as error:
            self.logger.error("Failed to check splittings")
            self.logger.error(str(error))

    def writeDatasetPatternName(self, *elements) -> str:
        try:
            if elements[3] != "v*" and all(element == "*" for element in elements[1:3]):
                return None
            return f"/{elements[0]}/{'-'.join(elements[1:4]/{elements[4]})}"

        except Exception as error:
            self.logger.error("Failed to write dataset pattern name for %s", elements)
            self.logger.error(str(error))
