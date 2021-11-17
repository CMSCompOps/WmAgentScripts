import re
import copy
from logging import Logger
from collections import defaultdict
from typing import Optional, Tuple, List, Any
import traceback

from Utilities.IteratorTools import filterKeys
from WorkflowMgmt.WorkflowSchemaHandlers.StepChainWfSchemaHandler import StepChainWfSchemaHandler


class TaskChainWfSchemaHandler(StepChainWfSchemaHandler):
    """
    __TaskChainWfSchemaHandler__
    General API for handling the request data of task chain request type
    """

    def __init__(self, wfSchema: dict, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__(wfSchema, logger=logger)

            self.logMsg = {
                "wfUrl": "Workflow URL: https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=task_%s",
                "smallEventsPerLumi": "Possible events per lumi of this wf (min(%s)) is smaller than %s evt/lumi of the input dataset",
                "smallEventsPerLumi": "Task will get %s events per lumi in output. Smaller than %s is troublesome.",
                "largeOutputSize": "The output size task is expected to be too large: %.2f GB > %f GB even for one lumi (effective lumi size is ~%d). It should go as low as %d",
                "largeOutputTime": "The running time of task is expected to be too large even for one lumi section: %s x %.2f s = %.2f h. It should go as low as %s",
                "reduceLargeOutput": "The output size of task is expected to be too large : %d x %.2f kB * %.4f = %.2f GB > %f GB. Reducing to %d",
                "diffMulticoreConversion": "The conversion to StepChain encoutered different value of Multicore: %s != %s"
            }

        except Exception as error:
            raise Exception(f"Error initializing WorkflowController\n{str(error)}")

    def _isBelowMinEventsPerLumi(self, eventsPerLumi: int) -> bool:
        """
        The function to check if given number of events per lumi is below the min allowed value
        :param: number of events per lumi
        :return: True if below min, False o/w
        """
        minEventsPerLumi = self.unifiedConfiguration.get("min_events_per_lumi_output")

        if eventsPerLumi < minEventsPerLumi:
            self.logger.critical(self.logMsg["smallEventsPerLumi"], eventsPerLumi, minEventsPerLumi)
            self.logger.critical(self.logMsg["wfUrl"], self.getParamList("PrepID")[0])

            return True

        return False

    def _hasAcceptableEfficiency(self) -> bool:
        """
        The function to check if the request has acceptable efficiency
        :return: True if acceptable efficiency, False o/w
        """
        maxCores = self.unifiedConfiguration.get("max_nCores_for_stepchain")

        time = self._getTimeInfo()
        totalTimePerEvent, efficiency = 0, 0
        for _, info in time.items():
            totalTimePerEvent += info["timePerEvent"]
            efficiency += info["timePerEvent"] * min(info["cores"], maxCores)

        self.logger.info("Total time per event for TaskChain: %0.1f", totalTimePerEvent)

        if totalTimePerEvent:
            efficiency /= totalTimePerEvent * maxCores
            self.logger.info("CPU efficiency of StepChain with %u cores: %0.1f%%", maxCores, efficiency * 100)
            return efficiency > self._getStepchainConversionThreshold()

        return False

    def _getStepchainConversionThreshold(self) -> float:

        try:

            priority = self.get("RequestPriority")
            if priority >= self.unifiedConfiguration.get("block1_priority"):
                return self.unifiedConfiguration.get("efficiency_threshold_for_stepchain_high_priority")
            else:
                return self.unifiedConfiguration.get("efficiency_threshold_for_stepchain_low_priority")

        except Exception as error:
            self.logger.error("Failed to get the stepchain conversion threshold")
            self.logger.error(str(error))


    def _hasNonZeroEventStreams(self) -> bool:
        """
        The function to check if the request has non-zero event streams.
        :return: True if it has non-zero event streams, False o/w
        """
        taskKeys = [*filter(re.compile(f"^Task").search, self.wfSchema)]
        for _, task in filterKeys(taskKeys, self.wfSchema).items():
            if isinstance(task, dict) and task.get("EventStreams", 0) != 0:
                return True
        return False

    def _getTaskEfficiencyFactor(self, schema: dict, efficiencyFactor: float = 1.0) -> float:
        """
        The function to get the efficiency factor for a given task
        :param schema: task schema
        :param efficiencyFactor: starting efficiency factor
        :return: efficiency factor
        """
        if "InputTask" in schema:
            dataset = self._getTaskSchema(schema["InputTask"])
            efficiencyFactor *= dataset.get("FilterEfficiency", 1.0)
            return self._getTaskEfficiencyFactor(dataset, efficiencyFactor)

        return efficiencyFactor

    def _getTaskSchema(self, task: str) -> dict:
        """
        The function to get the schema for a given task
        :param task: task name
        :return: task schema
        """
        for _, schema in filterKeys(self.chainKeys, self.wfSchema).items():
            if schema[f"{self.base}Name"] == task:
                return copy.deepcopy(schema)
        return {}

    def _getTaskMaxEventsPerLumiAndJob(self, schema: dict, eventsPerLumi: int) -> Tuple[list, Optional[int]]:
        """
        The function to get the max number of events per lumi and per job for a given task
        :param schema: task schema
        :param eventsPerLumi: base events per lumi
        :return: list of max events per lumi by time and by size, and max events per job
        """
        maxEventsPerLumiByTime = self._getTaskMaxEventsByTime(schema, eventsPerLumi)
        maxEventsPerLumiBySize, maxEventsPerJob = self._getTaskMaxEventsBySize(schema, eventsPerLumi)
        return [*filter(None, [maxEventsPerLumiByTime, maxEventsPerLumiBySize])], maxEventsPerJob

    def _getTaskMaxEventsByTime(self, schema: dict, eventsPerLumi: int) -> Optional[int]:
        """
        The function to get the max events per lumi allowed by time params for a given task,
        considering a set number of events per lumi and time per event
        :param schema: task schema
        :param eventsPerLumi: base events per lumi
        :return: max events per lumi if base events per lumi will last longer than timoeout, None o/w
        """
        timeoutHours, targetHours = 45.0, 8.0

        efficiencyFactor = self._getTaskEfficiencyFactor(schema)
        eventsPerLumi *= efficiencyFactor
        timePerEvent = schema.get("TimePerEvent", 0)

        time = eventsPerLumi * timePerEvent
        if time > timeoutHours * 3600:
            maxEventsPerLumi = int(targetHours * 3600) / timePerEvent

            self.logger.info(
                self.logMsg["largeOutputTime"],
                eventsPerLumi,
                timePerEvent,
                time,
                maxEventsPerLumi,
            )

            return maxEventsPerLumi / efficiencyFactor

        return None

    def _getTaskMaxEventsBySize(self, schema: dict, eventsPerLumi: int) -> Tuple[Optional[int], Optional[int]]:
        """
        The function to get the max events per lumi allowed by size params for a given task,
        considering a set number of events per lumi and size per event
        :param schema: task schema
        :param eventsPerLumi: base events per lumi
        :return: max events per lumi if base events per lumi are larger than limit space, and
        max events per job if the output size is larger than limit space
        """
        gbSpaceLimit = self.unifiedConfiguration.get("GB_space_limit")

        sizePerEvent = schema.get("SizePerEvent", 0)
        if not sizePerEvent:
            return None

        filterEfficiency = schema.get("FilterEfficiency", 1.0)
        efficiencyFactor = self._getTaskEfficiencyFactor(schema)

        sizePerEvent *= efficiencyFactor * filterEfficiency
        eventsPerLumi *= efficiencyFactor
        eventsPerJob = schema.get("events_per_job", 0)

        maxEventsPerLumi = int(gbSpaceLimit / sizePerEvent)

        size = (eventsPerLumi * sizePerEvent) / (1024.0 ** 2.0)
        if size > gbSpaceLimit:
            self.logger.info(self.logMsg["largeOutputSize"], size, gbSpaceLimit, eventsPerLumi, maxEventsPerLumi)
            return maxEventsPerLumi / efficiencyFactor, None

        outputSize = (eventsPerJob * sizePerEvent) / (1024.0 ** 2.0)
        if outputSize > gbSpaceLimit:
            self.logger.info(
                self.logMsg["reduceLargeOutput"],
                eventsPerJob,
                sizePerEvent,
                filterEfficiency,
                outputSize,
                gbSpaceLimit,
                maxEventsPerLumi,
            )
            return maxEventsPerLumi / efficiencyFactor, maxEventsPerLumi

        return None, None

    def _getOutputSizeCorrectionFactor(self, task: str) -> float:
        """
        The function to get the output size correction for a given task
        :param task: task name
        :return: correction factor
        """
        outputSizeCorrection = self.unifiedConfiguration.get("output_size_correction")
        for keyword, factor in outputSizeCorrection.items():
            if keyword in task:
                return factor
        return 1.0

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

    def isGoodToConvertToStepChain(self, keywords: Optional[list] = None) -> bool:
        """
        The function to check if a request is good to be converted to step chain
        :param keywords: optional keywords list
        :return: True if good, False o/w
        """
        try:

            tiers = [*map(lambda x: x.split("/")[-1], self.get("OutputDatasets", []))]
            processingString = "".join(f"{k}{v}" for k, v in self.getProcessingString().items())
            foundKeywords = any(keyword in processingString + self.wf for keyword in keywords) if keywords else True

            relValCheck = not self.isRelVal()
            efficiencyCheck = self._hasAcceptableEfficiency()
            dataTierCheck = len(tiers) == len(set(tiers))
            archCheck = len(set(map(lambda x: x[:4], self.getParamList("ScramArch")))) == 1 # All steps should have the same architecture
            keywordCheck = any(keyword in processingString + self.wf for keyword in keywords) if keywords else True
            nTaskCheck = self.get("TaskChain", 0) > 1
            eventStreamCheck = not self._hasNonZeroEventStreams()

            self.logger.info(f"Stepchain criteria: RelVal check: {relValCheck}")
            self.logger.info(f"Stepchain criteria: Efficiency check: {efficiencyCheck}")
            self.logger.info(f"Stepchain criteria: Data tier check: {dataTierCheck}")
            self.logger.info(f"Stepchain criteria: Architecture check: {archCheck}")
            self.logger.info(f"Stepchain criteria: Keyword check: {keywordCheck}")
            self.logger.info(f"Stepchain criteria: # of tasks check: {nTaskCheck}")
            self.logger.info(f"Stepchain criteria: Event Stream check: {eventStreamCheck}")

            return (
                relValCheck
                and efficiencyCheck
                and dataTierCheck
                and archCheck
                and keywordCheck
                and nTaskCheck
                and eventStreamCheck
            )

        except Exception as error:
            self.logger.error("Failed to check if good to convert to step chain")
            self.logger.error(str(error))

    def convertToStepChain(self) -> object:
        """
        The function to convert the request to step chain
        :return: a StepChainWfSchemaHandler if the convertion is possible, itself o/w
        """
        try:
            multicore, memory = 0, 0
            stepNames = {}

            convertedWfSchema = self.wfSchema.copy()
            convertedWfSchema["RequestType"] = "StepChain"
            convertedWfSchema["StepChain"] = convertedWfSchema.pop("TaskChain")

            for key in self.chainKeys:
                stepName = "Step{}".format(re.findall(r'\d+', key)[0])
                self.logger.info(f"stepName: {stepName}")
                self.logger.info(f"key: {key}")
                self.logger.info(f"convertedWfSchema[stepName]: {convertedWfSchema[stepName]}")
                convertedWfSchema[stepName] = convertedWfSchema.pop(key)
                self.logger.info(f"convertedWfSchema[stepName]: {convertedWfSchema[stepName]}")
                convertedWfSchema[stepName]["StepName"] = convertedWfSchema[stepName].pop("TaskName")
                stepNames[convertedWfSchema[stepName]["StepName"]] = stepName

                efficiencyFactor = self._getTaskEfficiencyFactor(self.wfSchema[key])
                convertedWfSchema["TimePerEvent"] += efficiencyFactor * convertedWfSchema[stepName].pop("TimePerEvent")
                convertedWfSchema["SizePerEvent"] += efficiencyFactor * convertedWfSchema[stepName].pop("SizePerEvent")

                if "InputTask" in convertedWfSchema[stepName]:
                    convertedWfSchema[stepName]["InputStep"] = convertedWfSchema[stepName].pop("InputTask")

                if "KeepOutput" not in convertedWfSchema[stepName]:
                    convertedWfSchema[stepName]["KeepOutput"] = False

                stepMulticore = convertedWfSchema[stepName].get("Multicore")
                if stepMulticore != 1:
                    stepMulticore = convertedWfSchema[stepName].pop("Multicore")
                if multicore and stepMulticore != multicore:
                    self.logger.info(self.logMsg["diffMulticoreConversion"], stepMulticore, multicore)
                multicore = max(multicore, stepMulticore)
                memory = max(memory, convertedWfSchema[stepName].pop("Memory"))
                    
            if multicore > self.unifiedConfiguration.get("max_nCores_for_stepchain") or memory > self.unifiedConfiguration.get("max_memory_for_stepchain"):
                multicore = self.unifiedConfiguration.get("max_nCores_for_stepchain")
                memory = memory > self.unifiedConfiguration.get("max_memory_for_stepchain")
            
            convertedWfSchema["Multicore"] = multicore
            convertedWfSchema["Memory"] = memory

            return StepChainWfSchemaHandler(convertedWfSchema)

        except Exception as error:
            self.logger.error("Failed to convert workflow to step chain")
            self.logger.error(str(error))
            self.logger.error(traceback.format_exc())

    def getRequestNumEvents(self) -> int:
        """
        The function to get the number of events in the request
        :return: number of events
        """
        return int(self.get("Task1").get("RequestNumEvents") or 0)

    def getBlowupFactor(self, splittings: list) -> float:
        """
        The function to get the blow up factor considering the given splittings
        :param splittings: splittings schemas
        :return: blow up
        """
        try:
            maxBlowUp = 0
            eventKeys = ["events_per_job", "avg_events_per_job"]

            for splitting in splittings:
                childrenSize, parentsSize = 0, 0

                key = "splittingTask"
                task = splitting.get(key)

                parents = [splt for splt in splittings if task.startswith(splt.get(key)) and task != splt.get(key)]
                if parents:
                    for parent in parents:
                        for k in eventKeys:
                            parentsSize = parent.get(k, parentsSize)

                    for k in eventKeys:
                        childrenSize = splitting.get(k, childrenSize)

                    if childrenSize:
                        blowUp = float(parentsSize) / childrenSize
                        if blowUp > maxBlowUp:
                            maxBlowUp = blowUp

            return maxBlowUp

        except Exception as error:
            self.logger.error("Failed to get blowup factors")
            self.logger.error(str(error))

    def checkSplittings(self, splittings: dict) -> Tuple[bool, list]:
        """
        The function to check the splittings sizes and if any action is required
        :param splittings: splittings schema
        :return: if to hold and a list of modified splittings
        """
        try:
            hold, modifiedSplittings = False, []

            eventsPerLumi, inputEventsPerLumi, maxEventsPerLumi = None, None, []
            smallLumi = False

            _, primaries, _, _ = self.getIO()

            for splitting in splittings:
                params = splitting.get("splitParams", {})

                task = splitting.get("taskName").split("/")[-1]
                schema = self._getTaskSchema(task)
                schema["SizePerEvent"] *= self._getOutputSizeCorrectionFactor(task)

                inputDataset = schema.get("InputDataset")
                if inputDataset:
                    inputEventsPerLumi = self.dbsReader.getDatasetEventsPerLumi(schema.get(inputDataset))

                eventsPerLumi = params.get("events_per_lumi") or inputEventsPerLumi or eventsPerLumi
                if not eventsPerLumi or "events_per_job" not in params:
                    continue

                maxTaskEventsPerLumi, maxTaskEventsPerJob = self._getTaskMaxEventsPerLumiAndJob(schema, eventsPerLumi)
                maxEventsPerLumi += maxTaskEventsPerLumi
                if maxTaskEventsPerJob:
                    splitting["splitParams"]["events_per_job"] = maxTaskEventsPerJob
                    modifiedSplittings.append(splitting)

                if not smallLumi and not primaries and not self.isRelVal():
                    effEventsPerLumi = min(eventsPerLumi, min(maxEventsPerLumi, default=eventsPerLumi))
                    effEventsPerLumi *= schema.get("FilterEfficiency", 1.0)
                    smallLumi = self._isBelowMinEventsPerLumi(effEventsPerLumi)
                    hold |= smallLumi

            if maxEventsPerLumi:
                if inputEventsPerLumi and min(maxEventsPerLumi) < inputEventsPerLumi:
                    self.logger.critical(self.logMsg["smallEventsPerLumi"], maxEventsPerLumi, inputEventsPerLumi)
                    self.logger.critical(self.logMsg["wfUrl"], self.getParamList("PrepID")[0])
                    hold = True

                elif not inputEventsPerLumi:
                    rootSplitting = splittings[0]
                    rootEventsPerLumi = rootSplitting.get("splitParams", {}).get("events_per_lumi")
                    if rootEventsPerLumi and min(maxEventsPerLumi) < rootEventsPerLumi:
                        rootSplitting["splitParams"]["events_per_lumi"] = min(maxEventsPerLumi)
                        modifiedSplittings.append(rootSplitting)

            return hold, modifiedSplittings

        except Exception as error:
            self.logger.error("Failed to check splittings")
            self.logger.error(str(error))

    def writeDatasetPatternName(self, elements: list) -> str:
        """
        The function to write the dataset pattern name from given elements
        :param elements: dataset name elements — name, acquisition era, processing string, version, tier
        :return: dataset name
        """
        try:
            if elements[3] != "v*" and all(element == "*" for element in elements[1:3]):
                return None
            return f"/{elements[0]}/{'-'.join(elements[1:4])}/{elements[4]}"

        except Exception as error:
            self.logger.error("Failed to write dataset pattern name for %s", elements)
            self.logger.error(str(error))

    def setMemory(self, memory: int) -> None:
        """
        The function to set a given memory value to the workflow schema
        :param memory: new memory value
        """
        try:
            for key in self.chainKeys:
                self.wfSchema[key]["Memory"] = memory
        
        except Exception as error:
            self.logger.error("Failed to set memory to schema")
            self.logger.error(str(error))

    def setMulticore(self, multicore: int, tasks: Optional[List[str]] = None) -> None:
        """
        The function to set a given multicore value to the workflow schema
        :param multicore: new multicore value
        :param tasks: tasks names
        """
        try:
            for key, task in filterKeys(self.chainKeys, self.wfSchema).items():
                if key not in tasks and task.get("TaskName") not in tasks:
                    continue
                
                memoryPerCore = int(0.6 * task.get("Memory") / task.get("Multicore"))
                self.logger.info("%s: %s of memory per core, %s of base memory", key, memoryPerCore, task.get("Memory"))

                self.wfSchema[key]["Memory"] += (multicore - task.get("Multicore")) * memoryPerCore
                self.wfSchema[key]["TimePerEvent"] /= (multicore / task.get("Multicore"))
                self.wfSchema[key]["Multicore"] = multicore

        except Exception as error:
            self.logger.error("Failed to set multicore to schema")
            self.logger.error(str(error))

    def setParamValue(self, key: str, value: Any, task: Optional[str] = None) -> None:
        """
        The function to set a value for a given param
        :param key: key name
        :param value: new value
        :param task: optional task name
        """
        try:
            if task is None:
                self.wfSchema[key] = value
            elif task in self.wfSchema:
                self.wfSchema[task][key] = value
        
        except Exception as error:
            self.logger.error("Failed to set value to %s on schema", key)
            self.logger.error(str(error))

    def setNoOutput(self) -> None:
        """
        The function to set not keeping the output in the schema
        """
        try:
            for key in self.chainKeys[:-1]:
                self.wfSchema[key]["KeepOutput"] = False
            
            self.wfSchema["TaskChain"] = len(self.chainKeys) - 1
            self.wfSchema.pop(self.chainKeys.pop())
        
        except Exception as error:
            self.logger.error("Failed to set no output to schema")
            self.logger.error(str(error))

    def shortenTaskName(self) -> None:
        """
        The function to shorten the tasks names
        """
        try:
            newShortNames = {}
            for key, task in filterKeys(self.chainKeys, self.wfSchema).items():
                taskName = task.get("TaskName")
                shortName = T.format(re.findall(r'\d+', taskName)[0])

                newShortNames[taskName] = shortName
                self.wfSchema[key]["TaskName"] = shortName

            for param in ["ProcessingString", "AcquisitionEra"]:
                for key in self.wfSchema.get(param, {}).keys():
                    self.wfSchema[param][newShortNames[key]] = self.wfSchema[param].pop(key)
            
        except Exception as error:
            self.logger.error("Failed to shorten the tasks names")
            self.logger.error(str(error))
    
    
