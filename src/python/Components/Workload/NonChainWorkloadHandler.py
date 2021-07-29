from collections import defaultdict

from typing import Optional, Union, Any, Tuple

from Components.Workload.BaseWorkloadHandler import BaseWorkloadHandler


class NonChainWorkloadHandler(BaseWorkloadHandler):
    """
    __NonChainWorkloadHandler__
    General API for handling the request data of non-chain request type
    """

    def isGoodToConvertToStepChain(self, _: Optional[list]) -> bool:
        """
        The function to check if a request is good to be converted to step chain.
        :return: False, since the convertion is not supported for non-chain requests
        """
        self.logger.info("Convertion is supported only from TaskChain to StepChain")
        return False

    def getAcquisitionEra(self) -> str:
        """
        The function to get the workflow acquisition era
        :return: acquisition era
        """
        return self.get("AcquisitionEra")

    def getProcessingString(self) -> str:
        """
        The function to get the workflow processing string
        :return: processing string
        """
        return self.get("ProcessingString")

    def getMemory(self) -> float:
        """
        The function to get the workflow memory
        :return: memory value if any, None o/w
        """
        return self.get("Memory")

    def getIO(self) -> Tuple[bool, set, set, set]:
        """
        The function to get the inputs/outputs
        :return: if any lhe input file, primaries, parents and secondaries
        """
        return self._getTaskIO()

    def getMulticore(self, maxOnly: bool = True) -> Union[int, list]:
        """
        The function to get the workflow multicore
        :param maxOnly: if True return max multicore, o/w return list of multicore values
        :return: multicore
        """
        return int(self.get("Multicore")) if maxOnly else [int(self.get("Multicore"))]

    def getRequestNumEvents(self) -> int:
        """
        The function to get the number of events in the request
        :return: number of events
        """
        return int(self.get("RequestNumEvents"))

    def getCampaigns(self, details: bool = True) -> Union[str, list]:
        """
        The function to get the workflow campaigns
        :param details: if True and if the request type is a chain it returns details of campaigns, o/w just campaigns names
        :return: campaigns
        """
        return self.get("Campaign") if details else [self.get("Campaign")]

    def getCampaignsAndLabels(self) -> list:
        """
        The function to get a list of campaigns and labels
        :return: a list of tuples containing campaign name and processing string
        """
        return [(self.getCampaigns(), self.getProcessingString())]

    def getParamList(self, key: str) -> list:
        """
        The function to get the workflow's param list
        :param key: key name
        :return: values list
        """
        value = self.get(key, [])
        if isinstance(value, str):
            value = [value]
        return list(set(value))

    def getParamByTask(self, key: str, _: str) -> Any:
        """
        The function to get a param value for a given key
        :param key: key name
        :return: value

        Since non-chain requests have no task then return request value for the given key.
        """
        return self.get(key)

    def getExpectedEventsPerTask(self) -> dict:
        """
        The function to get the number of expected events
        :return: empty dict, since non-chain requests have no tasks
        """
        return {}

    def getOutputDatasetsPerTask(self, workTasks=None) -> dict:
        """
        The function to get the output datasets by task
        :param workTasks: work tasks
        :return: a dict of dataset names by task names
        """
        try:
            outputPerTask = defaultdict(set)
            for task in workTasks:
                outputModules = (
                    task.subscriptions.outputModules
                    if hasattr(task.subscriptions, "outputModules")
                    else task.subscriptions.outputSubs
                )

                for module in outputModules:
                    dataset = getattr(task.subscriptions, module).dataset
                    if dataset in self.get("OutputDatasets", []):
                        outputPerTask[task._internal_name].append(dataset)

            return dict(outputPerTask)

        except Exception as error:
            self.logger.error("Failed to get output datasets by task")
            self.logger.error(str(error))

    def getComputingTime(self) -> int:
        """
        The function to get the computing time (in seconds)
        :return: computing time
        """
        try:
            if self.get("BlockWhiteList"):
                events, _ = self.dbsReader.getBlocksEventsAndLumis(self.get("BlockWhiteList"))
            elif self.get("InputDataset"):
                events, _ = self.dbsReader.getDatasetEventsAndLumis(self.get("InputDataset"))
            else:
                events = float(self.get("RequestNumEvents")) / float(self.get("FilterEfficiency"))

            return events * self.get("TimePerEvent")

        except Exception as error:
            self.logger.error("Failed to get the computing time")
            self.logger.error(str(error))

    def getBlowupFactor(self, _: list) -> float:
        """
        The function to get the blow up factor
        :return: 1, since blow up factor does not exist for non-chain request
        """
        self.logger.info("Blockup factor only exists for TaskChain")
        return 1.0

    def checkSplittingsSize(self, _: list) -> Tuple[bool, list]:
        """
        The function to check the splittings sizes
        :return: no hold and no modified splittings, since this check does not exist for non-chain request
        """
        return False, []

    def writeDatasetPatternName(self, elements: list) -> str:
        """
        The function to write the dataset pattern name from given elements
        :param elements: dataset name elements — name, acquisition era, processing string, version, tier
        :return: dataset name
        """
        try:
            if (elements[3] == "v*" and all(element == "*" for element in elements[1:3])) or (
                elements[3] != "v*" and elements[2] == "*"
            ):
                return None

            return f"/{elements[0]}/{'-'.join(elements[1:4]/{elements[4]})}"

        except Exception as error:
            self.logger.error("Failed to write dataset pattern name for %s", elements)
            self.logger.error(str(error))
