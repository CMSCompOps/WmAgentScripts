from collections import defaultdict

from typing import Optional, Union, Any, Tuple

from Components.RequestData.BaseRequestDataHandler import BaseRequestDataHandler


class NonChainRequestDataHandler(BaseRequestDataHandler):
    """
    __NonChainRequestDataHandler__
    General API for handling the request data of non-chain request type
    """

    def isGoodToConvertToStepChain(self, _: Optional[list]) -> bool:
        self.logger.info("Convertion is supported only from TaskChain to StepChain")
        return False

    def getAcquisitionEra(self) -> str:
        return self.get("AcquisitionEra")

    def getProcessingString(self) -> str:
        return self.get("ProcessingString")

    def getMemory(self) -> float:
        return self.get("Memory")

    def getIO(self) -> Tuple[bool, list, list, list]:
        return self._getTaskIO()

    def getMulticore(self, details: bool = False) -> Union[int, list]:
        return [int(self.get("Multicore"))] if details else int(self.get("Multicore"))

    def getEvents(self) -> int:
        return int(self.get("RequestNumEvents"))

    def getCampaigns(self, details: bool = True) -> Union[str, list]:
        return self.get("Campaign") if details else [self.get("Campaign")]

    def getCampaignsAndLabels(self) -> list:
        return [(self.getCampaigns(), self.getProcessingString())]

    def getParamList(self, key: str) -> list:
        return list(set(self.get(key, [])))

    def getParamByTask(self, key: str, _: str) -> Any:
        return self.get(key)

    def getExpectedEventsByTask(self) -> dict:
        return {}

    def getOutputDatasetsByTask(self, workTasks=None) -> dict:
        try:
            outputByTask = defaultdict(set)
            for task in workTasks:
                outputModules = (
                    task.subscriptions.outputModules
                    if hasattr(task.subscriptions, "outputModules")
                    else task.subscriptions.outputSubs
                )

                for module in outputModules:
                    dataset = getattr(task.subscriptions, module).dataset
                    if dataset in self.get("OutputDatasets", []):
                        outputByTask[task._internal_name].append(dataset)

            return dict(outputByTask)

        except Exception as error:
            self.logger.error("Failed to get output datasets by task")
            self.logger.error(str(error))

    def getComputingTime(self) -> int:
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

    def getBlowupFactors(self, _: list) -> Tuple[float, float, float]:
        self.logger.info("Blockup factors only exists for TaskChain")
        return 1.0, 1.0, 1.0

    def checkSplittings(self, _: list) -> Tuple[bool, list]:
        return False, []

    def writeDatasetPatternName(self, *elements) -> str:
        try:
            if (elements[3] == "v*" and all(element == "*" for element in elements[1:3])) or (
                elements[3] != "v*" and elements[2] == "*"
            ):
                return None
            return f"/{elements[0]}/{'-'.join(elements[1:4]/{elements[4]})}"

        except Exception as error:
            self.logger.error("Failed to write dataset pattern name for %s", elements)
            self.logger.error(str(error))
