from logging import Logger
from typing import Optional, Tuple

from Components.RequestData.BaseChainRequestDataHandler import BaseChainRequestDataHandler


class StepChainRequestDataHandler(BaseChainRequestDataHandler):
    """
    __StepChainRequestDataHandler__
    General API for handling the request data of step chain request type
    """
    def __init__(self, wfSchema: dict, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__(wfSchema, logger)
            self.includeHEPCloudInSiteWhiteList = True
        
        except Exception as error:
            raise Exception(f"Error initializing StepChainRequestDataHandler\n{str(error)}")

    def isGoodToConvertToStepChain(self, _: Optional[list]) -> bool:
        self.logger.info("Convertion is supported only from TaskChain to StepChain")
        return False

    def getEvents(self) -> int:
        return int(self.get("RequestNumEvents"))

    def getBlowupFactors(self, _: list) -> Tuple[float, float, float]:
        self.logger.info("Blockup factors only exists for TaskChain")
        return 1.0, 1.0, 1.0

    def checkSplittings(self, splittings: dict) -> Tuple[bool, list]:
        try:
            hold, modifiedSplittings = False, []

            nCores = self.getMulticore()
            gbSpaceLimit = self.unifiedConfiguration.get("GB_space_limit")
            totalGbSpaceLimit = nCores * gbSpaceLimit

            sizePerEvent = self.wfSchema.get("SizePerEvent")
            for task in splittings:
                avgEventsPerJob = task["splitParams"].get("events_per_job")
                if (
                    avgEventsPerJob
                    and sizePerEvent
                    and avgEventsPerJob * sizePerEvent > totalGbSpaceLimit * (1024 ** 2)
                ):
                    self.logger.info(
                        "The output size of task %s if expected to be large: %d x %.2f kB = %.2f GB > %f GB",
                        task["taskName"].split("/")[-1],
                        avgEventsPerJob,
                        sizePerEvent,
                        avgEventsPerJob * sizePerEvent / (1024 ** 2),
                        totalGbSpaceLimit,
                    )
                    taskAvgEventsPerJob = int(totalGbSpaceLimit * (1024 ** 2) / sizePerEvent)
                    task["splitParams"]["events_per_job"] = taskAvgEventsPerJob
                    modifiedSplittings.append(task)

            return hold, modifiedSplittings

        except Exception as error:
            self.logger.error("Failed to check splittings")
            self.logger.error(str(error))

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
