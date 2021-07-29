import logging
from logging import Logger
from abc import ABC, abstractmethod

from Services.DBS.DBSReader import DBSReader
from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional, Any, Union, Tuple, List


class BaseWorkloadHandler(ABC):
    """
    __BaseWorkloadHandler__
    General Abstract Base Class for building the concrete request data handlers based on the request type
    """

    def __init__(self, wfSchema: dict, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.unifiedConfiguration = ConfigurationHandler("unifiedConfiguration.json")
            self.dbsReader = DBSReader()
            self.reqmgrReader = ReqMgrReader()

            self.wfSchema = wfSchema
            self.includeHEPCloudInSiteWhiteList = False

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing {self.__class__.__name__}\n{str(error)}")

    def __contains__(self, value: str) -> bool:
        return value in self.wfSchema

    def _getTaskIO(self, schema: Optional[dict] = None) -> Tuple[bool, set, set, set]:
        """
        The function to get the inputs/outputs for a given request schema
        :param request: request schema, use workflow schema if None is given
        :return: if any lhe input file, primaries, parents and secondaries
        """
        schema = schema or self.wfSchema

        lhe = schema.get("LheInputFiles") in ["True", True]
        primaries = set(list(filter(None, [schema.get("InputDataset")])))
        secondaries = set(list(filter(None, [schema.get("MCPileup")])))

        parents = set()
        if primaries and schema.get("IncludeParents"):
            parents.update(self.reqmgrReader.getDatasetParent(primary) for primary in primaries)

        return lhe, primaries, parents, secondaries

    def isRelVal(self) -> bool:
        """
        The function to check if a request is for release validation or not
        :return: True if is RelVal, False o/w
        """
        return "RelVal" in self.get("SubRequestType", [])

    def isProducingPremix(self) -> bool:
        """
        The function to determine whether the workflow is producing PREMIX
        :return: True if producing premix, False o/w
        """
        return "premix" in [*map(lambda x: x.split("/")[-1].lower(), self.get("OutputDatasets", []))]

    def get(self, key: str, defaultValue: Any = None) -> Any:
        """
        The function to get the request value for a given key
        :param key: key name
        :param defaultValue: optional default value
        :return: key value
        """
        return self.wfSchema.get(key, defaultValue)

    def getTasksPerOutputDatasets(self, workTasks) -> dict:
        """
        The function to get the tasks by the output datasets
        :param workTasks: work tasks
        :return: a dict of dataset names by task names
        """
        try:
            taskPerOutput = {}
            for task, datasets in self.getOutputDatasetsPerTask(workTasks).items():
                for dataset in datasets:
                    taskPerOutput[dataset] = task

            return taskPerOutput

        except Exception as error:
            self.logger.error("Failed to get tasks by the output datasets")
            self.logger.error(str(error))

    @abstractmethod
    def isGoodToConvertToStepChain(self, keywords: Optional[list] = None) -> bool:
        """
        The function to check if a request is good to be converted to step chain
        :param keywords: optional keywords list
        :return: True if good, False o/w
        """
        pass

    @abstractmethod
    def getAcquisitionEra(self) -> Union[str, dict]:
        """
        The function to get the workflow acquisition era
        :return: acquisition era
        """
        pass

    @abstractmethod
    def getProcessingString(self) -> Union[str, dict]:
        """
        The function to get the workflow processing string
        :return: processing string
        """
        pass

    @abstractmethod
    def getMemory(self) -> Optional[float]:
        """
        The function to get the workflow memory
        :return: memory value if any, None o/w
        """
        pass

    @abstractmethod
    def getIO(self) -> Tuple[bool, set, set, set]:
        """
        The function to get the inputs/outputs
        :return: if any lhe input file, primaries, parents and secondaries
        """
        pass

    @abstractmethod
    def getMulticore(self, maxOnly: bool = True) -> Union[int, list]:
        """
        The function to get the workflow multicore
        :param maxOnly: if True return max multicore, o/w return list of multicore values
        :return: multicore
        """
        pass

    @abstractmethod
    def getRequestNumEvents(self) -> int:
        """
        The function to get the number of events in the request
        :return: number of events
        """
        pass

    @abstractmethod
    def getCampaigns(self, details: bool = True) -> Union[str, dict, list]:
        """
        The function to get the workflow campaigns
        :param details: if True and if the request type is a chain it returns details of campaigns, o/w just campaigns names
        :return: campaigns
        """
        pass

    @abstractmethod
    def getCampaignsAndLabels(self) -> list:
        """
        The function to get a list of campaigns and labels
        :return: a list of tuples containing campaign name and processing string
        """
        pass

    @abstractmethod
    def getParamList(self, key: str) -> list:
        """
        The function to get the workflow's param list
        :param key: key name
        :return: values list
        """
        pass

    @abstractmethod
    def getParamByTask(self, key: str, task: str) -> Any:
        """
        The function to get a param value for a given task
        :param key: key name
        :param task: task name
        :return: value
        """
        pass

    @abstractmethod
    def getExpectedEventsPerTask(self) -> dict:
        """
        The function to get the number of expected events
        :return: expected events by task
        """
        pass

    @abstractmethod
    def getOutputDatasetsPerTask(self, workTasks) -> dict:
        """
        The function to get the output datasets by task
        :param workTasks: work tasks
        :return: a dict of dataset names by task names
        """
        pass

    @abstractmethod
    def getComputingTime(self) -> int:
        """
        The function to get the computing time (in seconds)
        :return: computing time
        """
        pass

    @abstractmethod
    def getBlowupFactor(self, splittings: List[dict]) -> float:
        """
        The function to get the blow up factor considering the given splittings
        :param splittings: splittings schemas
        :return: blow up
        """
        pass

    @abstractmethod
    def checkSplittingsSize(self, splittings: List[dict]) -> Tuple[bool, list]:
        """
        The function to check the splittings sizes and if any action is required
        :param splittings: splittings schema
        :return: if to hold and a list of modified splittings
        """
        pass

    @abstractmethod
    def writeDatasetPatternName(self, elements: list) -> str:
        """
        The function to write the dataset pattern name from given elements
        :param elements: dataset name elements — name, acquisition era, processing string, version, tier
        :return: dataset name
        """
        pass
