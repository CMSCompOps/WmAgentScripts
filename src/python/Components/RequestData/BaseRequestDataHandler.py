import logging
from logging import Logger
from abc import ABC, abstractmethod

from Services.DBS.DBSReader import DBSReader
from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional, Any, Union, Tuple


class BaseRequestDataHandler(ABC):
    """
    __BaseRequestDataHandler__
    General Abstract Base Class for building the concrete request data handlers based on the request type
    """

    def __init__(self, wfSchema: dict, logger: Optional[Logger] = None) -> None:
        try:
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

    def _getTaskIO(self, schema: Optional[dict] = None) -> Tuple[bool, list, list, list]:
        """
        The function to get the inputs/outputs for a given request schema
        :param request: request schema, use workflow schema if None is given
        :return: if any lhe input file, primaries, parents and secondaries
        """
        schema = schema or self.wfSchema

        primaries, parents, secondaries = set(), set(), set()
        primaries.update(filter(None, [schema.get("InputDataset")]))
        secondaries.update(filter(None, [schema.get("MCPileup")]))
        if primaries and schema.get("IncludeParents"):
            parents.update(self.reqmgrReader.getDatasetParent(primary) for primary in primaries)

        lhe = schema.get("LheInputFiles") in ["True", True]
        return lhe, primaries, parents, secondaries

    def isRelval(self) -> bool:
        """
        The function to check if a request is relval or not
        :return: True if is relval, False o/w
        """
        return "RelVal" in self.get("SubRequestType", [])

    def isProducingPremix(self) -> bool:
        """
        The function to determine whether the workflow is producing PREMIX
        :return: True if producing premix, False o/w
        """
        return "premix" in [x.split("/")[-1].lower() for x in self.get("OutputDatasets", [])]

    def get(self, key: str, defaultValue: Any = None) -> Any:
        """
        The function to get the request value for a given key
        :param key: key name
        :param defaultValue: optional default value
        :return: key value
        """
        return self.wfSchema.get(key, defaultValue)

    def getTasksByOutputDatasets(self, workTasks) -> dict:
        """
        The function to get the tasks by the output datasets
        :param workTasks: work tasks
        :return: a dict of dataset names by task names
        """
        try:
            taskByOutput = {}
            for task, datasets in self.getOutputDatasetsByTask(workTasks).items():
                for dataset in datasets:
                    taskByOutput[dataset] = task

            return taskByOutput

        except Exception as error:
            self.logger.error("Failed to get tasks by the output datasets")
            self.logger.error(str(error))

    @abstractmethod
    def isGoodToConvertToStepChain(self, keywords: Optional[list] = None) -> bool:
        """
        The function to check if a request is good to be converted to step chain
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
    def getMemory(self) -> float:
        """
        The function to get the workflow memory
        :return: memory
        """
        pass

    @abstractmethod
    def getIO(self) -> Tuple[bool, list, list, list]:
        """
        The function to get the inputs/outputs
        :return: if any lhe input file, primaries, parents and secondaries
        """
        pass

    @abstractmethod
    def getMulticore(self, details: bool = False) -> Union[int, list]:
        """
        The function to get the workflow multicore
        :param details: if True return list of multicores by task, o/w return multicore value
        :return: multicore
        """
        pass

    @abstractmethod
    def getEvents(self) -> int:
        """
        The function to get the number of events in the request
        :return: number of events
        """
        pass

    @abstractmethod
    def getCampaigns(self, details: bool = True) -> Union[str, dict, list]:
        """
        The function to get the workflow campaigns
        :param details: if True and if the request type is a chain it returns details of campaigns, o/w, just campaigns names
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
        :return: block white list
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
    def getExpectedEventsByTask(self) -> dict:
        """
        The function to get the number of expected events
        :return: expected events by task
        """
        pass

    @abstractmethod
    def getOutputDatasetsByTask(self, workTasks) -> dict:
        """
        The function to get the output datasets by task
        :param workTasks: work tasks
        :return: a dict of dataset names by task names
        """
        pass

    @abstractmethod
    def getComputingTime(self) -> int:
        """
        The function to get the computing time (is seconds)
        :return: computing time
        """
        pass

    @abstractmethod
    def getBlowupFactors(self, splittings: list) -> Tuple[float, float, float]:
        """
        The function to get the blow up factors
        :return: number of min children by event, number of root job by event and max blow up
        """
        pass

    @abstractmethod
    def checkSplittings(self, splittings: dict) -> Tuple[bool, list]:
        """
        The function to check the splittings
        :param splittings: splittings schema
        :return: if to hold and a list of modified splittings
        """
        pass

    @abstractmethod
    def writeDatasetPatternName(self, *elements) -> str:
        """
        The function to write the dataset pattern name from given elements
        """
        pass
