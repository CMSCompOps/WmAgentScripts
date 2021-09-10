import logging
from logging import Logger
from collections import defaultdict

from Services.DBS.DBSReader import DBSReader
from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Utilities.IteratorTools import mapValues
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional, Any, Union, Tuple, List


class BaseWfSchemaHandler(object):
    """
    __BaseWfSchemaHandler__
    General API for handling the request data of a given workflow
    """

    def __init__(self, wfSchema: dict, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

            self.unifiedConfiguration = ConfigurationHandler("unifiedConfiguration.json")
            self.dbsReader = DBSReader()
            self.reqmgrReader = ReqMgrReader()

            self.wfSchema = wfSchema
            self.includeHEPCloudInSiteWhiteList = False

        except Exception as error:
            raise Exception(f"Error initializing {self.__class__.__name__}\n{str(error)}")

    def __contains__(self, value: str) -> bool:
        return value in self.wfSchema

    def _getTaskIO(self, schema: Optional[dict] = None) -> Tuple[bool, set, set, set]:
        """
        The function to get the inputs/outputs for a given task schema
        :param schema: task schema, use workflow schema if None is given
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
        return "RelVal" in self.get("SubRequestType", "")

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

    def isGoodToConvertToStepChain(self, keywords: Optional[list] = None) -> bool:
        """
        The function to check if a request is good to be converted to step chain
        :param keywords: optional keywords list
        :return: True if good, False o/w
        """
        self.logger.info("Convertion is supported only from TaskChain to StepChain")
        return False

    def convertToStepChain(self) -> object:
        """
        The function to convert the request to step chain
        :return: a StepChainWfSchemaHandler if the convertion is possible, itself o/w
        """
        self.logger.info(
            "Only a task chain request can be converted to step chain, will continue w/o changing the request type"
        )
        return self

    def getAcquisitionEra(self) -> Union[str, dict]:
        """
        The function to get the workflow acquisition era
        :return: acquisition era
        """
        return self.get("AcquisitionEra")

    def getProcessingString(self) -> Union[str, dict]:
        """
        The function to get the workflow processing string
        :return: processing string
        """
        return self.get("ProcessingString")

    def getMemory(self) -> Optional[float]:
        """
        The function to get the workflow memory
        :return: memory value if any, None o/w
        """
        return float(self.get("Memory"))

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
        return int(self.get("RequestNumEvents") or 0)

    def getCampaigns(self, details: bool = True) -> Union[str, dict, list]:
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

    def getLumiWhiteList(self) -> dict:
        """
        The function to get the workflow's lumi white list
        :return: lumi white list
        """
        return self.get("LumiList", {})

    def getParamList(self, key: str) -> list:
        """
        The function to get the workflow's param list
        :param key: key name
        :return: values list
        """
        try:
            value = self.get(key, [])
            if isinstance(value, str):
                return [value]
            return list(set(value))

        except Exception as error:
            self.logger.error("Failed to get param list")
            self.logger.error(str(error))

    def getParamByTask(self, key: str, task: str) -> Any:
        """
        The function to get a param value for a given task
        :param key: key name
        :param task: task name
        :return: value
        """
        return self.get(key)

    def getExpectedEventsPerTask(self) -> dict:
        """
        The function to get the number of expected events
        :return: expected events by task
        """
        return {}

    def getOutputDatasetsPerTask(self, workTasks) -> dict:
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
                        outputPerTask[task._internal_name].add(dataset)

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
            if self.get("BlockWhiteList"):
                events, _ = self.dbsReader.getBlocksEventsAndLumis(self.get("BlockWhiteList"))
            elif self.get("InputDataset"):
                events, _ = self.dbsReader.getDatasetEventsAndLumis(self.get("InputDataset"))
            else:
                events = float(self.get("RequestNumEvents")) / float(self.get("FilterEfficiency", 1))

            return events * self.get("TimePerEvent")

        except Exception as error:
            self.logger.error("Failed to get the computing time")
            self.logger.error(str(error))

    def getBlowupFactor(self, splittings: List[dict]) -> float:
        """
        The function to get the blow up factor considering the given splittings
        :param splittings: splittings schemas
        :return: blow up
        """
        self.logger.info("Blow up factor only exists for TaskChain")
        return 1.0

    def checkSplitting(self, splittings: List[dict]) -> Tuple[bool, list]:
        """
        The function to check the splittings sizes and if any action is required
        :param splittings: splittings schema
        :return: if to hold and a list of modified splittings
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
            self.wfSchema["Memory"] = memory

        except Exception as error:
            self.logger.error("Failed to set memory to schema")
            self.logger.error(str(error))

    def setMulticore(self, multicore: int, tasks: Optional[str] = None) -> None:
        """
        The function to set a given multicore value to the workflow schema
        :param multicore: new multicore value
        """
        try:
            self.wfSchema["Multicore"] = multicore

        except Exception as error:
            self.logger.error("Failed to set multicore to schema")
            self.logger.error(str(error))

    def setParamValue(self, key: str, value: Any, task: Optional[str] = None) -> None:
        """
        The function to set a value for a given param
        :param key: key name
        :param value: new value
        """
        try:
            if task is None:
                self.wfSchema[key] = value

        except Exception as error:
            self.logger.error("Failed to set value to %s on schema", key)
            self.logger.error(str(error))

    def setNoOutput(self) -> None:
        """
        The function to set not keeping the output in the schema
        """
        pass

    def shortenTaskName(self) -> None:
        """
        The function to shorten the tasks names
        """
        pass
