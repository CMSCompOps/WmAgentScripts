import os
import json
import optparse
from logging import Logger

from Databases.Oracle.OracleClient import OracleClient
from Databases.Oracle.OracleDB import Workflow
from MongoControllers.BatchController import BatchController
from MongoControllers.ModuleLockController import ModuleLockController
from Services.ServicesChecker import ServicesChecker
from Services.DBS.DBSWriter import DBSWriter
from Services.DBS.DBSReader import DBSReader
from Services.ReqMgr.ReqMgrWriter import ReqMgrWriter
from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Utilities.Logging import getLogger
from Utilities.DataTools import filterWorkflowSchemaParam
from WorkflowMgmt.WorkflowController import WorkflowController
from WorkflowMgmt.WorkflowStatusEnforcer import WorkflowStatusEnforcer
from WorkflowMgmt.WorkflowSchemaHandlers.BaseWfSchemaHandler import BaseWfSchemaHandler

from typing import Optional, Tuple


class Rejector(OracleClient):
    """
    __Rejector__
    General API for rejecting workflows and datasets
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__(self)
            self.logger = logger or getLogger(self.__class__.__name__)

            self.options, self.specific = kwargs.get("options"), kwargs.get("specific")
            if self.options is None:
                self.options, self.specific = self.parseOptions()

            self.specificType = (
                None if self.specific is None else "dataset" if self.specific.startswith("/") else "workflow"
            )

            self.dbs = {"writer": DBSWriter(), "reader": DBSReader()}
            self.reqmgr = {"writer": ReqMgrWriter(), "reader": ReqMgrReader()}

            self.user = os.getenv("USER")
            self.logMsg = {
                "dataset": "Rejected dataset %s: %s",
                "wf": "Rejected workflow %s: %s",
                "nWfs": "%s workflows to reject: %s",
                "schema": "Original schema: %s",
                "invalidate": f"Invalidating the workflow {'' if self.options.keep else 'and outputs'} by unified operator {self.user}, reason: {self.options.comment}",
                "reject": f"Rejected the workflow by unified operator {self.user}",
                "return": "Rejector was finished by user",
            }

        except Exception as error:
            raise Exception(f"Error initializing Rejector\n{str(error)}")

    @staticmethod
    def parseOptions() -> Tuple[dict, Optional[str]]:
        """
        The function to parse the Rejector's options and specific workflow
        :return: options and the specific workflow, if any
        """
        parser = optparse.OptionParser()

        parser.add_option("-m", "--manual", help="bypass JIRA check", action="store_true", default=False)
        parser.add_option("-k", "--keep", help="keep the output current status", action="store_true", default=False)
        parser.add_option(
            "-t", "--set_trouble", help="set status to trouble instead of forget", action="store_true", default=False
        )
        parser.add_option("-c", "--clone", help="clone the workflow", action="store_true", default=False)
        parser.add_option("--comments", help="comment about the clone", default="-")
        parser.add_option("--memory", help="change the memory of the clone", default=0, type=int)
        parser.add_option("--multicore", help="change the number of cores in the clone", default=None)
        parser.add_option("--processing_string", help="change the processing string of the clone", default=None)
        parser.add_option("--acquisition_era", help="change the acquisition era of the clone", default=None)
        parser.add_option("--prep_id", help="change the prepid of the clone", default=None)
        parser.add_option("--events_per_job", help="change the events/job of the clone", default=0, type=int)
        parser.add_option("--priority", help="change the priority of the clone", default=0, type=int)
        parser.add_option(
            "--event_aware_lumi_based",
            help="change the splitting algo of the clone",
            action="store_true",
            default=False,
        )
        parser.add_option("--time_per_event", help="change the time/event of the clone", default=0, type=float)
        parser.add_option(
            "--deterministic",
            help="change the splitting to deterministic in the clone",
            action="store_true",
            default=False,
        )
        parser.add_option("--runs", help="change the run whitelist in the clone", default=None)
        parser.add_option("--file_list", help="a file with a list of workflows", default=None)
        parser.add_option(
            "--no_output",
            help="keep only the output of the last task of TaskChain",
            action="store_true",
            default=False,
        )
        parser.add_option(
            "--short_task", help="Reduce the TaskName to a minimal value", default=False, action="store_true"
        )
        parser.add_option(
            "-s", "--to_stepchain", help="transform a TaskChain into StepChain", default=False, action="store_true"
        )

        options, args = parser.parse_args()
        return vars(options), args[0] if args else None

    def _getWorkflowsToReject(self) -> list:
        """
        The function to get the workflows to reject
        :return: list of workflows names
        """
        if self.options.file_list:
            return self._getWorkflowsByFilelist()
        if self.specificType == "workflow":
            return self._getWorkflowsByName()

        raise ValueError("Cannot get list of workflows to reject")

    def _getWorkflowsByFilelist(self) -> list:
        """
        The function to get the workflows by given file list
        :return: list of workflows names
        """
        wfs = set()

        with open(self.options.file_list, "r") as file:
            for item in filter(None, file.read().split("\n")):
                wfs.update(self.session.query(Workflow).filter(Workflow.name.contains(item)).all())

        return list(wfs)

    def _getWorkflowsByName(self) -> list:
        """
        The function to get the workflows by given specific name
        :return: list of workflows names
        """
        wfs = set()
        wfs.update(self.session.query(Workflow).filter(Workflow.name.contains(self.specific)).all())

        if not wfs:
            batchController = BatchController()
            batches = batchController.get()

            for prepId in batches.get(self.specific, []):
                batchWfs = self.reqmgr["reader"].getWorkflowsByPrepId(prepId)
                for wf in batchWfs:
                    wfs.add(self.session.query(Workflow).filter(Workflow.name == wf).first())

        return list(wfs)

    def _rejectDataset(self, dataset: Optional[str] = None) -> None:
        """
        The function to reject a given dataset
        :param dataset: optional dataset name
        """
        dataset = dataset or self.specific
        currentStatus = self.dbs["reader"].getDBSStatus(dataset)

        rejected = self.dbs["writer"].setDatasetStatus(dataset, currentStatus, "INVALID")

        self.logger.info(self.logMsg["dataset"], dataset, rejected)

    def _rejectWorkflow(self, wfController: WorkflowController) -> bool:
        """
        The function to reject a given workflow
        :param wfController: workflow controller
        :return: True if the workflow was rejected, False o/w
        """
        wf = wfController.request.get("RequestName")
        wfStatusEnforcer = WorkflowStatusEnforcer(wf)
        rejected = wfStatusEnforcer.invalidate(onlyResubmissions=True, invalidateOutputDatasets=not self.options.keep)

        wfController.logger.info(self.logMsg["invalidate"])
        self.logger.info(self.logMsg["wf"], wf, rejected)

        if rejected:
            wf.status = "trouble" if self.options.set_trouble or self.options.clone else "forget"
            self.session.commit()

            wfController.logger.info(self.logMsg["reject"])

        return rejected

    def _cloneWorkflow(self, wfSchemaHandler: BaseWfSchemaHandler) -> None:
        """
        The function to clone a given workflow
        :param wfSchemaHandler: original workflow schema handler
        """
        clonedWfSchemaHandler = self._buildClonedWorkflowSchema(wfSchemaHandler)
        if self.options.to_stepchain:
            clonedWfSchemaHandler = clonedWfSchemaHandler.convertToStepChain()

        clonedWfSchema = filterWorkflowSchemaParam(clonedWfSchemaHandler.wfSchema)
        self.reqmgr["writer"].submitWorkflow(clonedWfSchema)

    def _buildClonedWorkflowSchema(self, wfSchemaHandler: BaseWfSchemaHandler) -> BaseWfSchemaHandler:
        """
        The function to build the clone workflow schema
        :param wfSchemaHandler: original workflow schema handler
        :return: cloned workflow schema handler
        """
        self.logger.info(self.logMsg["schema"], json.dumps(wfSchemaHandler.wfSchema, indent=2))

        wfSchemaHandler.setParamValue("Requestor", self.user)
        wfSchemaHandler.setParamValue("Group", "DATAOPS")
        wfSchemaHandler.setParamValue("OriginalRequestName", wfSchemaHandler.get("RequestName"))
        wfSchemaHandler.setParamValue("ProcessingVersion", wfSchemaHandler.get("ProcessingVersion", 1) + 1)

        wfSchemaHandler.wfSchema = dict(
            (k, v)
            for k, v in wfSchemaHandler.wfSchema.items()
            if not k.startswith("Team") and not k.startswith("checkbox")
        )

        if self.options.memory:
            wfSchemaHandler.setMemory(self.options.memory)

        if self.options.multicore:
            tasks, multicore = (
                self.options.multicore.split(":")
                if ":" in self.options.multicore
                else ("Task1", self.options.multicore)
            )
            wfSchemaHandler.setMulticore(int(multicore), tasks.split(","))

        if self.options.short_task:
            wfSchemaHandler.shortenTaskName()

        if self.options.events_per_job:
            wfSchemaHandler.setParamValue("EventsPerJob", self.options.events_per_job, task="Task1")

        if self.options.deterministic:
            wfSchemaHandler.setParamValue("DeterministicPileup", True, task="Task1")

        if self.options.event_aware_lumi_based:
            wfSchemaHandler.setParamValue("SplittingAlgo", "EventAwareLumiBased")

        if self.options.time_per_event:
            wfSchemaHandler.setParamValue("TimePerEvent", self.options.time_per_event)

        if self.options.processing_string:
            wfSchemaHandler.setParamValue("ProcessingString", self.options.processing_string)

        if self.options.acquisition_era:
            wfSchemaHandler.setParamValue("AcquisitionEra", self.options.acquisition_era)

        if self.options.prep_id:
            wfSchemaHandler.setParamValue("PrepID", self.options.prep_id)

        if self.options.runs:
            wfSchemaHandler.setParamValue("RunWhitelist", [*map(int, self.options.runs)])

        if self.options.priority:
            wfSchemaHandler.setParamValue("RequestPriority", self.options.priority)

        if self.options.no_output:
            wfSchemaHandler.setNoOutput()

        return wfSchemaHandler

    def _proceedWithRejector(self) -> bool:
        userAnswer = input("Reject these?")
        return userAnswer.lower() in ["y", "yes"]

    def go(self) -> bool:
        """
        The function to check if the rejector can go
        :return: True if it can go, False o/w
        """
        try:
            moduleLockController = ModuleLockController()
            servicesChecker = ServicesChecker(softServices=["wtc", "jira"])

            return (self.options.get("manual") or not moduleLockController.isLocked()) and servicesChecker.check()

        except Exception as error:
            self.logger.error("Failed to check if Rejector can go")
            self.logger.error(str(error))

    def run(self) -> None:
        """
        The function to run rejector
        """
        try:
            if self.specificType == "dataset":
                return self._rejectDataset()

            wfsToReject = self._getWorkflowsToReject()
            self.logger.info(self.logMsg["nWfs"], len(wfsToReject), ", ".join(wfsToReject))

            if len(wfsToReject) > 1 and not self._proceedWithRejector():
                self.logger.info(self.logMsg["return"])
                return

            for wf in wfsToReject:
                wfController = WorkflowController(wf.name)
                rejected = self._rejectWorkflow(wfController)
                if rejected and self.options.clone:
                    self._cloneWorkflow(wfController.request)

        except Exception as error:
            self.logger.error("Failed to run rejection")
            self.logger.error(str(error))


if __name__ == "__main__":
    options, specific = Rejector.parseOptions()
    rejector = Rejector(options=options, specific=specific)
    if rejector.go():
        rejector.run()
