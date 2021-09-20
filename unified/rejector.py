import os
import json
import optparse
from logging import Logger

from Databases.Oracle.OracleClient import OracleClient
from Databases.Oracle.OracleDB import Workflow
from MongoControllers.BatchController import BatchController
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
                "failure": "Failed to %s workflow %s",
                "cloneError": "Error in cloning workflow %s",
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
            "-t", "--setTrouble", help="set status to trouble instead of forget", action="store_true", default=False
        )
        parser.add_option("-c", "--clone", help="clone the workflow", action="store_true", default=False)
        parser.add_option("--comments", help="comment about the clone", default="-")
        parser.add_option("--memory", help="change the memory of the clone", default=0, type=int)
        parser.add_option("--multicore", help="change the number of cores in the clone", default=None)
        parser.add_option("--processingString", help="change the processing string of the clone", default=None)
        parser.add_option("--acquisitionEra", help="change the acquisition era of the clone", default=None)
        parser.add_option("--prepId", help="change the prep id of the clone", default=None)
        parser.add_option("--eventsPerJob", help="change the events/job of the clone", default=0, type=int)
        parser.add_option("--priority", help="change the priority of the clone", default=0, type=int)
        parser.add_option(
            "--eventAwareLumiBased",
            help="change the splitting algorithm of the clone",
            action="store_true",
            default=False,
        )
        parser.add_option("--timePerEvent", help="change the time/event of the clone", default=0, type=float)
        parser.add_option(
            "--deterministic",
            help="change the splitting to deterministic in the clone",
            action="store_true",
            default=False,
        )
        parser.add_option("--runs", help="change the run whitelist in the clone", default=None)
        parser.add_option("--fileList", help="a file with a list of workflows", default=None)
        parser.add_option(
            "--noOutput",
            help="keep only the output of the last task of TaskChain",
            action="store_true",
            default=False,
        )
        parser.add_option(
            "--shortTask",
            help="Reduce the TaskName to a minimal value",
            action="store_true",
            default=False,
        )
        parser.add_option(
            "-s",
            "--toStepchain",
            help="transform a TaskChain into StepChain",
            action="store_true",
            default=False,
        )

        options, args = parser.parse_args()
        return vars(options), args[0] if args else None

    def _getWorkflowsToReject(self) -> list:
        """
        The function to get the workflows to reject
        :return: list of workflows names
        """
        if self.options.fileList:
            return self._getWorkflowsByFilelist()
        if self.specificType == "workflow":
            return self._getWorkflowsByName()

        raise ValueError("Cannot get list of workflows to reject, provide file or specific workflow name")

    def _getWorkflowsByFilelist(self) -> list:
        """
        The function to get the workflows by a given file list
        :return: list of workflows names
        """
        wfs = set()

        with open(self.options.fileList, "r") as file:
            for item in filter(None, file.read().split("\n")):
                wfs.update(self.session.query(Workflow).filter(Workflow.name.contains(item)).all())

        return list(wfs)

    def _getWorkflowsByName(self) -> list:
        """
        The function to get the workflows by a given specific name
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

    def _rejectDataset(self) -> bool:
        """
        The function to reject a given dataset
        :return: True if the dataset was properly rejected, False o/w
        """
        currentStatus = self.dbs["reader"].getDBSStatus(self.specific)

        rejected = self.dbs["writer"].setDatasetStatus(self.specific, currentStatus, "INVALID")
        self.logger.info(self.logMsg["dataset"], self.specific, rejected)

        return rejected

    def _rejectWorkflow(self, wf: Workflow, wfController: WorkflowController) -> bool:
        """
        The function to reject a given workflow
        :param wf: workflow
        :param wfController: workflow controller
        :return: True if the workflow was properly rejected, False o/w
        """
        wfStatusEnforcer = WorkflowStatusEnforcer(wf.name)
        rejected = wfStatusEnforcer.invalidate(onlyResubmissions=True, invalidateOutputDatasets=not self.options.keep)

        wfController.logger.info(self.logMsg["invalidate"])
        self.logger.info(self.logMsg["wf"], wf.name, rejected)

        if rejected:
            wf.status = "trouble" if self.options.setTrouble or self.options.clone else "forget"
            self.session.commit()

            wfController.logger.info(self.logMsg["reject"])

        return rejected

    def _cloneWorkflow(self, wfSchemaHandler: BaseWfSchemaHandler) -> None:
        """
        The function to clone a given workflow
        :param wfSchemaHandler: original workflow schema handler
        """
        clonedWfSchemaHandler = self._buildClonedWorkflowSchema(wfSchemaHandler)
        if self.options.toStepchain:
            clonedWfSchemaHandler = clonedWfSchemaHandler.convertToStepChain()

        clonedWfSchema = filterWorkflowSchemaParam(clonedWfSchemaHandler.wfSchema)

        submitted = self.reqmgr["writer"].submitWorkflow(clonedWfSchema)
        if not submitted:
            raise ValueError(self.logMsg["cloneError"], clonedWfSchema["RequestName"])

        self.reqmgr["writer"].approveWorkflow(clonedWfSchema["RequestName"])

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

        if self.options.shortTask:
            wfSchemaHandler.shortenTaskName()

        if self.options.eventsPerJob:
            wfSchemaHandler.setParamValue("EventsPerJob", self.options.eventsPerJob, task="Task1")

        if self.options.eventAwareLumiBased:
            wfSchemaHandler.setParamValue("SplittingAlgo", "EventAwareLumiBased")

        if self.options.timePerEvent:
            wfSchemaHandler.setParamValue("TimePerEvent", self.options.timePerEvent)

        if self.options.deterministic:
            wfSchemaHandler.setParamValue("DeterministicPileup", True, task="Task1")

        if self.options.processingString:
            wfSchemaHandler.setParamValue("ProcessingString", self.options.processingString)

        if self.options.acquisitionEra:
            wfSchemaHandler.setParamValue("AcquisitionEra", self.options.acquisitionEra)

        if self.options.prepId:
            wfSchemaHandler.setParamValue("PrepID", self.options.prepId)

        if self.options.priority:
            wfSchemaHandler.setParamValue("RequestPriority", self.options.priority)

        if self.options.runs:
            wfSchemaHandler.setParamValue("RunWhitelist", [*map(int, self.options.runs.split(","))])

        if self.options.noOutput:
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
            servicesChecker = ServicesChecker(softServices=["wtc", "jira"])
            return self.options.get("manual") or servicesChecker.check()

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
                try:
                    rejected = self._rejectWorkflow(wf, wfController)
                    if rejected and self.options.clone:
                        self._cloneWorkflow(wfController.request)

                except Exception as error:
                    wfController.logger.critical(
                        self.logMsg["failure"], "clone" if self.options.clone else "reject", wf.name
                    )
                    self.logger.error(self.logMsg["failure"], "reject", wf.name)
                    self.logger.error(str(error))

        except Exception as error:
            self.logger.error("Failed to run rejection")
            self.logger.error(str(error))


if __name__ == "__main__":
    options, specific = Rejector.parseOptions()
    rejector = Rejector(options=options, specific=specific)
    if rejector.go():
        rejector.run()
