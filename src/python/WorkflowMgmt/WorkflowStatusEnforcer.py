from logging import Logger

from WorkflowMgmt.WorkflowController import WorkflowController
from Services.ReqMgr.ReqMgrWriter import ReqMgrWriter
from Services.DBS.DBSWriter import DBSWriter
from Services.DBS.DBSReader import DBSReader
from Utilities.Logging import getLogger
from Utilities.Decorators import runWithRetries

from typing import Optional


class WorkflowStatusEnforcer(object):
    """
    _WorkflowStatusEnforcer_
    General API to force a workflow status
    """

    def __init__(self, wf: str, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.wf = wf
            self.workflowController = WorkflowController(wf)

            self.reqmgrWriter = ReqMgrWriter()
            self.dbs = {"writer": DBSWriter(), "reader": DBSReader()}

        except Exception as error:
            raise Exception(f"Error initializing WorkflowStatusEnforcer\n{str(error)}")

    @runWithRetries(tries=3, wait=0, default=False)
    def invalidate(self, onlyResubmissions: bool = False, invalidateOutputDatasets: bool = True) -> bool:
        """
        The function to invalidate the workflow
        :param onlyResubmissions: if to only include resubmissions
        :param invalidateOutputDatasets: if to invalidate output datasets too
        :return: if all invalidations succeeded or not
        """
        responses = []
        outputDatasets = set()

        family = self.workflowController.getFamily(onlyResubmissions, includeItself=True)

        for member in family:
            self.logger.info(
                "Checking wf family: %s, %s, %s",
                member.get("RequestName"),
                member.get("RequestStatus"),
                member.get("OutputDatasets"),
            )
            if invalidateOutputDatasets:
                outputDatasets.update(member.get("OutputDatasets"))

            response = self.reqmgrWriter.invalidateWorkflow(member.get("RequestName"), member.get("RequestStatus"))
            responses.append(response in ["None", None, True])

        for dataset in outputDatasets:
            currentStatus = self.dbs["reader"].getDBSStatus(dataset)
            response = self.dbs["writer"].setDatasetStatus(dataset, currentStatus, "INVALID")
            responses.append(response in ["None", None, True])

        if not all(responses):
            raise Exception("Failed to invalidate something")

        return True

    def forceComplete(self) -> bool:
        """
        The function to force completion
        :return: if all succeeded or not
        """
        try:
            family = self.workflowController.getFamily(includeItself=True)

            for member in family:
                self.logger.info("Considering %s as force-complete", member.get("RequestName"))

                if member.get("RequestStatus") in ["running-open", "running-closed"]:
                    self.logger.info("Setting %s as force-complete", member.get("RequestName"))
                    self.reqmgrWriter.forceCompleteWorkflow(member.get("RequestName"))
                elif member.get("RequestStatus") in ["acquired", "assignment-approved"]:
                    self.logger.info("Rejecting %s", member.get("RequestName"))
                    self.reqmgrWriter.invalidateWorkflow(member.get("RequestName"), member.get("RequestStatus"))

            return True

        except Exception as error:
            self.logger.error("Failed to force complete %s", self.wf)
            self.logger.error(str(error))
            return False
