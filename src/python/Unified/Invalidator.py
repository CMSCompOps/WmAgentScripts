from logging import Logger
from collections import defaultdict

from Databases.Oracle.OracleClient import OracleClient
from Databases.Oracle.OracleDB import Workflow
from Services.ServicesChecker import ServicesChecker
from Services.DBS.DBSWriter import DBSWriter
from Services.DBS.DBSReader import DBSReader
from Services.McM.McMClient import McMClient
from Utilities.Logging import getLogger
from WorkflowMgmt.WorkflowController import WorkflowController
from WorkflowMgmt.WorkflowStatusEnforcer import WorkflowStatusEnforcer

from typing import Optional, List


class Invalidator(OracleClient):
    """
    __Invalidator__
    General API for invalidating workflows and datasets
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__(self)
            self.logger = logger or getLogger(self.__class__.__name__)

            self.invalidStatus = kwargs.get("invalidStatus") or "INVALID"
            self.invalidatedDatasets = defaultdict(str)
            self.invalidatedWorkflows = defaultdict(str)

            self.mcmClient = McMClient()
            self.dbs = {"writer": DBSWriter(), "reader": DBSReader()}

            self.logMsg = {
                "wfStatus": "Setting the status of %s to forget",
                "datasetStatus": "Setting the status of %s to %s",
                "wfAcknowledgment": "The workflow {} ({}) was rejected due to invalidation in McM",
                "datasetAcknowledgment": "The dataset {} ({}) was set INVALID due to invalidation in McM",
                "mcmRequest": "Rejection is performed from McM invalidations request",
                "failure": "Could not invalidate %s. Please consider contacting data management team for manual intervention.",
                "nInvalidations": "%s objects to be invalidated",
                "autoMsg": "\n This is an automated message",
            }

        except Exception as error:
            raise Exception(f"Error initializing Rejector\n{str(error)}")

    def _getInvalidations(self) -> Optional[List[dict]]:
        """
        The function to get the workflows and datasets to invalidate
        :return: invalidations
        """
        return self.mcmClient.search("invalidations", query="status=announced")

    def _invalidateWorkflow(self, wf: str) -> bool:
        """
        The function to invalidate a given workflow
        :param wf: workflow name
        :return: True if the workflow was properly invalidated, False o/w
        """
        workflow = self.session.query(Workflow).filter(Workflow.name == wf).first()
        if workflow:
            workflow.status = "forget"
            self.session.commit()

        wfStatusEnforcer = WorkflowStatusEnforcer(wf)
        invalidated = wfStatusEnforcer.invalidate(onlyResubmissions=True, invalidateOutputDatasets=False)

        wfController = WorkflowController(wf)
        wfController.logger.info(self.logMsg["mcmRequest"])

        self.logger.info("Workflow rejection of %s is completed.", wf)
        self.logger.info("Status of the operation: %s", str(invalidated))

        return invalidated

    def _invalidateDataset(self, dataset: str) -> bool:
        """
        The function to invalidate a given dataset
        :param dataset: dataset name
        :return: True if the dataset was properly invalidated, False o/w
        """
        if any(word in dataset for word in ["?", "None", "FAKE-"]):
            return False

        self.logger.info(self.logMsg["datasetStatus"], dataset, self.invalidStatus)

        currentStatus = self.dbs["reader"].getDBSStatus(dataset)
        invalidated = self.dbs["writer"].setDatasetStatus(dataset, currentStatus, self.invalidStatus)
        if not invalidated:
            self.logger.critical(self.logMsg["failure"], dataset)

        self.logger.info("Dataset invalidation of %s is completed", dataset)
        self.logger.info("Status of the operation: %s", str(invalidated))

        return invalidated

    def _writeNotificationText(self, prepId: str, keyword: str, msg: str) -> None:
        """
        The function to write notification for the invalidation
        :param prepId: invalid prep id
        :param keyword: invalidation keyword
        :param msg: invalidation message
        """
        self.logger.info("Keyword: %s", keyword)
        batches = self.mcmClient.search("batches", query=f"contains={keyword}")
        batches = [*filter(lambda x: x["status"] in ["announced", "done", "reset"], batches)]

        if batches:
            self.invalidatedDatasets[batches[-1].get("prepid")] += msg + "\n\n"
        self.invalidatedWorkflows[prepId] += msg + "\n\n"


    def _acknowledge(self, id) -> None:
        """
        The function to acknowledge invalidation for the given id
        """
        endpoint = "/restapi/invalidations/acknowledge/"
        try:
            res = self.mcmClient.get(endpoint + str(id))
            self.logger.info("Acknowledgement is successful: " + str(res))
        except Exception as error:
            self.logger.error("Acknowledgement failed for " + str(id))
            self.logger.error(str(error))

    def _notifyWorkflowsInvalidation(self) -> None:
        """
        The function to acknowledge all workflow invalidations to McM
        """
        for prepId, msg in self.invalidatedWorkflows.items():
            try:
                self.mcmClient.set(
                    "/restapi/requests/notify", {"message": msg + self.logMsg["autoMsg"], "prepids": [prepId]}
                )
                self.logger.info("Notification is successful")
            except Exception as error:
                self.logger.error("Failed to notify workflow invalidation")
                self.logger.error(str(error))

    def _notifyDatasetsInvalidation(self) -> None:
        """
        The function to acknowledge all dataset invalidations to McM
        """
        for batchId, msg in self.invalidatedDatasets.items():
            try:
                self.mcmClient.set("/restapi/batches/notify", {"notes": msg + self.logMsg["autoMsg"], "prepid": batchId})
                self.logger.info("Notification is successful")
            except Exception as error:
                self.logger.error("Failed to notify dataset invalidation")
                self.logger.error(str(error))

    def go(self) -> bool:
        """
        The function to check if the invalidator can go
        :return: True if it can go, False o/w
        """
        try:
            servicesChecker = ServicesChecker(softServices=["wtc", "jira"])
            return servicesChecker.check()

        except Exception as error:
            self.logger.error("Failed to check if Invalidator can go")
            self.logger.error(str(error))

    def run(self) -> None:
        """
        The function to run invalidator
        """
        try:
            invalidations = self._getInvalidations()
            if not invalidations:
                self.logger.info("Nothing to invalidate, returning..")
                return

            self.logger.info(self.logMsg["nInvalidations"], len(invalidations))

            for invalid in invalidations:
                name, type = invalid.get("object"), invalid.get("type")
                prepId = invalid.get("prepid")
                try:
                    if type == "request":
                        invalidated = self._invalidateWorkflow(name)
                    elif type == "dataset":
                        invalidated = self._invalidateDataset(name)
                    else:
                        self.logger.info("%s type not recognized", type)
                        continue

                    if invalidated:
                        self.logger.info("Rejection/Invalidation is successful, acknowledging..")
                        self._acknowledge(invalid['_id'])
                        self._writeNotificationText(
                            prepId,
                            name if type == "request" else prepId,
                            self.logMsg[f"{'wf' if type == 'request' else 'dataset'}Acknowledgment"].format(
                                name, prepId
                            ),
                        )

                except Exception as error:
                    self.logger.error("Failed to invalidate %s", name)
                    self.logger.error(str(error))

            self.logger.info("Rejections/Invalidations are finished. Starting notifications.")
            self._notifyWorkflowsInvalidation()
            self._notifyDatasetsInvalidation()
            self.logger.info("Notifications are complete, halting")

        except Exception as error:
            self.logger.error("Failed to run invalidation")
            self.logger.error(str(error))


if __name__ == "__main__":
    invalidator = Invalidator()
    if invalidator.go():
        invalidator.run()
