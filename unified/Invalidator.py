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
            self.invalidatedBatches = defaultdict(str)
            self.invalidatedRequests = defaultdict(str)

            self.mcmClient = McMClient()
            self.dbs = {"writer": DBSWriter(), "reader": DBSReader()}

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
            self.logger.info("Setting the status of %s to forget", wf)
            workflow.status = "forget"
            self.session.commit()

        wfStatusEnforcer = WorkflowStatusEnforcer(wf)
        invalidated = wfStatusEnforcer.invalidate(onlyResubmissions=True, invalidateOutputDatasets=False)

        wfController = WorkflowController(wf)
        wfController.logger.info("Rejection is performed from McM invalidations request")

        return invalidated

    def _invalidateDataset(self, dataset: str) -> bool:
        """
        The function to invalidate a given dataset
        :param dataset: dataset name
        :return: True if the dataset was properly invalidated, False o/w
        """
        if any(word in dataset for word in ["?", "None", "FAKE-"]):
            return False

        self.logger.info("Setting the status of %s to %s", dataset, self.invalidStatus)

        currentStatus = self.dbs["reader"].getDBSStatus(dataset)
        invalidated = self.dbs["writer"].setDatasetStatus(dataset, currentStatus, self.invalidStatus)
        if not invalidated:
            self.logger.critical(
                "Could not invalidate %s. Please consider contacting data management team for manual intervention.",
                dataset,
            )

        return invalidated

    def _writeInvalidationAcknowledgement(self) -> None:
        pass

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
                return

            self.logger.info("%s objects to be invalidated", len(invalidations))

            for invalid in invalidations:
                try:
                    if invalid.get("type") == "request":
                        invalidated = self._invalidateWorkflow(invalid.get("object"))
                    elif invalid.get("type") == "dataset":
                        invalidated = self._invalidateDataset(invalid.get("object"))
                    else:
                        self.logger.info("%s type not recognized", invalid.get("type"))
                        continue

                    if invalidated:
                        pass

                except Exception as error:
                    self.logger.error("Failed to invalidate %s", invalid.get("object"))
                    self.logger.error(str(error))

        except Exception as error:
            self.logger.error("Failed to run invalidation")
            self.logger.error(str(error))


if __name__ == "__main__":
    invalidator = Invalidator()
    if invalidator.go():
        invalidator.run()
