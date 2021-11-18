import optparse
from logging import Logger
from collections import defaultdict

from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Logging import getLogger
from MongoControllers.ModuleLockController import ModuleLockController
from Services.ServicesChecker import ServicesChecker
from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Services.DBS.DBSReader import DBSReader
from Databases.Oracle.OracleClient import OracleClient
from Databases.Oracle.OracleDB import Workflow
from WorkflowMgmt.WorkflowController import WorkflowController

from Unified.Rejector import Rejector

from typing import Optional, List, Tuple
from pprint import pformat
import traceback
import os

class Injector(OracleClient):
    """
    _Injector_
    General API for injecting workflows
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__(self)
            self.logger = logger or getLogger(self.__class__.__name__)

            self.options, self.specificWf = kwargs.get("options"), kwargs.get("specificWf")
            if self.options is None:
                self.options, self.specificWf = self.parseOptions()

            unifiedConfiguration = ConfigurationHandler("config/unifiedConfiguration.json")
            self.users = {
                "pnr": unifiedConfiguration.get("pnr_users"),
                "rereco": unifiedConfiguration.get("rereco_users"),
                "relval": unifiedConfiguration.get("relval_users"),
            }

            self.reqmgrReader = ReqMgrReader()
            self.dbsReader = DBSReader()

            self.statusCache = defaultdict(str)

            self.logMsg = {
                "family": "%s has %d family members and %d true family members: %s",
                "badFamilyStatus": "Duplicate check is failed. The following workflow %s cannot be injected because of %s %s",
                "badInputStatus": "One of the inputs of %s is not VALID: %s %s",
                "nWfs": "%d in line",
                "addWf": "Adding %s",
                "duplicate": "The workflow %s cannot be added in because of duplicates",
                "noReplacement": "The workflow was found in trouble with no replacement",
                "manyReplacements": "Multiple wfs for replacement, will take the last one for %s",
                "foundReplacement": "The workflow was found in trouble and has a replacement: %s",
                "putReplacement": "Putting %s as replacement of %s",
                "forgetRelval": "As a relval, there is no clean way to handle this. Setting forget",
                "conversionError": "Error in converting %s",
                "backfill": "Keeping only backfill workflows (i.e. running in test mode)"
            }

        except Exception as error:
            raise Exception(f"Error initializing Injector\n{str(error)}")

    @staticmethod
    def parseOptions() -> Tuple[dict, Optional[str]]:
        """
        The function to parse the Injector's options and specific workflow
        :return: options and the specific workflow, if any
        """
        parser = optparse.OptionParser()

        parser.add_option("-w", "--wmStatus", help="From which status in req-mgr", default="assignment-approved")
        parser.add_option("-s", "--setStatus", help="What status to set locally", default="staged")
        parser.add_option("-u", "--user", help="What user to fetch workflow from", default="pdmvserv")
        parser.add_option("--noConvert", help="Prevent the conversion to stepchain", default=False)
        parser.add_option(
            "-r", "--replace", help="The workflow name that should be used for replacement", default=None
        )
        parser.add_option(
            "-m", "--manual", help="Manual inject, bypassing lock check", action="store_true", default=False
        )
        parser.add_option("--backfill", help="To run in test mode (only with backfill workflows)", action="store_true", default=False)

        options, args = parser.parse_args()
        return vars(options), args[0] if args else None

    def _filterBackfills(self, workflows: List[str]) -> List[str]:
        """
        The function to filter only backfill workflows
        :workflows: workflows names
        :return: backfill workflows names
        """
        if self.options.get("backfill"):
            self.logger.info(self.logMsg["backfill"])
            return [wf for wf in workflows if "backfill" in wf.lower()]
        return workflows

    def _getWorkflowsByWMStatus(self) -> List[str]:
        """
        The function to get the list of workflows by defined wmstatus and allowed users
        :return: workflows names
        """
        workflows = self.reqmgrReader.getWorkflowsByStatus(self.options.get("wmStatus"), user=self.options.get("user"))

        userRequestTypePairs = [
            (self.users["rereco"], "ReReco"),
            (self.users["relval"], "TaskChain"),
            (self.users["pnr"], "TaskChain"),
            (self.users["pnr"], "StepChain"),
        ]

        for users, requestType in userRequestTypePairs:
            for user in users:
                workflows.extend(
                    self.reqmgrReader.getWorkflowsByStatus(
                        self.options.get("wmStatus"), user=user, requestType=requestType
                    )
                )
        
        self.logger.info(self.logMsg["nWfs"], len(workflows))
        return workflows

    def _getWorkflowFamily(self, wfController: WorkflowController) -> List[Workflow]:
        """
        The function to get the family for a given workflow
        :param wfController: workflow controller
        :return: list of family members
        """
        family = self.session.query(Workflow).filter(Workflow.name.contains(wfController.request.get("PrepID"))).all()

        if not family:
            prepIds = wfController.getPrepIds()

            possibleFamily = []
            for prepId in prepIds:
                possibleFamily.extend(self.reqmgrReader.getWorkflowsByPrepId(prepId, details=True))

            for member in possibleFamily:
                memberWfController = WorkflowController(member.get("RequestName"), request=member)
                if set(prepIds) == set(memberWfController.getPrepIds()):
                    family.extend(
                        self.session.query(Workflow).filter(Workflow.name == member.get("RequestName")).all()
                    )

        return family

    def _getWorkflowReplacementName(self, wf: str, prepId: str) -> Optional[str]:
        """
        The function to get the replacement workflow name for a given workflow
        :param wf: workflow name
        :param prepId: prep id
        :return: replacement workflow name if any, None o/w
        """
        replacements = []

        possibleReplacements = self.reqmgrReader.getWorkflowsByPrepId(prepId)
        for candidate in possibleReplacements:
            if self._canReplaceWorkflow(candidate, wf):
                replacements.append(candidate)

        if not replacements:
            return None

        self.logger.info(
            self.logMsg["family"], wf, len(possibleReplacements), len(replacements), ", ".join(replacements)
        )
        if len(replacements) > 1:
            self.logger.info(self.logMsg["manyReplacements"], wf.name)

        return replacements[-1]

    def _getReplacementWorkflow(self, wf: str, currentStatus: str) -> Workflow:
        """
        The function to get the replacement workflow
        :param wf: replacement workflow name
        :param currentStatus: current replacement workflow status
        :return: workflow
        """
        replacement = self.session.query(Workflow).filter(Workflow.name == wf).first()
        if not replacement:
            replacement = Workflow(
                name=wf, status="staged" if currentStatus == "assignment-approved" else "away", wm_status=currentStatus
            )
            self.session.add(replacement)

        return replacement

    def _canInjectWorkflow(self, wfController: WorkflowController) -> bool:
        """
        The function to check if a workflow can be injected
        :param wfController: workflow controller
        :return: True if workflow can be injected, False o/w
        """
        return not self._hasBadFamilyStatus(wfController) and not self._hasBadInputStatus(wfController)

    def _hasBadFamilyStatus(self, wfController: WorkflowController) -> bool:
        """
        The function to check if any family member of a given workflow has a status that prevent injection
        :param wfController: workflow controller
        :return: True if any bad family status, False o/w
        """
        family = self._getWorkflowFamily(wfController)
        for member in family:
            if member and member.status not in ["forget", "trouble", "forget-unlock", "forget-out-unlock"]:
                self.logger.critical(
                    self.logMsg["badFamilyStatus"], wfController.request.get("RequestName"), member.name, member.status
                )
                return True

        return False

    def _hasBadInputStatus(self, wfController: WorkflowController) -> bool:
        """
        The function to check if input of a given workflow has a status that prevent injection
        :param wfController: workflow controller
        :return: True if any bad input status, False o/w
        """
        _, primaries, parents, secondaries = wfController.request.getIO()
        for dataset in primaries | parents | secondaries:
            if dataset not in self.statusCache:
                self.statusCache[dataset] = self.dbsReader.getDBSStatus(dataset)
            if self.statusCache[dataset] != "VALID":
                self.logger.critical(
                    self.logMsg["badInputStatus"],
                    wfController.request.get("RequestName"),
                    dataset,
                    self.statusCache[dataset],
                )
                return True

        return False

    def _canConvertWorkflowToStepChain(self, wfController: WorkflowController) -> bool:
        """
        The function to check if a workflow can be converted to step chain
        :param wfController: workflow controller
        :return: True if workflow can be converted, False o/w
        """
        goodForStepChain = wfController.request.isGoodToConvertToStepChain()
        self.logger.info(f"Stepchain criteria: General conversion flag: { not self.options.get('noConvert')}")
        return not self.options.get("noConvert") and goodForStepChain

    def _convertWorkflowsToStepChain(self, wfControllers: set) -> None:
        """
        The function to convert to step chain a given set of workflows
        :param wfControllers: workflows controllers
        """
        options = {"clone": True, "toStepchain": True, "comments": "Transform to StepChain"}

        for wfController in wfControllers:
            try:
                options["specific"] = wfController.request.get("RequestName")
                rejector = Rejector(options=options)
                rejector.run()

            except Exception as error:
                wfController.logger.critical(self.logMsg["conversionError"], wfController.request.get("RequestName"))
                self.logger.error(self.logMsg["conversionError"], wfController.request.get("RequestName"))
                self.logger.error(str(error))

    def _canReplaceWorkflow(self, candidate: str, wf: str) -> bool:
        """
        The function to check if a replacement candidate can really replace a workflow
        :param candidate: replacement candidate name
        :param wf: workflow name
        :return: True if it can replace, False o/w
        """
        if candidate == wf or (self.options.get("replace") and candidate == self.options.get("replace")):
            return False

        candidateSchema = self.reqmgrReader.getWorkflowSchema(candidate)
        return (
            candidateSchema["RequestDate"] > candidateSchema["RequestDate"]
            and candidateSchema["RequestType"] != "Resubmission"
            and str(candidateSchema["RequestStatus"])
            not in ["None", "new", "rejected", "rejected-archived", "aborted", "aborted-archived"]
        )

    def _replaceTroubleWorkflows(self) -> None:
        """
        The function to replace trouble workflows
        """
        troubleWorkflows = self.session.query(Workflow).filter(Workflow.status == "trouble").all()
        for wf in troubleWorkflows:
            if self.specificWf and self.specificWf not in wf.name:
                continue

            workflowController = WorkflowController(wf.name)

            replacement = self._getWorkflowReplacementName(wf.name, workflowController.request.get("PrepID"))
            if replacement is None:
                workflowController.logger.critical(self.logMsg["noReplacement"])
                if workflowController.request.isRelVal():
                    workflowController.logger.critical(self.logMsg["forgetRelval"])
                    wf.status = "forget"
                    self.session.commit()
                continue

            workflowController.logger.critical(self.logMsg["foundReplacement"], replacement)
            replacementWf = self._getReplacementWorkflow(replacement["RequestName"], replacement["RequestStatus"])
            if replacementWf.status == "forget":
                continue

            self.logger.info(self.logMsg["putReplacement"], replacement["RequestName"], wf.name)
            wf.status = "forget"

        self.session.commit()

    def go(self) -> bool:
        """
        The function to check if the injector can go
        :return: True if it can go, False o/w
        """
        try:
            moduleLockController = ModuleLockController()
            servicesChecker = ServicesChecker(softServices=["mcm", "wtc", "jira"])

            return (self.options.get("manual") or not moduleLockController.isLocked()) and servicesChecker.check()

        except Exception as error:
            self.logger.error("Failed to check if Injector can go")
            self.logger.error(str(error))

    def run(self) -> None:
        """
        The function to run injection
        """
        try:
            wfsToConvert = set()

            workflows = self._filterBackfills(self._getWorkflowsByWMStatus())
            self.logger.info(f"Workflows to process: \n {pformat(workflows)}")
            for wf in workflows:
                if self.specificWf and self.specificWf not in wf:
                    continue

                wfExists = self.session.query(Workflow).filter(Workflow.name == wf).first()
                if not wfExists:
                    self.logger.info(f"Current workflow to inject: {wf}")
                    wfController = WorkflowController(wf)
                    if not self._canInjectWorkflow(wfController):
                        self.logger.critical(self.logMsg["duplicate"], wf)
                        continue

                    self.logger.info(f"Stepchain eligibility check for {wf}")
                    if self._canConvertWorkflowToStepChain(wfController):
                        wfsToConvert.add(wfController)
                        self.logger.info(f"The following workflow is eligible to be a stepchain: {wf}")
                    else:
                        self.logger.info(f"The following workflow is NOT eligible to be a stepchain: {wf}")

                    self.logger.info("Inserting the workflow into OracleDB")
                    self.session.add(
                        Workflow(name=wf, status=self.options.get("setStatus"), wm_status=self.options.get("wmStatus"))
                    )
                    self.session.commit()
                    self.logger.info("Insertion is successful")

            self.logger.info("Injection process has ended for all workflows. Conversion process starts for eligible ones")
            self._convertWorkflowsToStepChain(wfsToConvert)
            #self._replaceTroubleWorkflows()

        except Exception as error:
            self.logger.error("Failed to run injection")
            self.logger.error(str(error))
            self.logger.error(traceback.format_exc())


if __name__ == "__main__":
    options, specificWf = Injector.parseOptions()
    injector = Injector(options=options, specificWf=specificWf)
    if injector.go():
        injector.run()
    else:
        logger = getLogger("Injector")
        logger.critical("Injector isn't allowed run") # Improve logging: explain why
