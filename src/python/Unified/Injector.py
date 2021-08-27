import os
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
from Databases.Oracle.OracleDB import Workflow, TransferImp
from WorkflowMgmt.WorkflowController import WorkflowController

from typing import Optional, List, Tuple


class Injector(OracleClient):
    """
    _Injector_
    General API for injecting workflows
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__(self)
            self.logger = logger or getLogger(self.__class__.__name__)

            self.options, self.spec = kwargs.get("options"), kwargs.get("spec")
            if self.options is None:
                self.options, self.spec = self.parseOptions()

            unifiedConfiguration = ConfigurationHandler("config/unifiedConfiguration.json")
            self.users = {
                "rereco": unifiedConfiguration.get("user_rereco"),
                "relval": self.options.get("user_relval").split(",") or unifiedConfiguration.get("user_relval"),
                "storeResults": self.options.get("user_storeresults").split(",")
                or unifiedConfiguration.get("user_storeresults"),
            }

            self.reqmgrReader = ReqMgrReader()
            self.dbsReader = DBSReader()

            self.statusCache = defaultdict(str)
            self.useMcm = False

            self.logMsg = {
                "family": "%s has %d family members and %d true family members: %s",
                "badFamilyStatus": "Should not put %s because of %s %s",
                "badInputStatus": "One of the inputs of %s is not VALID: %s %s",
                "nWfs": "%d in line",
                "noReplacement": "The workflow was found in trouble with no replacement",
                "manyReplacements": "Multiple wfs for replacement, will take the last one for %s",
                "foundReplacement": "The workflow was found in trouble and has a replacement: %s",
                "putReplacement": "Putting %s as replacement of %s",
                "forgetRelval": "As a relval, there is no clean way to handle this. Setting forget",
            }

        except Exception as error:
            raise Exception(f"Error initializing Injector\n{str(error)}")

    @staticmethod
    def parseOptions() -> Tuple[dict, Optional[str]]:
        """
        The function to parse the Injector's options and specific pattern in workflow
        :return: options and spec, if any
        """
        parser = optparse.OptionParser()

        parser.add_option("-w", "--wmstatus", help="From which status in req-mgr", default="assignment-approved")
        parser.add_option("-s", "--setstatus", help="What status to set locally", default="staged")
        parser.add_option("-u", "--user", help="What user to fetch workflow from", default="pdmvserv")
        parser.add_option(
            "-r", "--replace", help="The workflow name that should be used for replacement", default=None
        )
        parser.add_option("--user_relval", help="The user that can inject workflows for relvals", default=None)
        parser.add_option(
            "--user_storeresults", help="The user that can inject workflows for store results", default=None
        )
        parser.add_option("--no_convert", help="Prevent the conversion to stepchain", default=False)
        parser.add_option(
            "-m",
            "--manual",
            help="Manual inject, bypassing lock check",
            action="store_true",
            dest="manual",
            default=False,
        )

        options, args = parser.parse_args()
        return vars(options), args[0] if args else None

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
                possibleFamily.extend(self.reqmgrReader.getWorkflowsByPrepId(prepId, deatils=True))

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
        family = self._getWorkflowFamily(self, wfController)
        for member in family:
            if member and member.status not in ["forget", "trouble", "forget-unlock", "forget-out-unlock"]:
                self.logger.critical(
                    self.logMsg["badFamilyStatus"], wfController.request.get("RequestName"), member.name, member.status
                )
                return False

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
                return False

        return True

    def _canConvertWorkflowToStepChain(self, wfController: WorkflowController) -> bool:
        """
        The function to check if a workflow can be converted to step chain
        :param wfController: workflow controller
        :return: True if workflow can be converted, False o/w
        """
        goodForStepChain = wfController.request.isGoodToConvertToStepChain()
        return not self.options.get("no_convert") and not wfController.request.isRelVal() and goodForStepChain

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

    def _updateWorkflowTransfer(self, currentWfId: str, replacementWf: Workflow) -> None:
        """
        The function to update the workflow transfer working by the workflow replacement
        :param currentWfId: current workflow id
        :param replacementWf: replacement workflow
        """
        transfers = self.session.query(TransferImp).filter(TransferImp.workflow_id == currentWfId).all()

        for transfer in transfers:
            transfer.active = False
            if (
                not self.session.query(TransferImp)
                .filter(TransferImp.phedexid == transfer.phedexid)
                .filter(TransferImp.workflow_id == replacementWf.id)
                .all()
            ):
                self.session.add(TransferImp(phedexid=transfer.phedexid, workflow=replacementWf))

            self.session.commit()

    def go(self) -> bool:
        """
        The function to check if the injector can go
        :return: True if it can go, False o/w
        """
        try:
            moduleLockController = ModuleLockController()

            servicesChecker = ServicesChecker(softServices=["mcm", "wtc", "jira"])
            checkedServices = servicesChecker.check()
            self.useMcm = servicesChecker.status.get("mcm")

            return (self.options.get("manual") or not moduleLockController.isLocked()) and checkedServices

        except Exception as error:
            self.logger.error("Failed to check if Injector can go")
            self.logger.error(str(error))

    def getWorkflowsByWMStatus(self) -> List[str]:
        """
        The function to get the list of workflows by defined by wmstatus
        :return: workflows names
        """
        try:
            workflows = self.reqmgrReader.getWorkflowsByStatus(
                self.options.get("wmstatus"), user=self.options.get("user")
            )

            for users, requestType in [
                (self.users["rereco"], "ReReco"),
                (self.users["relval"], "TaskChain"),
                (self.users["storeResults"], "StoreResults"),
            ]:
                workflows.extend(
                    self.reqmgrReader.getWorkflowsByStatus(
                        self.options.get("wmstatus"), user=user, requestType=requestType
                    )
                    for user in users
                )

            self.logger.info(self.logMsg["nWfs"], len(workflows))
            return workflows

        except Exception as error:
            self.logger.error("Failed to get workflows")
            self.logger.error(str(error))

    def convertWorkflowsToStepChain(self, workflows: set) -> None:
        """
        The function to convert to step chain a given set of workflows
        :param workflows: workflows names
        """
        try:
            for wf in workflows:
                # TODO: implement rejector
                pass

        except Exception as error:
            self.logger.error("Failed to run rejector to convert workflows %s to stepchain", workflows)
            self.logger.error(str(error))

    def invalidate(self) -> None:
        """
        The function to invalidate what needs to be invalidated
        """
        try:
            if self.useMcm:
                # TODO:  implement invalidator
                pass

        except Exception as error:
            self.logger.error("Failed to run invalidator")
            self.logger.error(str(error))

    def replaceTroubleWorkflows(self) -> None:
        """
        The function to replace trouble workflows
        """
        try:
            troubleWorkflows = self.session.query(Workflow).filter(Workflow.status == "trouble").all()
            for wf in troubleWorkflows:
                if self.spec and self.spec not in wf.name:
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

                self._updateWorkflowTransfer(wf.id, replacementWf)
                wf.status = "forget"

            self.session.commit()

        except Exception as error:
            self.logger.error("Failed to replace trouble workflows")
            self.logger.error(str(error))

    def run(self) -> None:
        """
        The function to run injection
        """
        try:
            workflowsToConvert = set()
            for wf in self.getWorkflowsByWMStatus():
                if self.spec and self.spec not in wf:
                    continue

                wfExists = self.session.query(Workflow).filter(Workflow.name == wf).first()
                if not wfExists:
                    wfController = WorkflowController(wf)
                    if not self._canInjectWorkflow(wfController):
                        self.logger.critical("The workflow %s cannot be added in because of duplicates", wf)
                        continue

                    if self._canConvertWorkflowToStepChain(wfController):
                        workflowsToConvert.add(wf)

                    self.logger.info("Considering %s", wf)
                    self.session.add(
                        Workflow(name=wf, status=self.options.get("setstatus"), wm_status=self.options.get("wmstatus"))
                    )
                    self.session.commit()

            self.convertWorkflowsToStepChain(workflowsToConvert)
            self.invalidate()
            self.replaceTroubleWorkflows()

        except Exception as error:
            self.logger.error("Failed to run injection")
            self.logger.error(str(error))


if __name__ == "__main__":
    options, spec = Injector.parseOptions()
    injector = Injector(options=options, spec=spec)
    if injector.go():
        injector.run()
