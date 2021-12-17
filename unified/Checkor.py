import os
import re
import optparse
import random
import math
from logging import Logger
from collections import defaultdict
from time import mktime, asctime, gmtime, struct_time

from Utilities.DataTools import unnestList, flattenDictKeys
from Utilities.Decorators import runWithMultiThreading
from Utilities.Logging import getLogger
from Utilities.ConfigurationHandler import ConfigurationHandler
from Cache.DataCacheLoader import DataCacheLoader
from Databases.Oracle.OracleClient import OracleClient
from Databases.Oracle.OracleDB import Workflow
from MongoControllers.CampaignController import CampaignController
from MongoControllers.CloseoutController import CloseoutController
from MongoControllers.WTCController import WTCController
from Services.ServicesChecker import ServicesChecker
from Services.McM.McMClient import McMClient
from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Services.ReqMgr.ReqMgrWriter import ReqMgrWriter
from Services.ACDC.ACDCReader import ACDCReader
from Services.DBS.DBSReader import DBSReader
from Services.DBS.DBSWriter import DBSWriter
from Services.Rucio.RucioReader import RucioReader
from WorkflowMgmt.SiteController import SiteController
from WorkflowMgmt.UserLockChecker import UserLockChecker
from WorkflowMgmt.WorkflowController import WorkflowController
from WorkflowMgmt.WorkflowStatusEnforcer import WorkflowStatusEnforcer

from typing import Optional, Tuple, Union


class Checkor(OracleClient):
    """
    __Checkor__
    General API for checking workflows in completed status
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__(self)
            self.logger = logger or getLogger(self.__class__.__name__)

            self.options, self.specificWf = kwargs.get("options"), kwargs.get("specificWf")
            if self.options is None:
                self.options, self.specificWf = self.parseOptions()

            now = mktime(gmtime())
            self.timePoint = {"lap": now, "subLap": now, "start": now}

            self.unifiedConfiguration = ConfigurationHandler("config/unifiedConfiguration.json")

            self.acdcReader = ACDCReader()
            self.dbs = {"reader": DBSReader(), "writer": DBSWriter()}
            self.reqMgr = {"reader": ReqMgrReader(), "writer": ReqMgrWriter()}

            self.dataCacheLoader = DataCacheLoader()

            self.campaignController = CampaignController()
            self.closeoutController = CloseoutController()
            self.siteController = SiteController()
            self.wtcController = WTCController()

            self.overrideWfs = None
            self.onHoldWfs = None
            self.bypassWfs = None
            self.forceCompleteWfs = None

            self.jiraClient = None
            self.mcmClient = None

        except Exception as error:
            raise Exception(f"Error initializing Checkor\n{str(error)}")

    @staticmethod
    def parseOptions() -> Tuple[dict, Optional[str]]:
        """
        The function to parse the Checkor's options and specific workflow
        :return: options and the specific workflow, if any
        """
        parser = optparse.OptionParser()

        parser.add_option("--go", help="Does not check on duplicate process", action="store_true", default=False)
        parser.add_option(
            "--update",
            help="Running workflows that have not yet reached completed",
            action="store_true",
            default=False,
        )
        parser.add_option(
            "--strict", help="Only running workflows that reached completed", action="store_true", default=False
        )
        parser.add_option(
            "--clear", help="Only running workflows that have reached custodial", action="store_true", default=False
        )
        parser.add_option(
            "--review",
            help="Look at the workflows that have already completed and had required actions",
            action="store_true",
            default=False,
        )
        parser.add_option(
            "--recovering",
            help="Look at the workflows that already have on-going acdc",
            action="store_true",
            default=False,
        )
        parser.add_option("--manual", help='Look at the workflows in "manual"', action="store_true", default=False)
        parser.add_option("--limit", help="The number of workflows to consider for checking", default=0, type=int)
        parser.add_option("--threads", help="The number of threads for processing workflows", default=10, type=int)
        parser.add_option(
            "--fractionPass", help="The completion fraction that is permitted", default=0.0, type="float"
        )
        parser.add_option("--lumiSize", help="Force the upper limit on lumisection", default=0, type="float")
        parser.add_option(
            "--ignoreFiles", help="Force ignoring dbs/rucio differences", action="store_true", default=False
        )
        parser.add_option(
            "--ignoreInvalid", help="Force ignoring high level of invalid files", action="store_true", default=False
        )
        parser.add_option(
            "--ignoreDuplicates", help="Force ignoring lumi duplicates", action="store_true", default=False
        )
        parser.add_option("--tapeSizeLimit", help="The limit in size of all outputs", default=0, type=int)
        parser.add_option("--html", help="Build the the monitor page", action="store_true", default=False)
        parser.add_option(
            "--noReport", help="Prevent from making the error report", action="store_true", default=False
        )
        parser.add_option(
            "--backfill", help="To run in test mode (only with backfill workflows)", action="store_true", default=False
        )

        options, args = parser.parse_args()
        options = vars(options)

        options["manual"] = not options.get("recovering")
        actions = ["strict", "update", "clear", "review"]
        if all(not options.get(option) for option in actions):
            for option in actions + ["recovering", "manual"]:
                options[option] = True

        return options, args[0] if args else None

    def _setWfs(self) -> None:
        """
        The function to set workflows to override, on hold, bypass and force complete.
        """
        self.overrideWfs = self._getWorkflowsByAction("force-complete", details=True)
        self.onHoldWfs = self._getWorkflowsByAction("hold")
        self.bypassWfs = self._getWorkflowsByAction("bypass") + unnestList(self.overrideWfs)
        self.forceCompleteWfs = self.mcmClient.get("/restapi/requests/forcecomplete") if self.mcmClient is not None else {}

        if self.jiraClient is not None:
            # TODO: update wfs by JIRA tickets / migrate JiraClient
            pass

    def _checkPoint(self, label: str = "", subLap: bool = False, now: struct_time = gmtime()) -> None:
        """
        The function to get the check points
        :label: log message label
        :subLap: True if for sub lap, False o/w
        :now: time now
        """
        self.logger.info("Time check (%s) point at: %s", label, asctime(now))
        self.logger.info("Since start: %s [s]", now - self.timePoint.get("start", now))

        self.logger.info(
            "%s: %s [s]",
            "Sub lap" if subLap else "Lap",
            now - self.timePoint.get("subLap" if subLap else "lap", now),
        )
        self.timePoint["subLap"] = now
        if not subLap:
            self.timePoint["lap"] = now

    def _filterBackfills(self, workflows: list) -> list:
        """
        The function to filter only backfill workflows
        :workflows: workflows
        :return: backfill workflows
        """
        if self.options.get("backfill"):
            self.logger.info(self.logMsg["backfill"])
            return [wf for wf in workflows if "backfill" in wf.name.lower()]
        return workflows

    def _getWorkflowsToCheck(self) -> list:
        """
        The function to get the workflows to check
        :return: workflows
        """
        workflows = set()

        awayWfs = self.session.query(Workflow).filter(Workflow.status == "away").all()
        assistanceWfs = self.session.query(Workflow).filter(Workflow.status.startswith("assistance")).all()
        completedWfs = self.reqMgr["reader"].getWorkflowsByStatus("completed")

        if self.options.get("strict"):
            self.logger.info("Strict option is on: checking workflows that freshly completed")
            workflows.update(filter(lambda wf: wf.name in completedWfs, awayWfs))

        if self.options.get("update"):
            self.logger.info("Update option is on: checking workflows that have not completed yet")
            workflows.update(filter(lambda wf: wf.name not in completedWfs, awayWfs))

        if self.options.get("clear"):
            self.logger.info("Clear option is on: checking workflows that are ready to toggle closed-out")
            workflows.update(filter(lambda wf: "custodial" in wf.status, assistanceWfs))

        if self.options.get("review"):
            nonCustodialWfs = [*filter(lambda wf: "custodial" not in wf.status, assistanceWfs)]
            if self.options.get("recovering"):
                self.logger.info("Review-recovering option is on: checking only the workflows that had been already acted on")
                workflows.update(filter(lambda wf: "manual" not in wf.status, nonCustodialWfs))
            if self.options.get("manual"):
                self.logger.info("Review-manual option is on: checking the workflows to be acted on")
                workflows.update(filter(lambda wf: "manual" in wf.status, nonCustodialWfs))

        return list(workflows)

    def _getWorkflowsByAction(self, action: str, details: bool = False) -> Union[list, dict]:
        """
        The function to get workflows for a given user action
        :action: user action
        :details: if True return dict of workflows by user. O/w return workflows names
        :return: workflows
        """
        workflows = defaultdict(set)

        allowedUsers = [*self.unifiedConfiguration.get("allowed_bypass", {}).keys()]

        userWfs = (
            self.wtcController.getHold()
            if action == "hold"
            else self.wtcController.getBypass()
            if action == "bypass"
            else self.wtcController.getForce()
            if action == "force-complete"
            else {}
        )
        for user, wf in userWfs.items():
            if user not in allowedUsers:
                self.logger.info("%s is not allowed to %s", user, action)
                continue

            self.logger.info("%s allowed to %s %s", user, action, wf)
            workflows[user].add(wf)

        if details:
            return workflows
        return unnestList(workflows)

    def _filterMaxNumberOfWorkflows(self, workflows: list) -> list:
        """
        The function to filter the max number of workflows per round
        :param workflows: list of workflows
        :return: filtered list of workflows
        """
        maxPerRound = self.unifiedConfiguration.get("max_per_round", {}).get("checkor")
        if self.options.get("limit"):
            self.logger.info("Command line to limit workflows to %s", self.options.get("limit"))
            maxPerRound = self.options.get("limit")

        if maxPerRound and not self.specific:
            self.logger.info("Limiting workflows to %s this round", maxPerRound)

            workflows = self._rankWorkflows(workflows)
            if self.option.get("update"):
                random.shuffle(workflows)
            return workflows[:maxPerRound]

        return workflows

    def _rankWorkflows(self, workflows: list) -> list:
        """
        The function to rank the workflows by their priority
        :param workflows: workflows
        :return: sorted workflows
        """
        completedWfs = self.reqMgr["reader"].getWorkflowsByStatus("completed", details=True)
        completedWfs = sorted(completedWfs, key=lambda wf: wf.get("RequestPriority", 0))
        completedWfs = [wf.get("RequestName") for wf in completedWfs]

        return sorted(workflows, key=lambda wf: completedWfs.index(wf) if wf in completedWfs else 0, reverse=True)

    def _updateWorkflowsRecords(self, wfsRecords: dict) -> None:
        """
        The function to update the workflows records
        :param wfsRecords: records
        """
        for wf in wfsRecords:
            if wf.get("record"):
                self.closeoutController.set(wf.get("wf"), wf.get("record"))

    def _updateWorkflowsStatus(self, wfsStatus: list) -> None:
        """
        The function to update the workflows status
        :param wfsStatus: status
        """
        for wf in wfsStatus:
            if wf.get("newStatus"):
                newStatus = wf.get("newStatus")
                wf["workflow"].status = newStatus
                self.session.commit()
                
                if newStatus == "close":
                    self.closeoutController.clean(wf.get("wf"))
                    if self.mcmClient is not None and wf.get("mcmForceComplete"):
                        for prepId in wf.get("prepIds"):
                            self.mcmClient.clean(f"/restapi/requests/forcecomplete/{prepId}")
    
    def _checkExecutionTime(self, nWfs: int, now: struct_time = mktime(gmtime())) -> None:
        """
        The function to check the execution time of the module
        :param nWfs: number of workflows
        :param now: time now
        """
        if nWfs:
            avgExecutionTime = (now - self.timePoint.get("start")) / nWfs
            self.logger.info("Average time spend per workflow: %s s", avgExecutionTime)
            
            if avgExecutionTime > 120:
                self.logger.critical("Checkor took %s [s] per workflow", avgExecutionTime)

    def _countAssistanceWorkflowsByStatus(self) -> dict:
        """
        The function to count the number of assistance workflows by status
        :return: count of workflows by status
        """
        status = defaultdict(int)
        for wf in self.session.query(Workflow).filter(Workflow.status.startswith("assistance")).all():
            status[wf.status] += 1
        
        return status

    def _writeSummary(self) -> None:
        """
        The function to write a summary of checkor
        """
        if not self.specificWf:
            msg = ""
            if self.options.get("strict"):
                msg += "Workflows which just got in completed were looked at. Look in manual.\n"
            if self.options.get("update"):
                msg += "Workflows that are still running (and not completed) got looked at.\n"
            if self.options.get("clear"):
                msg += "Workflows that just need to close-out were verified. Nothing too new a-priori.\n"
            if self.options.get("review"):
                msg += "Workflows under intervention got review.\n"
            
            msg += "\n".join([f"{count} in status {status}" for status, count in self._countAssistanceWorkflowsByStatus()])

            self.logger.info(msg)

    def go(self) -> bool:
        """
        The function to check if the checkor can go
        :return: True if it can go, False o/w
        """
        try:
            userLockChecker = UserLockChecker()
            servicesChecker = ServicesChecker(softServices=["mcm", "wtc"])

            if not userLockChecker.isLocked() and servicesChecker.check():
                self.mcmClient = McMClient() if servicesChecker.status.get("mcm") else None
                self.jiraClient = None #JiraClient() if servicesChecker.status.get("jira") else None    TODO: migrate JiraClient
                return True

            return False

        except Exception as error:
            self.logger.error("Failed to check if Checkor can go")
            self.logger.error(str(error))

    def run(self) -> None:
        """
        The function to run checkor
        """
        try:
            self._setWfs()

            wfsToCheck = self._filterBackfills(self._getWorkflowsToCheck())
            random.shuffle(wfsToCheck)

            self.logger.info("Considering %s workflows (before any limitation)", len(wfsToCheck))

            wfsToCheck = self._filterMaxNumberOfWorkflows(wfsToCheck)
            self._check(wfsToCheck)

            self._writeSummary()

        except Exception as error:
            self.logger.error("Failed to run checkor")
            self.logger.error(str(error))

    def _check(self, wfsToCheck: list) -> None:
        """
        The wrapper function to check the workflows
        :param wfsToCheck: workflows to check
        """

        @runWithMultiThreading(mtParam="wfsToCheck", maxThreads=len(wfsToCheck))
        def _checkWorkflow(self, wfsToCheck: list) -> dict:
            return WorkflowCheckor(wfsToCheck, checkor=self).check()

        checkResponses = _checkWorkflow(wfsToCheck)
        self._updateWorkflowsRecords(checkResponses)
        self._updateWorkflowsStatus(checkResponses)

        self._checkExecutionTime(len(wfsToCheck))

class WorkflowCheckor(object):
    """
    __WorkflowCheckor__
    General API for checking a fiven workflow
    """

    def __init__(self, wfToCheck: Workflow, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__(self)
            self.logger = logger or getLogger(self.__class__.__name__)
            self.now = mktime(gmtime())

            self.wf = wfToCheck.name
            self.wfController = WorkflowController(self.wf)

            self.wfToCheck = wfToCheck
            self.wfToCheck.wm_status = self.wfController.request.get("RequestStatus")

            self.checkor = kwargs.get("checkor")
            self.rucioReader = RucioReader()

            self.assistanceTags, self.existingAssistaceTags = set(), set(wfToCheck.status.split("-")[1:])
            
            self.acdcs = dict()
            self.campaigns = dict()
            self.fractions = dict()
            self.percentCompletions, self.percentAvgCompletions = dict(),dict()

            self.eventsPerLumi = dict()
            self.lumiUpperLimit = dict()
            self.expectedLumis, self.expectedEvents =dict(),dict() 
            self.producedLumis, self.producedEvents = dict(),dict()

            self.outputDatasetsToCheck = list()
            self.passStatsCheck, self.passStatsCheckToAnnounce, self.passStatsCheckOverCompletion = dict(), dict(), dict()
            
            self.rucioPresence = dict()
            self.dbsPresence, self.dbsInvalid = dict(), dict()

            self.failed = False
            self.isClosing = False
            self.newStatus = None
            self._setBypassChecks()
            self._setBypassChecksByMcMForceComplete(self.wfController.getPrepIDs())
            self._setBypassChecksByUserForceComplete()

            self.record = {}

        except Exception as error:
            raise Exception(f"Error initializing Checkor\n{str(error)}")
    
    def _skipWorkflow(self) -> bool:
        """
        The function to check if a given workflow should be skipped or not
        :return: True if workflow should be skipped, False o/w
        """
        if (
            (self.checkor.specific and self.checkor.specific not in self.wf)
            or os.path.isfile(".checkor_stop")
            or self.wfToCheck.wm_status in ["assigned", "acquired"]
            or (self.wfToCheck.wm_status != "completed" and self.wf not in self.checkor.exceptions)
        ):
            self.logger.info("Skipping workflow %s", self.wf)
            return True

        return False

    def _setWorkflowToClose(self) -> bool:
        """
        The function to check if a given workflow should be closed
        :return: True if workflow should be closed, False o/w
        """
        if self.wfToCheck.wm_status in ["closed-out", "announced"] and self.wf not in self.checkor.exceptions:
            self.logger.info("%s is already %s, setting as close", self.wf, self.wfToCheck.wm_status)
            self.newStatus = "close"
            return True

        return False

    def _setWorkflowToForget(self) -> bool:
        """
        The function to check if a given workflow should be forgotten
        :return: True if workflow should be forgotten, False o/w
        """
        if self.wfController.request.isRelVal() and self.wfToCheck.wm_status in [
            "failed",
            "aborted",
            "aborted-archived",
            "rejected",
            "rejected-archived",
            "aborted-completed",
        ]:
            self.logger.info("%s is %s, but will not be set in trouble to find a replacement", self.wf, self.wfToCheck.wm_status)
            self.newStatus = "forget"
            return True

        return False

    def _setWorkflowToTrouble(self) -> bool:
        """
        The function to check if a given workflow should be set as trouble
        :return: True if workflow should be set as trouble, False o/w
        """
        if not self.wfController.request.isRelVal() and self.wfToCheck.wm_status in [
            "failed",
            "aborted",
            "aborted-archived",
            "rejected",
            "rejected-archived",
            "aborted-completed",
        ]:
            self.logger.info("%s is %s, setting as trouble", self.wf, self.wfToCheck.wm_status)
            self.newStatus = "trouble"
            return True

        return False

    def _setBypassChecks(self) -> None:
        """
        The function to check if bypass checks for a given workflow
        """
        for bypassWf in self.checkor.bypassWfs:
            if bypassWf == self.wf:
                self.logger.info(self.logMsg["bypassCheck"], self.wf, bypassWf)
                self.bypassChecks = True

        self.bypassChecks = False

    def _setBypassChecksByMcMForceComplete(self, prepIds: list) -> None:
        """
        The function to check if bypass checks for a given workflow because of McM force-complete status.
        :param prepIds: list of prep ids
        """
        for forceCompleteWf in self.checkor.forceCompleteWfs:
            if forceCompleteWf in prepIds:
                self.logger.info("Bypassing checks and force completing %s because of prep id %s", self.wf, forceCompleteWf)
                self.bypassChecksByMcMForceComplete = True
                
        self.bypassChecksByMcMForceComplete = False

    def _setBypassChecksByUserForceComplete(self) -> None:
        """
        The function to check if bypass checks for a given workflow because of user force-complete status.
        """
        for user, userOverrideWfs in self.checkor.overrideWfs.items():
            forceCompleteWfs = [overrideWf for overrideWf in userOverrideWfs if overrideWf in self.wf]
            if forceCompleteWfs:
                self.logger.info(
                    "Bypassing checks and force completing %s because of user/keyword %s/%s", self.wf, user, forceCompleteWfs[0]
                )
                self.bypassChecksByUserForceComplete = True

        self.bypassChecksByUserForceComplete =  False

    def _isWorkflowOnHold(self) -> bool:
        """
        The function to check if a given workfloe is on hold
        :return: True if workflow is on hold, False o/w
        """
        return "-onhold" in self.wfToCheck.wm_status or self.wf in self.checkor.onHoldWfs

    def _getMinFamilyCompletedDelay(self, family: list) -> float:
        """
        The function to get how long a given workflow in the family has been requested to complete.
        :param family: workflow family
        :param now: time now
        :return: how long a given family workflow has been requested to complete
        """
        completedFamily = [
            filter(lambda wf: wf.get("Status") == "completed", member["RequestTransition"]) for member in family
        ]
        return min(
            [
                (self.now - completed[-1].get("UpdateTime", self.now)) / 86400.0 if completed else 0
                for completed in completedFamily
            ]
        )


    def _setNewOnHoldStatus(self, family: list) -> bool:
        """
        The function to get on hold status for a given workflow
        :param family: workflow family
        :return: if set new status
        """
        onHoldTimeout = self.unifiedConfiguration.get("onhold_timeout")
        minFamilyCompletedDelay = self._getMinFamilyCompletedDelay(family, self.now)

        self.logger.info("On hold since %s, timeout at %s", minFamilyCompletedDelay, onHoldTimeout)

        if onHoldTimeout > 0 and onHoldTimeout < minFamilyCompletedDelay:
            self.logger.info("%s is on hold and stopped for %.2f days, letting this through with current statistics", self.wf, minFamilyCompletedDelay)
            self.bypassChecks = True
            return False

        if "-onhold" in self.wfToCheck.wm_status and self.wf in self.checkor.onHoldWfs and not self.bypassChecks:
            self.logger.info("%s is on hold", self.wf)
            return True

        if self.wf in self.checkor.onHoldWfs and not self.bypassChecks:
            self.logger.info("%s is %s, setting as on hold", self.wf, self.wfToCheck.wm_status)
            self.newStatus = "assistance-onhold"
            return True

        return False

    def _updateWorkflowStatus(self) -> bool:
        """
        The function to check if the status of a given workflow should be updated
        :return: True if update status, False o/w
        """
        if self._setWorkflowToClose() or self._setWorkflowToForget() or self._setWorkflowToTrouble():
            return True
        if self._isWorkflowOnHold() and self._setNewOnHoldStatus(self.wfController.getFamily(includeItself=True)):
            return True

        return False

    def _getTiersWithNoCheckAndCustodial(self) -> Tuple[list, list]:
        """
        The function yo get the tiers with no check and custodial
        :return: list of tiers with no check, and list of tiers with no custodial
        """
        tiersWithNoCheck = set(self.unifiedConfiguration.get("tiers_with_no_check"))
        tiersWithNoCustodial = set(
            self.unifiedConfiguration.get("tiers_with_no_custodial") if not self.wfController.request.isRelVal() else []
        )

        for campaign in self._getWorkflowCampaigns(self.wfController).values():
            if (
                campaign in self.campaignController.campaigns
                and "custodial_override" in self.campaignController.campaigns[campaign]
            ):
                if isinstance(self.campaignController.campaigns[campaign]["custodial_override"], list):
                    tiersWithNoCheck -= self.campaignController.campaigns[campaign]["custodial_override"]
                elif self.campaignController.campaigns[campaign]["custodial_override"] == "notape":
                    tiersWithNoCustodial = set(
                        sorted([dataset.split("/")[-1] for dataset in self.wfController.request.get("OutputDatasets")])
                    )

        return tiersWithNoCheck, tiersWithNoCustodial

    def _setOutputDatasetsToCheck(self) -> None:
        """
        The function to set the datasets to check
        """
        tiersWithNoCheck, tiersWithNoCustodial = self._getTiersWithNoCheckAndCustodial(self.wfController)

        expectedOutputsDatasets = self.wfController.request.get("OutputDatasets")
        self.outputDatasetsToCheck = [
            dataset
            for dataset in expectedOutputsDatasets
            if all([dataset.split("/")[-1] != tier for tier in tiersWithNoCheck])
        ]

        self.wfController.logger.debug(
            "Initial outputs: %s\nWill check on: %s\nTiers out: %s\nTiers no custodials: %s",
            ", ".join(sorted(expectedOutputsDatasets)),
            ", ".join(sorted(self.outputDatasetsToCheck)),
            ", ".join(sorted(tiersWithNoCheck)),
            ", ".join(sorted(tiersWithNoCustodial)),
        )

    def _skipFamilyWorkflow(self, wfSchema: dict) -> bool:
        """
        The function to check if the workflow family should be skipped or not
        :param wfSchema: workflow schema
        :return: True if to skip, False o/w
        """
        return (
            wfSchema.get("RequestType") != "Ressubmission"
            or wfSchema.get("RequestDate") < wfSchema.request.get("RequestDate")
            or wfSchema.get("PrepID") != wfSchema.request.get("PrepID")
            or wfSchema.get("RequestStatus") == None
        )


    def _getWorkflowFamily(self) -> list:
        """
        The function to get the workflow family
        :return: workflow family
        """
        family = []
        for member in self.reqMgr["reader"].getWorkflowsByPrepId(self.wfController.request.get("PrepID"), details=True):
            if (
                member.get("RequestName") == self.wf
                or self._skipFamilyWorkflow(member)
                or not set(member.get("OutputDatasets", [])).issubset(set(self.wfController.request.get("OutputDatasets")))
            ):
                continue

            family.append(member)

        return family

    def _setBadAcdcs(self) -> None:
        """
        The function to set the bad acdcs
        """
        badAcdcs = []
        for member in self.reqMgr["reader"].getWorkflowsByPrepId(self.wfController.request.get("PrepID"), details=True):
            if member.get("RequestName") == self.wf or self._skipFamilyWorkflow(member):
                continue

            if not set(member.get("OutputDatasets", [])).issubset(set(self.wfController.request.get("OutputDatasets"))):
                if member.get("RequestStatus") not in [
                    "rejected-archived",
                    "rejected",
                    "aborted",
                    "aborted-archived",
                ]:
                    badAcdcs.append(member.get("RequestName"))
                    self.wfController.logger.info("Inconsistent ACDC %s", member.get("RequestName"))

        self.acdcs["bad"] = badAcdcs
        if badAcdcs:
            self.assistanceTags.update(["manual", "assistance"])

    def _setHealthyAcdcs(self, family: list) -> None:
        """
        The function to set healthy acdcs
        :param family: workflow family
        """
        healthyAcdcs = []
        for member in family:
            if member.get("RequestStatus") in [
                "running-open",
                "running-closed",
                "assigned",
                "acquired",
                "staging",
                "staged",
            ]:
                self.logger.info("%s still has an ACDC running", member.get("RequestName"))
                healthyAcdcs.append(member.get("RequestName"))

        self.acdcs["healthy"] = healthyAcdcs
        if healthyAcdcs:
            self.assistanceTags.add("recovering")

    def _setInactiveAcdcs(self, family: list) -> None:
        """
        The function to set the inactive acdcs
        :param family: workflow family
        """
        inactiveAcdcs = []
        for member in family:
            if member.get("RequestStatus") not in [
                "running-open",
                "running-closed",
                "assigned",
                "acquired",
                "staging",
                "staged",
                "failed",
            ]:
                inactiveAcdcs.append(member.get("RequestName"))

        self.acdcs["inactive"] = inactiveAcdcs
        if inactiveAcdcs:
            self.assistanceTags.add("recovered")

    def _setFailedAcdcs(self, family: list) -> None:
        """
        The function to set the failed acdcs
        :param family: workflow family
        """
        failedAcdcs = []
        for member in family:
            if member.get("RequestStatus") in ["failed"]:
                failedAcdcs.append(member.get("RequestName"))

        self.acdcs["failed"] = failedAcdcs
    
    def _setAcdcsOrder(self, family: list) -> None:
        """
        The function to set the acdcs order
        :param family: workflow family
        """
        order = -1
        for member in family:
            memberOrder = sorted(filter(re.compile(f"^ACDC\d+$").search, member.get("RequestName")))
            if memberOrder:
                order = max(order, int(memberOrder[-1].split("ACDC")[1]))

        self.acdcs["order"] = order

    def _checkWorkflowFamily(self) -> None:
        """
        The function to check the workflow family
        """
        family = self._getWorkflowFamily()

        self._setBadAcdcs()
        self._setHealthyAcdcs(family)
        self._setInactiveAcdcs(family)
        self._setFailedAcdcs(family)
        self._setAcdcsOrder(family)

        if (self.bypassChecksByMcMForceComplete or self.bypassChecksByUserForceComplete) and len(self.acdcs.get("healthy")):
            self.wfController.logger.info("%s is being force completed while recovering", self.wf)
            self.wfController.logger.critical("The workflow %s was force completed", self.wf)
            WorkflowStatusEnforcer(self.wf).forceComplete()

        if self.acdcs.get("failed"):
            self.logger.critical("For %s, ACDC %s failed", self.wf, ", ".join(self.acdcs.get("failed")))
        if self.acdcs.get("bad"):
            self.logger.critical("For %s, ACDC %s is inconsistent, preventing from closing or will create a mess.", self.wf, ", ".join(self.acdcs.get("bad")))

    def _getWorkflowCompletedDelay(self) -> float:
        """
        The function to get how long a given workflow has been requested to complete
        :return: how long a given workflow has been requested to complete
        """
        completed = [*filter(lambda wf: wf.get("Status") == "completed", self.wfController.request.get("RequestTransition"))]
        delay = (self.now - completed[-1].get("UpdateTime", self.now)) / 86400.0 if completed else 0

        self.logger.info("%s days since completed", delay)

        return delay

    def _getFractionDumping(self) -> float:
        """
        The function to compute the fraction dumping
        :return: fraction dumping
        """
        wfCompletedDelay = self._getWorkflowCompletedDelay()
        fractionDamping = min(
            0.01
            * (
                max(wfCompletedDelay - self.unifiedConfiguration.get("damping_fraction_pass"), 0)
                / self.unifiedConfiguration.get("damping_fraction_pass_rate")
            ),
            self.unifiedConfiguration.get("damping_fraction_pass_max") / 100.0,
        )

        self.logger.info("The passing fraction could be reduced to %s given it has been in for long", fractionDamping)

        return fractionDamping

    def _setWorkflowCampaigns(self) -> None:
        """
        The function to set the campaigns
        """
        campaigns = {}
        wfCampaigns = self.wfController.request.getCampaigns(details=False)
        if len(wfCampaigns) == 1:
            for dataset in self.wfController.request.get("OutputDatasets"):
                campaigns[dataset] = wfCampaigns[0]
        else:
            campaigns = self.wfController.getCampaignsFromOutputDatasets()

        self.logger.info("Campaigns: %s", campaigns)
        self.campaigns = campaigns

    def _setDatasetsFractionsToAnnounce(self) -> None:
        """
        The function to set the dataset fractions to announce
        """
        fractionsAnnounce = {}
        for dataset in self.outputDatasetsToCheck:
            fractionsAnnounce[dataset] = 1.0

        self.fractions["announce"] = fractionsAnnounce

    def _setDatasetsFractionsToPass(self) -> None:
        """
        The function to set the dataset fractions to pass
        """
        fractionsPass = {}
        defaultPass = self.unifiedConfiguration.get("default_fraction_pass")

        campaigns = self.campaigns or self._setWorkflowCampaigns()

        for dataset in self.outputDatasetsToCheck:
            campaign = campaigns.get(dataset)

            if self.options.get("fractionPass"):
                fractionsPass[dataset] = self.options.get("fractionPass")
                self.wfController.logger.info(
                    "Overriding fraction to %s for %s by command line", fractionsPass[dataset], dataset
                )
            elif self.campaignController.campaigns.get(campaign, {}).get("fractionpass"):
                fractionPass = self.campaignController.campaigns.get(campaign).get("fractionpass")
                if isinstance(fractionPass, dict):
                    tier = dataset.split("/")[-1]
                    priority = str(self.wfController.request.get("RequestPriority"))
                    fractionsPass[dataset] = fractionPass.get("all", defaultPass)
                    if fractionPass.get(tier):
                        tierFractionPass = fractionPass.get(tier)
                        if isinstance(tierFractionPass, dict):
                            fractionsPass[dataset] = tierFractionPass.get("all", defaultPass)
                            for key, passValue in tierFractionPass.items():
                                if dataset.startswith(key):
                                    fractionsPass[dataset] = passValue
                        else:
                            fractionsPass[dataset] = tierFractionPass
                    if fractionPass.get(priority):
                        fractionsPass[dataset] = fractionPass.get("priority")
                else:
                    fractionsPass[dataset] = fractionPass
                self.wfController.logger.info(
                    "Overriding fraction to %s for %s by campaign requirement", fractionsPass[dataset], dataset
                )
            else:
                fractionsPass[dataset] = defaultPass

            for key, passValue in self.unifiedConfiguration.get("pattern_fraction_pass").items():
                if key in dataset:
                    fractionsPass[dataset] = passValue
                    self.wfController.logger.info("Overriding fraction to %s for %s by dataset key", passValue, dataset)

        self.fractions["pass"] = fractionsPass

    def _setDatasetsFractionsToTruncateRecovery(self) -> None:
        """
        The function to set the dataset fractions to truncate recovery
        """
        fractionsTruncateRecovery = {}

        weightFull = 7.0
        weightUnderPass = 0.0
        weightPass = self._getWorkflowCompletedDelay()

        campaigns = self.campaigns or self._setWorkflowCampaigns()

        for dataset in self.outputDatasetsToCheck:
            passPercentBelow = self.fractions["pass"][dataset] - 0.02
            fractionsTruncateRecovery[dataset] = (
                self.fractions["pass"][dataset] * weightPass + weightFull + passPercentBelow * weightUnderPass
            ) / (weightPass + weightFull * weightUnderPass)

            campaign = campaigns.get(dataset)
            if self.campaignController.campaigns.get(campaign, {}).get("truncaterecovery"):
                fractionsTruncateRecovery[dataset] = self.campaignController.campaigns.get(campaign).get(
                    "truncaterecovery"
                )
                self.wfController.logger.info(
                    "Allowed to truncate recovery of %s over %.2f by campaign requirement",
                    dataset,
                    self.campaignController.campaigns.get(campaign).get("truncaterecovery"),
                )

            if fractionsTruncateRecovery[dataset] < self.fractions["pass"][dataset]:
                fractionsTruncateRecovery[dataset] = self.fractions["pass"][dataset]

        self.fractions["truncate"] = fractionsTruncateRecovery

    def _setStatisticsThresholds(self) -> None:
        """
        The function to set the statistics thresholds
        """
        self._setDatasetsFractionsToAnnounce()
        self._setDatasetsFractionsToPass()
        self._setDatasetsFractionsToTruncateRecovery()

        fractionDamping = self._getFractionDumping()
        for dataset, value in self.fractions["pass"].items():
            if value != 1.0 and fractionDamping and self.unifiedConfiguration.get("timeout_for_damping_fraction"):
                self.fractions["pass"][dataset] -= fractionDamping
                self.fractions["truncate"][dataset] -= fractionDamping

        if self.acdsWfs.get("order") > self.unifiedConfiguration.get("acdc_rank_for_truncate"):
            self.wfController.logger.info("Truncating at pass threshold because of ACDC at rank %d", self.acdsWfs.get("order"))
            self.fractions["truncate"][dataset] = self.fractions["pass"][dataset]

        self._updateFractionsToPassAndToTruncateRecovery()

    def _updateFractionsToPassAndToTruncateRecovery(self) -> None:
        """
        The function to update the fractions to pass and to truncate recovery
        """
        family = dict([(dataset, self.dbs["reader"].getDatasetParent(dataset)) for dataset in self.fractions["pass"]])

        for dataset, value in self.fractions["pass"].items():
            ancestors = flattenDictKeys(family, family.get(dataset, []))

            descendingTruncate = self.fractions["truncate"][dataset]
            descendingPass = value
            for ancestor in ancestors:
                descendingPass *= self.fractions["pass"].get(ancestor, 1.0)
                descendingTruncate *= self.fractions["truncate"].get(ancestor, 1.0)

            if self.unifiedConfiguration.get("cumulative_fraction_pass"):
                self.fractions["pass"][dataset] = descendingPass
                self.fractions["truncate"][dataset] = descendingTruncate
                self.logger.info(
                    "For %s, previously passing at %s, is now passing at %s", dataset, value, descendingPass
                )
            else:
                self.logger.info(
                    "For %s, instead of passing at %s, could be passing at %s", dataset, value, descendingPass
                )

    def _getExpectedEvents(self) -> float:
        """
        The function to get the expected events
        :return: number of expected events
        """
        if self.wfController.request.get("RequestType") in ["TaskChain", "StepChain"]:
            return self.wfController.getRequestNumEvents()

        expectedEvents = self.wfController.request.get("TotalInputEvents")
        if expectedEvents is None:
            self.wfController.logger.critical("TotalInputEvents is missing from the workload of %s", self.wf)
            return 0

        return expectedEvents

    def _getTasksByOutputDataset(self) -> dict:
        """
        The function to get the task by output datasets
        :return: tasks by output datasets
        """
        tasksByDataset = {}
        for task, outputs in self.wfController.getOutputDatasetsPerTask().items():
            for output in outputs:
                tasksByDataset[output] = self.wfController.request.get(task, {}).get("TaskName", task)

        return tasksByDataset

    def _checkCompletionStatistics(self) -> None:
        """
        The function to check the completion statistics
        """
        lumisExpected = self.wfController.request.get("TotalInputLumis")
        eventsExpected = self._getExpectedEvents()
        eventsExpectedPerTask = self.wfController.request.getExpectedEventsPerTask()
        taskOutputs = self._getTasksByOutputDataset()

        for dataset in self.outputDatasetsToCheck:
            events, lumis = self.dbs["reader"].getDatasetEventsAndLumis(dataset)
            self.producedEvents[dataset] = events
            self.producedLumis[dataset] = lumis
            self.eventsPerLumi[dataset] = events / float(lumis) if lumis else 100
            self.percentCompletions[dataset] = 0.0

            if lumisExpected:
                self.wfController.logger.info("Lumi completion %s expected for %s", lumis, lumisExpected, dataset)
                self.percentCompletions[dataset] = lumis / float(lumisExpected)
                self.expectedLumis[dataset] = lumisExpected

            outputEventsExpected = eventsExpectedPerTask.get(taskOutputs.get(dataset, "NoTaskFound"), eventsExpected)
            if outputEventsExpected:
                self.expectedEvents[dataset] = outputEventsExpected
                eventsFraction = float(events) / float(outputEventsExpected)
                if eventsFraction > self.percentCompletions[dataset]:
                    self.percentCompletions[dataset] = eventsFraction
                    self.wfController.logger.info(
                        "Overriding: event completion real %s expected %s for %s",
                        events,
                        outputEventsExpected,
                        dataset,
                    )

    def _setPassStatisticsCheck(self) -> None:
        """
        The function to set pass statistics
        """
        self.passStatsCheck = dict(
            [
                (dataset, self.bypassChecks or self.percentCompletions[dataset] >= passValue)
                for dataset, passValue in self.fractions["pass"].items()
            ]
        )

    def _setPassStatisticsCheckToAnnounce(self) -> None:
        """
        The function to set the pass statistics to announce
        """
        self.passStatsCheckToAnnounce = dict(
            [
                (dataset, self.percentAvgCompletions[dataset] >= passValue) for dataset, passValue in self.fractions["pass"].items()
            ]
        )

    def _setPassStatisticsCheckOverCompletion(self) -> None:
        """
        The function to set the pass statistics to over complete
        """
        defaultFractionOverdoing = self.unifiedConfiguration.get("default_fraction_overdoing")
        self.passStatsCheckOverCompletion = dict([(dataset, value >= defaultFractionOverdoing) for dataset, value in self.percentCompletions.items()])


    def _checkAvgCompletionStatistics(self) -> None:
        """
        The function to check the average completion statistics
        """
        percentAvgCompletions = {}

        _, primaries, _, _ = self.wfController.request.getIO()
        runWhiteList = self.wfController.getRunWhiteList()
        lumiWhiteList = self.wfController.getLumiWhiteList()

        lumisPerRun = {}
        if not all(self.passStatsCheck.values()):
            nRuns = 1
            for primary in primaries:
                if len(self.dbs["reader"].getDatasetRuns(primary)) > 1:
                    self.logger.info("Fetching input lumis and files for %s", primary)
                    lumisPerRun[primary], _ = self.dbs["reader"].getDatasetLumisAndFiles(
                        primary, runs=runWhiteList, lumiList=lumiWhiteList
                    )
                    nRuns = len(set(lumisPerRun[primary].keys()))

            for dataset in self.passStatsCheck:
                if primaries and nRuns > 1:
                    lumisPerRun[dataset], _ = self.dbs["reader"].getDatasetLumisAndFiles(dataset)

                    fractionPerRun = {}
                    primary = primaries[0]
                    allRuns = sorted(set(lumisPerRun[primary].keys() + lumisPerRun[dataset].keys()))
                    for run in allRuns:
                        if lumisPerRun[primary].get(run, []):
                            fractionPerRun[run] = float(len(lumisPerRun[dataset].get(run, []))) / lumisPerRun[
                                primary
                            ].get(run, [])

                    if fractionPerRun:
                        avgFraction = sum(fractionPerRun.values()) / len(fractionPerRun.values())
                        percentAvgCompletions[dataset] = avgFraction

                        self.logger.info("The average completion fraction per run for %s is %s", dataset, avgFraction)

        self.percentAvgCompletions = percentAvgCompletions

    def _getAnnounceAssistanceTags(self) -> list:
        """
        The function to get announce assistance tags
        :return: announce assistance tags
        """
        if self.passStatsCheckToAnnounce and all(self.passStatsCheckToAnnounce.values()):
            self.wfController.logger.info(
                "The output of this workflow are essentially good to be announced while we work on the rest"
            )
            return ["announced" if "announced" in self.wfToCheck.status else "announce"]

        return []

    def _getRecoveryAssistanceTags(self) -> list:
        """
        The function to get recovery assistance tags
        :return: recovery assistance tags
        """
        if not all(self.passStatsCheck.values()):
            possibleRecoveries = self.acdcReader.getRecoveryDocs()
            if not possibleRecoveries:
                self.logger.info(
                    "The workflow is not completed/has missing statistics, but nothing is recoverable. Passing through to announcement"
                )
                self.bypassChecks = True

            if not self.bypassChecks:
                return ["recovery" if self.unifiedConfiguration.get("use_recoveror") else "manual"]

        return []

    def _passOver100(self) -> bool:
        """
        The function to check if passing over 100
        :return: True if pass over 100, False o/w
        """
        lhe, primaries, _, _ = self.wfController.request.getIO()
        return False if (lhe or primaries) else True

    def _forceCompleteWorkflow(self) -> bool:
        """
        The function to check if workflow should be force completed or not
        :return: True to force complete, False o/w
        """
        if self.acdcs.get("healthy") and all(self.passStatsCheck.values()) and all(self.passStatsCheckToAnnounce.values()):
            self.logger.info("This is essentially good to truncate, setting to force-complete")
            return True

        return False


    def _checkOutputSize(self,) -> None:
        """
        The function check the output size
        """
        self.assistanceTags += set(self._getAnnounceAssistanceTags())

        recoveryAssistanceTags = self._getRecoveryAssistanceTags()
        if recoveryAssistanceTags:
            self.assistanceTags += set(recoveryAssistanceTags)
            self.bypassChecks, self.isClosing = False, False

        if self._passOver100() and all(self.passStatsCheckOverCompletion.values()):
            self.assistanceTags.add("over100")

        if self._forceCompleteWorkflow():
            WorkflowStatusEnforcer(self.wf).forceComplete()

    def _hasSmallLumis(self) -> bool:
        """
        The function to check if the workflow has small lumi sections
        :return: True if it has small lumis, False o/w
        """
        lumiLowerLimit = self.unifiedConfiguration.get("min_events_per_lumi_output")
        _, primaries, _, _ = self.wfController.request.getIO()

        if (
            not self.wfController.isRelVal()
            and not primaries
            and any(
                [
                    self.eventsPerLumi[dataset] <= lumiLowerLimit
                    for dataset in self.eventsPerLumi
                    if not dataset.endswith(("DQMIO", "ALCARECO"))
                ]
            )
        ):
            self.wfController.logger.info("The workflow has very small lumisections")
            return True

        return False

    def _hasBigLumis(self) -> bool:
        """
        The function to check if the workflow has big lumi sections
        :return: True if it has big lumis, False o/w
        """
        if any(
            [
                self.lumiUpperLimit[dataset] > 0 and self.eventsPerLumi[dataset] >= self.lumiUpperLimit[dataset]
                for dataset in self.eventsPerLumi
            ]
        ):
            self.wfController.logger.info("The has large lumisections")
            return True

        return False


    def _setLumiUpperLimit(self) -> None:
        """
        The function to set the lumi sections upper limit
        """
        lumiUpperLimit = {}
        campaigns = self.campaigns or self._getWorkflowCampaigns()

        for dataset in self.outputDatasetsToCheck:
            campaign = campaigns[dataset]

            if self.wfController.request.get("RequestType") in ["ReDigi", "ReReco"]:
                upperLimit = -1
            elif self.options.get("lumisize"):
                upperLimit = self.options.get("lumisize")
                self.logger.info("Overriding the upper lumi size to %s for %s", upperLimit, campaign)
            elif self.campaignController.campaigns.get(campaign, {}).get("lumisize"):
                upperLimit = self.campaignController.campaigns.get(campaign, {}).get("lumisize")
                self.logger.info("Overriding the upper lumi size to %s for %s", upperLimit, campaign)
            else:
                upperLimit = 1001

            lumiUpperLimit[dataset] = upperLimit

        self.lumiUpperLimit = lumiUpperLimit



    def _checkLumiSize(self) -> None:
        """
        The function to check the lumi sections sizes
        """
        if self._hasSmallLumis():
            self.assistanceTags.add("smalllumi")
            self.isClosing = False

        self._setLumiUpperLimit()
        if self._hasBigLumis():
            self.assistanceTags.add("biglumi")
            self.isClosing = False

    def _checkRucioFileCounts(self) -> None:
        """
        The function to check the number of files in Rucio
        """
        rucioPresence = {}

        for dataset in self.wfController.request.get("OutputDatasets"):
            filesPerBlock = set(self.rucioReader.countDatasetFilesPerBlock(dataset))
            allBlocks = set([*map(lambda x: x[0], filesPerBlock)])
            if len(allBlocks) == len(set(filesPerBlock)):
                rucioPresence[dataset] = sum(map(lambda x: x[1], filesPerBlock))
            else:
                self.wfController.logger.info(
                    "There are inconsistences of number of files per block for dataset: %s", dataset
                )
                rucioPresence[dataset] = 0

        if any([nFiles == 0 for nFiles in rucioPresence.values()]) and "announce" in self.assistanceTags:
            self.wfController.logger.info("No files in rucio yet, no good to announce")
            self.assistanceTags.remove("announce")

        self.rucioPresence = rucioPresence

    def _checkDBSFileCounts(self) -> None:
        """
        The function to check the number of files in DBS
        """
        dbsPresence, dbsInvalid = {}, {}
        for dataset in self.wfController.request.get("OutputDatasets"):
            dbsPresence[dataset] = self.checkor.dbs["reader"].countDatasetFiles(dataset)
            dbsInvalid[dataset] = self.checkor.dbs["reader"].countDatasetFiles(dataset, onlyInvalid=True)

        self.dbsPresence, self.dbsInvalid = dbsPresence, dbsInvalid

    def _hasFileMismatch(self) -> bool:
        """
        The function to check if there is any file mismatch
        :return: if number of files in DBS in different from Rucio
        """
        if not self.options.get("ignoreFiles") and not all(
            [self.dbsPresence[dataset] == self.dbsInvalid[dataset] + self.rucioPresence[dataset] for dataset in self.outputDatasetsToCheck]
        ):
            self.logger.info("The workflow has a dbs/rucio mismatch")
            return True

        return False

    def _checkFileCounts(self) -> None:
        """
        The function to check the number of files
        """
        showOnlyN = 10

        for dataset in self.dbsPresence:
            dbsFilenames = set(
                [
                    file.get("logical_file_name")
                    for file in self.dbs["reader"].getDatasetFiles(dataset, validFileOnly=True, details=True)
                ]
            )
            rucioReader = RucioReader()
            rucioFilenames = set(rucioReader.getDatasetFileNames(dataset))

            missingRucioFiles = dbsFilenames - rucioFilenames
            missingDBSFiles = rucioFilenames - dbsFilenames

            if missingRucioFiles:
                self.wfController.logger.info(
                    "These %d files are missing in Rucio, or extra in DBS, showing %s only.\n %s",
                    len(missingRucioFiles),
                    showOnlyN,
                    "\n".join(missingRucioFiles[:showOnlyN]),
                )
                wereInvalidated = sorted(missingRucioFiles & set(self.dataCacheLoader.load("file_invalidation")))
                if wereInvalidated:
                    self.wfController.logger.info(
                        "These %d files were invalidated globally, showing %d only.\n %s",
                        len(wereInvalidated),
                        showOnlyN,
                        "\n".join(wereInvalidated[:showOnlyN]),
                    )
                    self.dbs["writer"].setFileStatus(wereInvalidated, validate=False)

            if missingDBSFiles:
                self.wfController.logger.info(
                    "These %d files are missing in DBS, or extra in Rucio, showing %s only.\n %s",
                    len(missingDBSFiles),
                    showOnlyN,
                    "\n".join(missingDBSFiles[:showOnlyN]),
                )
                wereInvalidated = sorted(missingDBSFiles & set(self.dataCacheLoader.load("file_invalidation")))
                if wereInvalidated:
                    self.wfController.logger.info(
                        "These %d files were invalidated globally, showing %d only.\n %s",
                        len(wereInvalidated),
                        showOnlyN,
                        "\n".join(wereInvalidated[:showOnlyN]),
                    )

        minFamilyCompletedDelay = self._getMinFamilyCompletedDelay(self.wfController.getFamily(includeItself=True))
        self.assistanceTags.add("agentfilemismatch" if minFamilyCompletedDelay < 2 else "filemismatch")

        self.isClosing = False

    def _checkInvalidations(self) -> None:
        """
        The function to check the invalidations
        """
        fractionInvalid = 0.2
        if not self.options.get("ignoreinvalid") and not all(
            [
                self.dbsInvalid[dataset] <= int(fractionInvalid * self.dbsPresence[dataset])
                for dataset in self.wfController.request.get("OutputDatasets")
            ]
        ):
            self.wfController.logger.info("The workflow has a DBS invalid file level too high")
            self.assistanceTags.add("invalidfiles")

    def _setRecord(self) -> None:
        """
        The function to check set the record
        """
        wfRecord = {
            "datasets": {},
            "name": self.wf,
            "closeOutWorkflow": self.isClosing,
            "priority": self.wfController.request.get("RequestPriority"),
            "prepid": self.wfController.request.get("PrepId"),
        }
        for dataset in self.outputDatasetsToCheck:
            record = wfRecord["datasets"].get(dataset, {})
            record["expectedL"] = self.expectedLumis[dataset]
            record["expectedN"] = self.expectedEvents[dataset]
            record["producedL"] = self.producedLumis[dataset]
            record["producedN"] = self.producedEvents[dataset]
            record["percentage"] = round(self.percentCompletions[dataset], 2)
            record["fractionpass"] = round(self.fractions["pass"][dataset], 2)
            record["duplicate"] = "N/A"
            record["closeOutDataset"] = self.isClosing
            record["correctLumis"] = (
                int(self.eventsPerLumi[dataset]) if self.eventsPerLumi[dataset] > self.lumiUpperLimit[dataset] else True
            )
            record["dbsFiles"] = self.dbsPresence[dataset]
            record["dbsInvFiles"] = self.dbsInvalid[dataset]
            record["rucioFiles"] = set(RucioReader().getDatasetFileNames(dataset))
            record[
                "acdc"
            ] = f"{len(self.acdcs.get('healthy', []))} / {len(self.acdcs.get('healthy', []) + self.acdcs.get('inactive', []))}"
            record["family"] = self._getWorkflowFamily(self.wf, self.wfController)

            now = gmtime()
            record["timestamp"] = mktime(now)
            record["updated"] = asctime(now) + " (GMT)"

            wfRecord["datasets"][dataset] = record

        self.record = wfRecord

    def _closeWorkflow(self) -> None:
        """
        The function to close the workflow
        """
        self.wfController.logger.info("Setting %s as closed-out", self.wf)

        if self.wfToCheck.status in ["closed-out", "announced", "normal-archived"]:
            self.logger.info(
                "%s is already %s, not trying to close-out as assuming it does",
                self.wf,
                self.wfToCheck.status,
            )
            self.newStatus = "close"
            return

        response = self.checkor.reqMgr["writer"].closeoutWorkflow(self.wf, cascade=True)
        if response:
            self.newStatus =  "close"
            return

        self.logger.info("Could not close-out, will try again next time")


    def _checkAssistanceTags(self) -> None:
        """
        The function to check the assistance tags
        """
        self.logger.info("%s was tagged with: %s", self.wf, self.assistanceTags)
        if "recovering" in self.assistanceTags:
            self.assistanceTags -= set(["recovery", "filemismatch", "manual"])
        if "recovery" in self.assistanceTags and "recovered" in self.assistanceTags:
            self.assistanceTags -= set(["recovery", "recovered"])
            self.assistanceTags.add("manual")
        if "recovery" in self.assistanceTags and "manual" in self.assistanceTags:
            self.assistanceTags -= set(["recovery"])
        if "custodial" in self.assistanceTags:
            self.assistanceTags -= set(["announce", "announced"])
        if any([tag in self.assistanceTags for tag in ["duplicates", "filemismatch", "agentfilemismatch"]]):
            self.assistanceTags -= set(["announce"])

        self.logger.info("%s needs assistance with: %s", self.wf, self.assistanceTags)
        self.logger.info("%s has existing conditions: %s", self.wf, self.existingAssistaceTags)

    def _warnRequestor(self) -> None:
        """
        The function to warn the requestor about the workflow progress
        """
        if self.assistanceTags and "manual" not in self.existingAssistaceTags and self.existingAssistaceTags != self.assistanceTags and any(tag in self.assistanceTags for tag in ["recovery", "biglumi"]):

            msg = "The request PREPID (WORKFLOW) is facing issue in production.\n"

            if "recovery" in self.assistanceTags:
                msg += f"Samples completed with missing statistics\n{'\n'.join([f'{round(self.percentCompletions[dataset]*100, 2)}%% complete for {dataset}' for dataset in self.outputDatasetsToCheck ])}\nhttps://cmsweb.cern.ch/report/{self.wf}\n"
            if "biglumi" in self.assistanceTags:
                msg += f"Samples completed with large luminosity blocks:\n{'\n'.join([f'{self.eventsPerLumi[dataset]} > {self.lumiUpperLimit[dataset]} for {dataset}' for dataset in self.outputDatasetsToCheck])}\nhttps://cmsweb.cern.ch/reqmgr/view/splitting/{self.wf}\n"

            msg += "You are invited to check, while this is being taken care of by Comp-Ops.\n"
            msg += "This is an automated message from Comp-Ops.\n"

            self.wfController.logger.critical(msg)

    def _getAssistanceStatus(self) -> str:
        """
        The function to get the assistance status from the tags
        """
        if self.assistanceTags:
            return "assistance-" + "-".join(sorted(self.assistanceTags))
        return "assistance"
    
    def _setWorkflowToAssistance(self) -> None:
        """
        The function to set the workflow to assistance
        """
        assistanceStatus = self._getAssistanceStatus()
        if "manual" not in self.wfToCheck.status or assistanceStatus != "assistance-recovery":
            self.newStatus = assistanceStatus

    def check(self) -> dict:
        """
        The function to check the workflow
        """
        try:
            self.checkor._checkPoint(f"Starting checkor with {self.wf}")
            if self._skipWorkflow() or self._updateWorkflowStatus():
                return self._writeResponse()
            self.checkor._checkPoint("Checked workflow status", subLap=True)
        
            self._setOutputDatasetsToCheck()
            self._checkWorkflowFamily()
            self.checkor._checkPoint("Checked workflow family", subLap=True)

            self._setStatisticsThresholds()
            self.checkor._checkPoint("Checked statistics threshold", subLap=True)

            self._checkCompletionStatistics()
            self.checkor._checkPoint("Checked observed statistics", subLap=True)

            self._setPassStatisticsCheck()
            self._checkAvgCompletionStatistics()
            self.checkor._checkPoint("Checked more detailed observed statistics", subLap=True)

            self._setPassStatisticsCheckToAnnounce()
            self._setPassStatisticsCheckOverCompletion()
            self._checkOutputSize()
            self.checkor._checkPoint("Checked output size", subLap=True)

            self._checkLumiSize()
            self.checkor._checkPoint("Checked lumi size", subLap=True)

            self._checkRucioFileCounts()
            self.checkor_checkPoint("Checked Rucio count", subLap=True)

            self._checkDBSFileCounts()
            self.checkor._checkPoint("DBS file count", subLap=True)

            if self._hasFileMismatch() and "recovering" not in self.assistanceTags:
                self._checkFileCounts()
            self.checkor._checkPoint("Checked file count", subLap=True)

            self._checkInvalidations()
            self.checkor._checkPoint("Checked invalidation", subLap=True)

            self.checkor._checkPoint(f"Done with {self.wf}")
            self._setRecord()

            if self.isClosing:
                self._closeWorkflow()
            else:
                self._checkAssistanceTags()
                self._warnRequestor()
                self._setWorkflowToAssistance()
                    
                ## TODO: update JIRA tickets / migrate JiraClient
            
            return self._writeResponse()

        except Exception as error:
            self.logger.error("Failed on checking %s", self.wf)
            self.logger.error(str(error))
            self.failed = True
            return self._writeResponse()
    
    def _writeResponse(self) -> dict:
        """
        The function to write the check response
        """
        response = {
            "workflow": self.wfToCheck,
            "wf": self.wf,
            "failed": self.failed, 
            "isClosing": self.isClosing, 
            "newStatus": self.newStatus, 
            "prepIds": self.wfController.getPrepIDs(), 
            "mcmForceComplete": self.bypassChecksByMcMForceComplete,
            "record": self.record
        }
        return response


if __name__ == "__main__":
    options, specificWf = Checkor.parseOptions()
    checkor = Checkor(options=options, specificWf=specificWf)
    if checkor.go():
        checkor.run()
