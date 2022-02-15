import optparse
import random
from logging import Logger
from collections import defaultdict
from time import mktime, asctime, gmtime, struct_time

from Utilities.DataTools import unnestList
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
from WorkflowMgmt.SiteController import SiteController
from WorkflowMgmt.UserLockChecker import UserLockChecker

from Unified.Helpers.WorkflowCheckor import WorkflowCheckor

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
            "--checkRunning",
            help="Running workflows that have not yet reached completed",
            action="store_true",
            default=False,
        )
        parser.add_option(
            "--checkNewlyCompleted", help="Only running workflows that reached completed", action="store_true", default=False
        )
        parser.add_option(
            "--checkAssistance",
            help="Look at the workflows that have already completed and had required actions",
            action="store_true",
            default=False,
        )
        parser.add_option(
            "--checkAssistanceRecovering",
            help="Look at the workflows that already have on-going acdc",
            action="store_true",
            default=False,
        )
        parser.add_option("--checkAssistanceManual", help='Look at the workflows in "manual"', action="store_true", default=False)
        parser.add_option("--maxPerRound", help="The number of workflows to consider for checking", default=0, type=int)
        parser.add_option("--nThreads", help="The number of threads for processing workflows", default=1, type=int)
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
        parser.add_option(
            "--dryRun",
            help="To run in test mode (with production but without making any changes)",
            actions="store_true",
            default=False,
        )

        options, args = parser.parse_args()
        options = vars(options)

        options["checkAssistanceManual"] = not options.get("checkAssistanceRecovering")
        actions = ["checkNewlyCompleted", "checkRunning", "checkAssistance"]
        if all(not options.get(option) for option in actions):
            for option in actions + ["checkAssistanceRecovering", "checkAssistanceManual"]:
                options[option] = True

        return options, args[0] if args else None

    def _setWfs(self) -> None:
        """
        The function to set workflows to override, on hold, bypass and force complete.
        """
        self.overrideWfs = self._getWorkflowsByAction("force-complete", details=True)
        self.onHoldWfs = self._getWorkflowsByAction("hold")
        self.bypassWfs = self._getWorkflowsByAction("bypass") + unnestList(self.overrideWfs)
        self.forceCompleteWfs = (
            self.mcmClient.get("/restapi/requests/forcecomplete") if self.mcmClient is not None else {}
        )

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

        if self.options.get("checkNewlyCompleted"):
            self.logger.info("checkNewlyCompleted option is on: checking workflows that freshly completed")
            workflows.update(filter(lambda wf: wf.name in completedWfs, awayWfs))

        if self.options.get("checkRunning"):
            self.logger.info("checkRunning option is on: checking workflows that have not completed yet")
            workflows.update(filter(lambda wf: wf.name not in completedWfs, awayWfs))

        if self.options.get("checkAssistance"):
            nonCustodialWfs = [*filter(lambda wf: "custodial" not in wf.status, assistanceWfs)]
            if self.options.get("checkAssistanceRecovering"):
                self.logger.info(
                    "checkAssistanceRecovering option is on: checking only the workflows that had been already acted on"
                )
                workflows.update(filter(lambda wf: "manual" not in wf.status, nonCustodialWfs))
            if self.options.get("checkAssistanceManual"):
                self.logger.info("checkAssistanceManual option is on: checking the workflows to be acted on")
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
        if self.options.get("maxPerRound"):
            self.logger.info("Command line to limit workflows to %s", self.options.get("maxPerRound"))
            maxPerRound = self.options.get("maxPerRound")

        if maxPerRound and not self.specificWf:
            self.logger.info("Number of workflows to check after limitation: %s", maxPerRound)

            workflows = self._rankWorkflows(workflows)
            if self.option.get("checkRunning"):
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
                if wf.options.get("dryRun"):
                    self.logger.debug("Dry run: updated workflow %s records to %s", wf.get("wf"), wf.get("record"))
                else:
                    self.closeoutController.set(wf.get("wf"), wf.get("record"))

    def _updateWorkflowsStatus(self, wfsStatus: list) -> None:
        """
        The function to update the workflows status
        :param wfsStatus: status
        """
        # TODO: Do the ReqMgr update here as well: WorkflowCheckor:_closeOutWorkflow
        for wf in wfsStatus:
            if wf.get("newStatus"):
                newStatus = wf.get("newStatus")
                wf["workflow"].status = newStatus

                if wf.options.get("dryRun"):
                    self.logger.debug("Dry run: Unified status update of %s to %s", wf.get("wf"), newStatus)
                else:
                    self.session.commit()

                if newStatus == "close":
                    if wf.options.get("dryRun"):
                        self.logger.debug("Dry run: cleaned workflow %s because set as close", wf.get("wf"))
                    else:
                        self.closeoutController.clean(wf.get("wf"))

                    if self.mcmClient is not None and wf.get("mcmForceComplete"):
                        if wf.options.get("dryRun"):
                            self.logger.debug(
                                "Dry run: cleaned workflow %s prep ids because of force-complete", wf.get("wf")
                            )
                        else:
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
            if self.options.get("checkNewlyCompleted"):
                msg += "Workflows which just got in completed were looked at. Look in manual.\n"
            if self.options.get("checkRunning"):
                msg += "Workflows that are still running (and not completed) got looked at.\n"
            if self.options.get("checkAssistance"):
                msg += "Workflows under intervention got review.\n"

            msg += "\n".join(
                [f"{count} in status {status}" for status, count in self._countAssistanceWorkflowsByStatus()]
            )

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
                self.jiraClient = (
                    None  # JiraClient() if servicesChecker.status.get("jira") else None    TODO: migrate JiraClient
                )
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
            # Review this later. Consider moving JIRA functionalities to a different module.
            #self._setWfs()

            wfsToCheck = self._filterBackfills(self._getWorkflowsToCheck())
            random.shuffle(wfsToCheck)

            self.logger.info("Number of workflows to check before any limitation: %s", len(wfsToCheck))

            wfsToCheck = self._filterMaxNumberOfWorkflows(wfsToCheck)
            self.logger.info("Workflows to check: ")
            for w in wfsToCheck:
                self.logger.info(w)
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

        @runWithMultiThreading(mtParam="wfsToCheck", maxThreads=self.options.get("nThreads"))
        def _checkWorkflow(self, wfsToCheck: list) -> dict:
            return WorkflowCheckor(wfsToCheck, checkor=self).check()

        # TODO: Workflow statuses have been already updated in WorkflowCheckor????
        checkResponses = _checkWorkflow(wfsToCheck)
        self.logger.critical("Response to Checkor:")
        self.logger.critical(checkResponses)
        # TODO: The following function updates closeoutInfo table of MongoDB.
        #self._updateWorkflowsRecords(checkResponses)
        # TODO: This does unified status update, mongodb record update and McM force-completion.
        # TODO: Why not do all the operations here, especially ReqMgr2 status update??
        #self._updateWorkflowsStatus(checkResponses)

        self._checkExecutionTime(len(wfsToCheck))


if __name__ == "__main__":
    options, specificWf = Checkor.parseOptions()
    checkor = Checkor(options=options, specificWf=specificWf)
    if checkor.go():
        checkor.run()
