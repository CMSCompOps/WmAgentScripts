import os
import re
from logging import Logger
from time import mktime, asctime, gmtime

from Utilities.DataTools import flattenDictKeys
from Utilities.Logging import getLogger
from Databases.Oracle.OracleDB import Workflow
from Services.Rucio.RucioReader import RucioReader
from WorkflowMgmt.WorkflowController import WorkflowController
from WorkflowMgmt.WorkflowStatusEnforcer import WorkflowStatusEnforcer

from typing import Optional, Tuple

#class WorkflowCheckor(object):
class WorkflowCheckor():
    """
    __WorkflowCheckor__
    General API for checking a given workflow
    """

    def __init__(self, workflow: Workflow, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            #super().__init__(self)
            self.logger = logger or getLogger(self.__class__.__name__)
            self.logger.info("Initializing Workflow Checkor")
            self.now = mktime(gmtime())

            self.wf = workflow.name
            self.wfController = WorkflowController(self.wf)

            self.workflow = workflow
            self.workflow.wm_status = self.wfController.request.get("RequestStatus")
            # TODO: Check this
            self.unifiedStatus = workflow.status

            self.checkor = kwargs.get("checkor")
            self.rucioReader = RucioReader()

            # TODO: What's the difference between assistanceTags & existingAssistaceTags?
            self.assistanceTags, self.existingAssistaceTags = set(), set(workflow.status.split("-")[1:])
            
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
            (self.checkor.specificWf and self.checkor.specificWf not in self.wf)
            or os.path.isfile(".checkor_stop")
            or self.workflow.wm_status in ["assigned", "acquired"]
            or self.workflow.wm_status != "completed"
        ):
            self.logger.info("Skipping workflow %s", self.wf)
            return True

        return False

    def _setWorkflowToClose(self) -> bool:
        """
        The function to check if a given workflow should be closed
        :return: True if workflow should be closed, False o/w
        """
        self.logger.info("Checking if the unified status should be 'close' already, but not")
        if self.workflow.wm_status in ["closed-out", "announced"] and self.workflow.unifiedStatus != "close":
            self.logger.info("%s is already %s, setting as close", self.wf, self.workflow.wm_status)
            self.newStatus = "close"
            return True
        else:
            self.logger.info("Unified status is already okay, skipping the request.")

        return False

    def _setWorkflowToForget(self) -> bool:
        """
        The function to check if a given workflow should be forgotten
        :return: True if workflow should be forgotten, False o/w
        """
        self.logger.info("Checking if the unified status should be 'forget'")
        if self.wfController.request.isRelVal() and self.workflow.wm_status in [
            "failed",
            "aborted",
            "aborted-archived",
            "rejected",
            "rejected-archived",
            "aborted-completed",
        ]:
            self.logger.info("%s is %s, setting the unified status as 'forget'", self.wf, self.workflow.wm_status)
            self.newStatus = "forget"
            return True
        else:
            self.logger.info("%s is %s, not setting it as 'forget'", self.wf, self.workflow.wm_status)

        return False

    def _setWorkflowToTrouble(self) -> bool:
        """
        The function to check if a given workflow should be set as trouble
        :return: True if workflow should be set as trouble, False o/w
        """
        if not self.wfController.request.isRelVal() and self.workflow.wm_status in [
            "failed",
            "aborted",
            "aborted-archived",
            "rejected",
            "rejected-archived",
            "aborted-completed",
        ]:
            self.logger.info("%s is %s, setting as trouble", self.wf, self.workflow.wm_status)
            self.newStatus = "trouble"
            return True

        return False

    def _setBypassChecks(self) -> None:
        """
        The function to check if bypass checks for a given workflow
        """
        for bypassWf in self.checkor.bypassWfs:
            if bypassWf == self.wf:
                self.logger.info("Bypassing checks %s because of keyword %s", self.wf, bypassWf)
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
        return "-onhold" in self.workflow.wm_status or self.wf in self.checkor.onHoldWfs

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
        onHoldTimeout = self.checkor.unifiedConfiguration.get("onhold_timeout")
        minFamilyCompletedDelay = self._getMinFamilyCompletedDelay(family, self.now)

        self.logger.info("On hold since %s, timeout at %s", minFamilyCompletedDelay, onHoldTimeout)

        if onHoldTimeout > 0 and onHoldTimeout < minFamilyCompletedDelay:
            self.logger.info("%s is on hold and stopped for %.2f days, letting this through with current statistics", self.wf, minFamilyCompletedDelay)
            self.bypassChecks = True
            return False

        if "-onhold" in self.workflow.wm_status and self.wf in self.checkor.onHoldWfs and not self.bypassChecks:
            self.logger.info("%s is on hold", self.wf)
            return True

        if self.wf in self.checkor.onHoldWfs and not self.bypassChecks:
            self.logger.info("%s is %s, setting as on hold", self.wf, self.workflow.wm_status)
            self.newStatus = "assistance-onhold"
            return True

        return False

    def _updateWorkflowStatus(self) -> bool:
        """
        The function to check if the status of a given workflow should be updated
        :return: True if update status, False o/w
        """
        if self._setWorkflowToClose():
            return True
        if self._setWorkflowToForget():
            return True
        # TODO Check this
        #if self._setWorkflowToTrouble():
        #    return True

        # TODO Check this later
        #if self._isWorkflowOnHold() and self._setNewOnHoldStatus(self.wfController.getFamily(includeItself=True)):
        #    return True

        return False

    def _getTiersWithNoCheck(self) -> Tuple[list, list]:
        """
        The function yo get the tiers with no check and custodial
        :return: list of tiers with no check, and list of tiers with no custodial
        """
        tiersWithNoCheck = set(self.checkor.unifiedConfiguration.get("tiers_with_no_check"))

        campaigns = self.campaigns or self._setWorkflowCampaigns()
        # TODO custodial_override is not good name anymore, change it!
        for campaign in campaigns.values():
            if (
                campaign in self.checkor.campaignController.campaigns
                and "custodial_override" in self.checkor.campaignController.campaigns[campaign]
            ):
                if isinstance(self.checkor.campaignController.campaigns[campaign]["custodial_override"], list):
                    tiersWithNoCheck -= self.checkor.campaignController.campaigns[campaign]["custodial_override"]

        return tiersWithNoCheck

    def _setOutputDatasetsToCheck(self) -> None:
        """
        The function to set the datasets to check
        """
        self.logger.info("Checking which output datasets to be handled")
        tiersWithNoCheck = self._getTiersWithNoCheck(self.wfController)
        expectedOutputsDatasets = self.wfController.request.get("OutputDatasets")
        self.outputDatasetsToCheck = [
            dataset
            for dataset in expectedOutputsDatasets
            if all([dataset.split("/")[-1] != tier for tier in tiersWithNoCheck])
        ]

        self.wfController.logger.info(
            "Initial outputs: %s\nWill check on: %s\nTiers out: %s\nTiers no custodials: %s",
            ", ".join(sorted(expectedOutputsDatasets)),
            ", ".join(sorted(self.outputDatasetsToCheck)),
            ", ".join(sorted(tiersWithNoCheck))
        )

    def _skipFamilyWorkflow(self, wfSchema: dict) -> bool:
        """
        The function to check if the workflow family should be skipped or not
        :param wfSchema: workflow schema
        :return: True if to skip, False o/w
        """
        return (
            wfSchema.get("RequestType") != "Resubmission"
            or wfSchema.get("RequestDate") < self.wfController.request.get("RequestDate")
            or wfSchema.get("PrepID") != self.wfController.request.get("PrepID")
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

            if self.checkor.options.get("dryRun"):
                self.checkor.logger.debug("Dry run: force completed workflow %s", self.wf)
            else:
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
                max(wfCompletedDelay - self.checkor.unifiedConfiguration.get("damping_fraction_pass"), 0)
                / self.checkor.unifiedConfiguration.get("damping_fraction_pass_rate")
            ),
            self.checkor.unifiedConfiguration.get("damping_fraction_pass_max") / 100.0,
        )

        self.logger.info("The passing fraction could be reduced to %s given it has been in for long", fractionDamping)

        return fractionDamping

    def _setWorkflowCampaigns(self) -> dict:
        """
        The function to set the campaigns
        :return: campaigns
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

        return campaigns

    def _setDatasetsFractionsToAnnounce(self) -> None:
        """
        The function to set the dataset fractions to announce
        """
        fractionsAnnounce = {}
        for dataset in self.outputDatasetsToCheck:
            fractionsAnnounce[dataset] = 1.0

        self.fractions["announce"] = fractionsAnnounce

    def _getFractionPassByCommandLine(self) -> float:
        """
        The function to get the fraction pass by the command line
        :return: fractions to pass if any, else None
        """
        if self.checkor.options.get("fractionPass"):
            fractionPass = self.checkor.options.get("fractionPass")
            self.wfController.logger.info("Overriding fraction to pass to %s by command line", fractionPass)
            return fractionPass

    def _getFractionPassByCampaignRequirement(self, campaign: str, dataset: str) -> float:
        """
        The function to get the fraction pass by the campaign requirement
        :param campaign: campaign name
        :param dataset: dataset name
        :return: fractions to pass if any, else None
        """
        fractionPass = None
        campaignFractionPass = self.checkor.campaignController.campaigns.get(campaign, {}).get("fractionpass")

        if campaignFractionPass:
            if isinstance(campaignFractionPass, dict):
                tier = dataset.split("/")[-1]
                priority = str(self.wfController.request.get("RequestPriority"))
                fractionPass = campaignFractionPass.get("all")
                if campaignFractionPass.get(tier):
                    tierFractionPass = campaignFractionPass.get(tier)
                    if isinstance(tierFractionPass, dict):
                        fractionPass = tierFractionPass.get("all")
                        for key, passValue in tierFractionPass.items():
                            if dataset.startswith(key):
                                fractionPass = passValue
                    else:
                        fractionPass = tierFractionPass

                if campaignFractionPass.get(priority):
                    fractionPass = campaignFractionPass.get("priority")
            
            else:
                fractionPass = campaignFractionPass
        
        if fractionPass is not None:
            self.wfController.logger.info(
                "Overriding fraction to pass to %s by campaign requirement", fractionPass
            )
            return fractionPass


    def _setDatasetsFractionPass(self) -> None:
        """
        The function to set the dataset fractions to pass
        """
        fractionsPass = {}
        defaultPass = self.checkor.unifiedConfiguration.get("default_fraction_pass")

        campaigns = self.campaigns or self._setWorkflowCampaigns()

        for dataset in self.outputDatasetsToCheck:
            campaign = campaigns.get(dataset)

            fractionsPass[dataset] = self._getFractionPassByCommandLine()
            if not fractionsPass[dataset]:
                fractionsPass[dataset] = self._getFractionPassByCampaignRequirement(campaign, dataset)
            if not fractionsPass[dataset]:
                fractionsPass[dataset] = defaultPass

            for key, passValue in self.checkor.unifiedConfiguration.get("pattern_fraction_pass").items():
                if key in dataset:
                    fractionsPass[dataset] = passValue
                    self.wfController.logger.info("Overriding fraction to %s for %s by dataset key", passValue, dataset)

        self.logger.info("Expected stats (Fraction pass): %s", str(fractionsPass))
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
            if self.checkor.campaignController.campaigns.get(campaign, {}).get("truncaterecovery"):
                fractionsTruncateRecovery[dataset] = self.checkor.campaignController.campaigns.get(campaign).get(
                    "truncaterecovery"
                )
                self.wfController.logger.info(
                    "Allowed to truncate recovery of %s over %.2f by campaign requirement",
                    dataset,
                    self.checkor.campaignController.campaigns.get(campaign).get("truncaterecovery"),
                )

            if fractionsTruncateRecovery[dataset] < self.fractions["pass"][dataset]:
                fractionsTruncateRecovery[dataset] = self.fractions["pass"][dataset]

        self.fractions["truncate"] = fractionsTruncateRecovery

    def _setExpectedStats(self) -> None:
        """
        The function to set the statistics thresholds
        """
        # TODO: # Sets it 1 regardless of the dataset and it's not used?
        #self._setDatasetsFractionsToAnnounce()
        self._setDatasetsFractionPass()


        # TODO: Disabling truncation for now. Check that later.
        """
        self._setDatasetsFractionsToTruncateRecovery()
        fractionDamping = self._getFractionDumping()
        for dataset, value in self.fractions["pass"].items():
            if value != 1.0 and fractionDamping and self.checkor.unifiedConfiguration.get("timeout_for_damping_fraction"):
                self.fractions["pass"][dataset] -= fractionDamping
                self.fractions["truncate"][dataset] -= fractionDamping

        if self.checkor.acdsWfs.get("order") > self.checkor.unifiedConfiguration.get("acdc_rank_for_truncate"):
            self.wfController.logger.info("Truncating at pass threshold because of ACDC at rank %d", self.checkor.acdsWfs.get("order"))
            self.fractions["truncate"][dataset] = self.fractions["pass"][dataset]

        self._updateFractionsToPassAndToTruncateRecovery()
        """

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

            if self.checkor.unifiedConfiguration.get("cumulative_fraction_pass"):
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
            return self.wfController.request.getRequestNumEvents()

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

    def _setActualStats(self) -> None:
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
        self.logger.info("Actual stats: %s", str(self.percentCompletions))

    def _compareAndSetExpectedAndActualStats(self) -> None:
        """
        The function to set pass statistics
        """
        # TODO: The way bypassChecks is introduced seems problematic, review it.
        self.passStatsCheck = dict(
            [
                (dataset, self.bypassChecks or self.percentCompletions[dataset] >= passValue)
                for dataset, passValue in self.fractions["pass"].items()
            ]
        )
        self.logger.info("Compared actual and expected stats: %s", str(self.passStatsCheck))

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
        defaultFractionOverdoing = self.checkor.unifiedConfiguration.get("default_fraction_overdoing")
        self.passStatsCheckOverCompletion = dict(
            [(dataset, value >= defaultFractionOverdoing) for dataset, value in self.percentCompletions.items()]
        )


    def _checkAvgCompletionStatistics(self) -> None:
        """
        The function to check the average completion statistics
        """
        percentAvgCompletions = {}

        _, primaries, _, _ = self.wfController.request.getIO()
        runWhiteList = self.wfController.getRunWhiteList()
        lumiWhiteList = self.wfController.request.getLumiWhiteList()

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
            return ["announced" if "announced" in self.workflow.status else "announce"]

        return []

    def _getRecoveryAssistanceTags(self) -> list:
        """
        The function to get recovery assistance tags
        :return: recovery assistance tags
        """
        if not all(self.passStatsCheck.values()):
            possibleRecoveries = self.checkor.acdcReader.getRecoveryDocs(self.wf)
            if not possibleRecoveries:
                self.logger.info(
                    "The workflow is not completed/has missing statistics, but nothing is recoverable. Passing through to announcement"
                )
                self.bypassChecks = True

            if not self.bypassChecks:
                return ["recovery" if self.checkor.unifiedConfiguration.get("use_recoveror") else "manual"]

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
            if self.checkor.options.get("dryRun"):
                self.checkor.logger.debug("Dry run: force completed workflow %s", self.wf)
            else:
                WorkflowStatusEnforcer(self.wf).forceComplete()

    def _hasSmallLumis(self) -> bool:
        """
        The function to check if the workflow has small lumi sections
        :return: True if it has small lumis, False o/w
        """
        lumiLowerLimit = self.checkor.unifiedConfiguration.get("min_events_per_lumi_output")
        _, primaries, _, _ = self.wfController.request.getIO()

        if (
            not self.wfController.request.isRelVal()
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
        campaigns = self.campaigns or self._setWorkflowCampaigns()

        for dataset in self.outputDatasetsToCheck:
            campaign = campaigns[dataset]

            if self.wfController.request.get("RequestType") in ["ReDigi", "ReReco"]:
                upperLimit = -1
            elif self.checkor.options.get("lumisize"):
                upperLimit = self.checkor.options.get("lumisize")
                self.logger.info("Overriding the upper lumi size to %s for %s", upperLimit, campaign)
            elif self.checkor.campaignController.campaigns.get(campaign, {}).get("lumisize"):
                upperLimit = self.checkor.campaignController.campaigns.get(campaign, {}).get("lumisize")
                self.logger.info("Overriding the upper lumi size to %s for %s", upperLimit, campaign)
            else:
                upperLimit = 1001

            lumiUpperLimit[dataset] = upperLimit

        self.lumiUpperLimit = lumiUpperLimit



    def _checkLumiSize(self) -> None:
        """
        The function to check the lumi sections sizes
        """
        self.logger.info("Checking lumi size")
        if self._hasSmallLumis():
            self.assistanceTags.add("smalllumi")
            self.isClosing = False
            # TODO: Give more details??
            self.logger.info("Output has small lumisections, not closing out.")

        self._setLumiUpperLimit()
        if self._hasBigLumis():
            self.assistanceTags.add("biglumi")
            self.isClosing = False
            # TODO: Give more details??
            self.logger.info("Output has big lumisections, not closing out.")

    def _checkRucioFileCounts(self) -> None:
        """
        The function to check the number of files in Rucio
        """
        rucioPresence = {}
        # TODO: Check the algorithm of this function
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
        self.logger.info("Checked the rucio presence: %s", str(self.rucioPresence))

    def _checkDBSFileCounts(self) -> None:
        """
        The function to check the number of files in DBS
        """
        dbsPresence, dbsInvalid = {}, {}
        for dataset in self.wfController.request.get("OutputDatasets"):
            dbsPresence[dataset] = self.checkor.dbs["reader"].countDatasetFiles(dataset)
            dbsInvalid[dataset] = self.checkor.dbs["reader"].countDatasetFiles(dataset, onlyInvalid=True)

        self.dbsPresence, self.dbsInvalid = dbsPresence, dbsInvalid

        self.logger.info("Checked DBS Presence: %s", str(self.dbsPresence))
        self.logger.info("Checked INVALID DBS Presence: %s", str(self.dbsInvalid))

    def _hasFileMismatch(self) -> bool:
        """
        The function to check if there is any file mismatch
        :return: if number of files in DBS in different from Rucio
        """
        if not self.checkor.options.get("ignoreFiles") and not all(
            [self.dbsPresence[dataset] == self.dbsInvalid[dataset] + self.rucioPresence[dataset] for dataset in self.outputDatasetsToCheck]
        ):
            self.logger.info("The workflow has a dbs/rucio mismatch")
            return True

        return False

    def _handleFileMismatch(self) -> None:
        """
        The function to check the number of files
        """
        # TODO: Make this configurable!
        showOnlyN = 10

        # TODO: Check the algorithm of this function
        self.logger.info("There is a RUCIO/DBS filemismatch. Checking the details")

        for dataset in self.dbsPresence:
            dbsFilenames = set(
                [
                    file.get("logical_file_name")
                    for file in self.dbs["reader"].getDatasetFiles(dataset, validFileOnly=True, details=True)
                ]
            )
            rucioFilenames = set(self.rucioReader.getDatasetFileNames(dataset))

            missingRucioFiles = dbsFilenames - rucioFilenames
            missingDBSFiles = rucioFilenames - dbsFilenames

            if missingRucioFiles:
                self.wfController.logger.info(
                    "These %d files are missing in Rucio, or extra in DBS, showing %s only.\n %s",
                    len(missingRucioFiles),
                    showOnlyN,
                    "\n".join(missingRucioFiles[:showOnlyN]),
                )
                wereInvalidated = sorted(missingRucioFiles & set(self.checkor.dataCacheLoader.load("file_invalidation")))
                # TODO: Check this invalidation!!
                if wereInvalidated:
                    self.wfController.logger.info(
                        "These %d files were invalidated globally, showing %d only.\n %s",
                        len(wereInvalidated),
                        showOnlyN,
                        "\n".join(wereInvalidated[:showOnlyN]),
                    )

                    if self.checkor.options.get("dryRun"):
                        self.checkor.logger.debug("Dry run: invalidated %s files", len(wereInvalidated))
                    else:
                        self.dbs["writer"].setFileStatus(wereInvalidated, validate=False)

            if missingDBSFiles:
                self.wfController.logger.info(
                    "These %d files are missing in DBS, or extra in Rucio, showing %s only.\n %s",
                    len(missingDBSFiles),
                    showOnlyN,
                    "\n".join(missingDBSFiles[:showOnlyN]),
                )
                wereInvalidated = sorted(missingDBSFiles & set(self.checkor.dataCacheLoader.load("file_invalidation")))
                # TODO: Check this invalidation
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

    def _checkInvalidFiles(self) -> None:
        """
        The function to check the invalidations
        """
        self.logger.info("Checking if the output(s) has/have a significant amount of invalid files")
        fractionInvalid = 0.2
        if not self.checkor.options.get("ignoreinvalid") and not all(
            [
                self.dbsInvalid[dataset] <= int(fractionInvalid * self.dbsPresence[dataset])
                for dataset in self.wfController.request.get("OutputDatasets")
            ]
        ):
            self.wfController.logger.info("The workflow has a DBS invalid file level too high")
            self.assistanceTags.add("invalidfiles")
        else:
            self.logger.info("The outputs don't have a significant amount of invalid files")

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
            record["rucioFiles"] = set(self.rucioReader.getDatasetFileNames(dataset))
            record[
                "acdc"
            ] = f"{len(self.acdcs.get('healthy', []))} / {len(self.acdcs.get('healthy', []) + self.acdcs.get('inactive', []))}"
            record["family"] = self._getWorkflowFamily(self.wf, self.wfController)

            now = gmtime()
            record["timestamp"] = mktime(now)
            record["updated"] = asctime(now) + " (GMT)"

            wfRecord["datasets"][dataset] = record

        self.logger.info("Following record has been produced for MongoDB update: %s", record)

        self.record = wfRecord

    def _closeOutWorkflow(self) -> None:
        """
        The function to close the workflow
        """
        self.wfController.logger.info("Setting %s as closed-out", self.wf)

        if self.workflow.status in ["closed-out", "announced", "normal-archived"]:
            self.logger.info(
                "%s is already %s, not trying to close-out as assuming it does",
                self.wf,
                self.workflow.status,
            )
            self.newStatus = "close"
            return

        if self.checkor.options.get("dryRun"):
            self.checkor.logger.debug("Dry run: closed out workflow %s", self.wf)
        else:
            response = self.checkor.reqMgr["writer"].closeoutWorkflow(self.wf, cascade=True)
            if response:
                self.newStatus =  "close"
                return

            self.logger.info("Could not close-out, will try again next time")


    def _updateAssistanceStatus(self) -> None:
        """
        The function to check the assistance tags
        """
        # TODO: Needs dry-run mode update
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
                msg += f"Samples completed with missing statistics\n{''.join([f'{round(self.percentCompletions[dataset]*100, 2)}%% complete for {dataset}' for dataset in self.outputDatasetsToCheck ])}\nhttps://cmsweb.cern.ch/report/{self.wf}\n"
            if "biglumi" in self.assistanceTags:
                msg += f"Samples completed with large luminosity blocks:\n{''.join([f'{self.eventsPerLumi[dataset]} > {self.lumiUpperLimit[dataset]} for {dataset}' for dataset in self.outputDatasetsToCheck])}\nhttps://cmsweb.cern.ch/reqmgr/view/splitting/{self.wf}\n"

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
    
    def _setAssistanceStatus(self) -> None:
        """
        The function to set the workflow to assistance
        """
        assistanceStatus = self._getAssistanceStatus()
        if "manual" not in self.workflow.status or assistanceStatus != "assistance-recovery":
            self.newStatus = assistanceStatus

        self.logger.info("Ultimate assistance status is: %s", str(assistanceStatus))

    def check(self) -> dict:
        """
        The function to check the workflow
        """
        try:
            self.checkor._checkPoint(f"Starting checkor with {self.wf}")

            # Investigate this later: There should not be a case to skip a workflow
            #if self._skipWorkflow():
            #    return self._writeResponse()

            # TODO: The following function checks workflows whose unified status should have been updated, but not.
            # TODO: Workflows to close & Workflows to forget
            # TODO: I think we can keep it for a while and see if it really has a use
            # TODO: I also think that it needs a renaming.
            if self._updateWorkflowStatus():
                # TODO: Check this function!!
                return self._writeResponse()
            self.checkor._checkPoint("Checked workflow status", subLap=True)
        
            self._setOutputDatasetsToCheck()

            # TODO: Check this function. It does more than checking
            #self._checkWorkflowFamily()
            #self.checkor._checkPoint("Checked workflow family", subLap=True)

            # TODO: I did simplifications. Review it later.
            self._setExpectedStats()
            self.checkor._checkPoint("Checked expected stats", subLap=True)

            # TODO: "_set" prefix seems confusing. The function gets and sets
            # TODO: Perhaps, break it down into 2 function: getActualStats & setActualStats
            self._setActualStats()
            self.checkor._checkPoint("Checked actual stats", subLap=True)

            # TODO: Rename this after understanding the following functions
            self._compareAndSetExpectedAndActualStats()

            # TODO: I don't understand these two functions. Could be related to over100 and announce tags. Disabling now
            #self._checkAvgCompletionStatistics()
            #self._setPassStatisticsCheckToAnnounce()
            #self.checkor._checkPoint("Checked more detailed observed statistics", subLap=True)

            # TODO: This might be a good feature. Disabling for now. Review later.
            #self._setPassStatisticsCheckOverCompletion()

            # TODO: Function does more than output size checking, which I don't understand. Disabling for now.
            #self._checkOutputSize()
            #self.checkor._checkPoint("Checked output size", subLap=True)

            # TODO: We might disable smalllumi check. Review later. Keep it for now.
            self._checkLumiSize()
            self.checkor._checkPoint("Checked lumi size", subLap=True)

            self._checkRucioFileCounts()
            self.checkor_checkPoint("Checked Rucio file count", subLap=True)

            self._checkDBSFileCounts()
            self.checkor._checkPoint("Checked DBS file count", subLap=True)

            if self._hasFileMismatch() and "recovering" not in self.assistanceTags:
                # TODO: Test this function carefully: filemismatch, agentfilemismatch, etc.
                self._handleFileMismatch()
            self.checkor._checkPoint("Checked file count", subLap=True)

            # TODO: Test this function
            self._checkInvalidFiles()
            self.checkor._checkPoint("Checked invalid files", subLap=True)

            self.checkor._checkPoint(f"Done with {self.wf}")
            # TODO: Log this properly and understand how it is used
            self._setRecord()

            if self.isClosing:
                self.logger.info("The workflow is okay to be closed-out. Perform the action later.")
                #self._closeOutWorkflow()
            else:
                self.logger.info("The workflow is not okay to be closed-out.")
                # TODO: Rename and update dry run mode
                self._updateAssistanceStatus()
                # TODO: What's the difference from the function above? Clear here.
                self._setAssistanceStatus()

                # TODO: Disabling for now, check later.
                # self._warnRequestor()
                    
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
        # TODO: What's the difference between workflow and wf???
        response = {
            "workflow": self.workflow,
            "wf": self.wf,
            "failed": self.failed, 
            "isClosing": self.isClosing, 
            "newStatus": self.newStatus, 
            "prepIds": self.wfController.getPrepIDs(), 
            "mcmForceComplete": self.bypassChecksByMcMForceComplete,
            "record": self.record
        }

        self.logger.info("An update is required: ")
        self.logger.info(response)
        return response

