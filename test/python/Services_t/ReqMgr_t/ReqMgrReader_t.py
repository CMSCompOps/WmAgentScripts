#!/usr/bin/env python
"""
_ReqMgrReader_t_
Unit test for ReqMgr DBS helper class.
"""

import unittest
from Services.ReqMgr.ReqMgrReader import ReqMgrReader

from WMCore.WMSpec.WMWorkload import WMWorkload


class ReqMgrReaderTest(unittest.TestCase):

    # There are many more workflows under this campaign, but that information could be updated as new workflows comes in
    # For now, just test for one of the workflows
    campaignParams = {
        "campaign": "Run3Winter21DRMiniAOD",
        "workflow": "cmsunified_task_TSG-Run3Winter21DRMiniAOD-00081__v1_T_210507_182332_1792",
    }

    # There is only one workflow for this output dataset
    # This data should not change over time
    outputParams = {
        "dataset": "/WplusToMuNu_NNPDF30_TuneEE5C_13TeV-powheg-herwigpp/RunIISummer15wmLHEGS-MCRUN2_71_V1-v1/GEN-SIM",
        "workflow": "pdmvserv_SMP-RunIISummer15wmLHEGS-00016_00051_v0__160525_042701_9941",
        "campaign": "RunIISummer15wmLHEGS",
    }

    # There are five workflows under this prep id
    # This data should not change over time
    prepIdParams = {
        "prep_id": "task_TOP-RunIIFall17wmLHEGS-00462",
        "workflows": [
            "pdmvserv_task_TOP-RunIIFall17wmLHEGS-00462__v1_T_201015_153154_2761",
            "cmsunified_ACDC0_task_TOP-RunIIFall17wmLHEGS-00462__v1_T_201207_062607_770",
            "cmsunified_ACDC0_task_TOP-RunIIFall17wmLHEGS-00462__v1_T_201207_062611_3260",
            "cmsunified_ACDC0_task_TOP-RunIIFall17wmLHEGS-00462__v1_T_201207_062602_7937",
            "cmsunified_ACDC0_task_TOP-RunIIFall17wmLHEGS-00462__v1_T_201207_062558_3366",
        ],
    }

    # There are three worflows under this combination of status, user and request type
    # Requests are made using the three filters, it will return too much data o/w
    statusParams = {
        "status": "normal-archived",
        "user": "sagarwal",
        "requestType": "ReReco",
        "workflows": [
            "sagarwal_Run2017E-31Mar2018-v1-HighMultiplicityEOF3-Nano1June2019_10215_190607_141831_7758",
            "sagarwal_Run2018A-v1-Commissioning-06Jun2018_1015_180823_210612_6118",
            "sagarwal_Run2017H-v1-SingleMuon-14Jan2019_944_190129_125411_428",
        ],
        "subrtype": "",
    }

    # Other workflow params
    otherWorkflowsParams = {
        "toTestSchema": {
            "workflow": "pdmvserv_task_BPH-RunIIFall18GS-00350__v1_T_201021_154340_8354",
        },
        "toTestSplittings": {
            "workflow": "pdmvserv_task_BPH-RunIIFall18GS-00350__v1_T_201021_154340_8354",
        },
    }

    # The values in the info response change, so just test for keys
    infoParams = {
        "keys": [
            "wmcore_reqmgr_version",
            "reqmgr_db_info",
            "reqmgr_last_injected_request",
        ]
    }

    def setUp(self) -> None:
        self.reqMgrReader = ReqMgrReader()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetWorkflowSchema(self) -> None:
        """getWorkflowSchema gets schema for a given workflow"""
        schema = self.reqMgrReader.getWorkflowSchema(self.otherWorkflowsParams.get("toTestSchema").get("workflow"))
        isDict = isinstance(schema, dict)
        self.assertTrue(isDict)

        isFound = schema["RequestName"] == self.otherWorkflowsParams.get("toTestSchema").get("workflow")
        self.assertTrue(isFound)

    def testGetWorkflowsByCampaign(self) -> None:
        """getWorkflowsByCampaign gets workflows for a given campaign"""
        workflows = self.reqMgrReader.getWorkflowsByCampaign(self.campaignParams.get("campaign"), details=True)
        isList = isinstance(workflows, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(workflows[0], dict)
        self.assertTrue(isListOfDicts)

        isFound = False
        for workflow in workflows:
            if workflow["RequestName"] == self.campaignParams.get("workflow"):
                isFound = True
        self.assertTrue(isFound)

    def testGetWorkflowsByOutput(self) -> None:
        """getWorkflowsByOutput gets workflows for a given output"""
        # Test when details is False
        workflows = self.reqMgrReader.getWorkflowsByOutput(self.outputParams.get("dataset"))
        isList = isinstance(workflows, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(workflows[0], str)
        self.assertTrue(isListOfStr)

        isFound = workflows[0] == self.outputParams.get("workflow")
        self.assertTrue(isFound)

        # Test when details is True
        workflows = self.reqMgrReader.getWorkflowsByOutput(self.outputParams.get("dataset"), details=True)
        isList = isinstance(workflows, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(workflows[0], dict)
        self.assertTrue(isListOfDicts)

        isFound = workflows[0]["RequestName"] == self.outputParams.get("workflow")
        self.assertTrue(isFound)

    def testGetWorkflowsByPrepId(self) -> None:
        """getWorkflowsByPrepId gets workflows for a given prep id"""
        # Test when details is False
        workflows = self.reqMgrReader.getWorkflowsByPrepId(self.prepIdParams.get("prep_id"))
        isList = isinstance(workflows, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(workflows[0], str)
        self.assertTrue(isListOfStr)

        sameLen = len(workflows) == len(self.prepIdParams.get("workflows"))
        self.assertTrue(sameLen)

        isFound = all(workflow in self.prepIdParams.get("workflows") for workflow in workflows)
        self.assertTrue(isFound)

        # Test when details is True
        workflows = self.reqMgrReader.getWorkflowsByPrepId(self.prepIdParams.get("prep_id"), details=True)
        isList = isinstance(workflows, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(workflows[0], dict)
        self.assertTrue(isListOfDicts)

        sameLen = len(workflows) == len(self.prepIdParams.get("workflows"))
        self.assertTrue(sameLen)

        isFound = False
        for workflow in workflows:
            if workflow["RequestName"] not in self.prepIdParams.get("workflows"):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

    def testGetWorkflowsByStatus(self) -> None:
        """getWorkflowsByStatus gets workflows for a given status"""
        workflows = self.reqMgrReader.getWorkflowsByStatus(
            self.statusParams.get("status"),
            user=self.statusParams.get("user"),
            details=False,
            requestType=self.statusParams.get("requestType"),
        )
        isList = isinstance(workflows, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(workflows[0], str)
        self.assertTrue(isListOfStr)

        sameLen = len(workflows) == len(self.statusParams.get("workflows"))
        self.assertTrue(sameLen)

        isFound = all(workflow in self.statusParams.get("workflows") for workflow in workflows)
        self.assertTrue(isFound)

    def testGetWorkflowsByNames(self) -> None:
        """getWorkflowsByNames gets workflows for a given name"""
        workflows = self.reqMgrReader.getWorkflowsByNames(
            self.statusParams.get("workflows"),
            details=True,
        )
        isList = isinstance(workflows, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(workflows[0], dict)
        self.assertTrue(isListOfDicts)

        sameLen = len(workflows) == len(self.statusParams.get("workflows"))
        self.assertTrue(sameLen)

        isFound = False
        for workflow in workflows:
            if workflow["RequestName"] not in self.statusParams.get("workflows"):
                break
        else:
            isFound = True
        self.assertTrue(isFound)

        isSubRequstTypeEqual = all(
            workflow["SubRequestType"] == self.statusParams.get("subrtype") for workflow in workflows
        )
        self.assertTrue(isSubRequstTypeEqual)

    def testGetReqmgrInfo(self) -> None:
        """getReqmgrInfo gets reqmgr info"""
        info = self.reqMgrReader.getReqmgrInfo()
        isList = isinstance(info, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(info[0], dict)
        self.assertTrue(isListOfDicts)

        sameLen = len(info[0]) == len(self.infoParams.get("keys"))
        self.assertTrue(sameLen)

        isFound = all(i in self.infoParams.get("keys") for i in info[0])
        self.assertTrue(isFound)

    def testGetSplittingsSchema(self) -> None:
        """getSplittingsSchema gets splittings for a given workflow name"""
        splittings = self.reqMgrReader.getSplittingsSchema(
            self.otherWorkflowsParams.get("toTestSplittings").get("workflow"),
        )
        isList = isinstance(splittings, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(splittings[0], dict)
        self.assertTrue(isListOfDicts)

        isFound = all(
            self.otherWorkflowsParams.get("toTestSplittings").get("workflow") in splt["taskName"]
            for splt in splittings
        )
        self.assertTrue(isFound)

    def testGetWorkloadSummary(self) -> None:
        """getWorkloadSummary gets the workload summary for a given workflow"""
        response = self.reqMgrReader.getWorkloadSummary(self.outputParams.get("workflow"))
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isFound = response.get("campaign") == self.outputParams.get("campaign")
        self.assertTrue(isFound)

    def testGetSpec(self) -> None:
        """getSpec gets the workflow spec"""
        response = self.reqMgrReader.getSpec(self.outputParams.get("workflow"))
        isWorkload = isinstance(response, WMWorkload)
        self.assertTrue(isWorkload)


if __name__ == "__main__":
    unittest.main()
