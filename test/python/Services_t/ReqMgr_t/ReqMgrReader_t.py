#!/usr/bin/env python
"""
_ReqMgrReader_t_
Unit test for ReqMgr DBS helper class.
"""

import unittest
from Services.ReqMgr.ReqMgrReader import ReqMgrReader


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
    # It will return too much data o/w
    statusParams = {
        "status": "normal-archived",
        "user": "sagarwal",
        "rtype": "ReReco",
        "workflows": [
            "sagarwal_Run2017E-31Mar2018-v1-HighMultiplicityEOF3-Nano1June2019_10215_190607_141831_7758",
            "sagarwal_Run2018A-v1-Commissioning-06Jun2018_1015_180823_210612_6118",
            "sagarwal_Run2017H-v1-SingleMuon-14Jan2019_944_190129_125411_428",
        ],
    }

    otherWorkflowsParams = {
        "toTestSchema": {
            "workflow": "pdmvserv_task_BPH-RunIIFall18GS-00350__v1_T_201021_154340_8354",
        },
    }

    def testGetWorkflowSchema(self):
        """getWorkflowSchema gets schema for a given workflow"""
        reqMgrReader = ReqMgrReader()
        schema = reqMgrReader.getWorkflowSchema(
            self.otherWorkflowsParams.get("toTestSchema").get("workflow"), tries=1
        )
        isDict = isinstance(schema, dict)
        self.assertTrue(isDict)

        isFound = schema["RequestName"] == self.otherWorkflowsParams.get(
            "toTestSchema"
        ).get("workflow")
        self.assertTrue(isFound)

    def testGetWorkflowByCampaign(self):
        """getWorkflowSchemaByCampaign gets workflows for a given campaign"""
        reqMgrReader = ReqMgrReader()
        workflows = reqMgrReader.getWorkflowSchemaByCampaign(
            self.campaignParams.get("campaign"), details=True
        )
        isFound = False
        for workflow in workflows:
            if workflow["RequestName"] == self.campaignParams.get("workflow"):
                isFound = True
        self.assertTrue(isFound)

    def testGetWorkflowSchemaByOutput(self):
        """getWorkflowSchemaByOutput gets workflows for a given output"""
        # Test when details is False
        reqMgrReader = ReqMgrReader()
        workflows = reqMgrReader.getWorkflowSchemaByOutput(
            self.outputParams.get("dataset")
        )
        isFound = workflows[0] == self.outputParams.get("workflow")
        self.assertTrue(isFound)

        # Test when details is True
        workflows = reqMgrReader.getWorkflowSchemaByOutput(
            self.outputParams.get("dataset"), details=True
        )
        isFound = workflows[0]["RequestName"] == self.outputParams.get("workflow")
        self.assertTrue(isFound)

    def testGetWorkflowSchemaByPrepId(self):
        """getWorkflowSchemaByPrepId gets workflows for a given prep id"""
        # Test when details is False
        reqMgrReader = ReqMgrReader()
        workflows = reqMgrReader.getWorkflowSchemaByPrepId(
            self.prepIdParams.get("prep_id")
        )
        sameLen = len(workflows) == len(self.prepIdParams.get("workflows"))
        self.assertTrue(sameLen)

        isFound = all(
            workflow in self.prepIdParams.get("workflows") for workflow in workflows
        )
        self.assertTrue(isFound)

        # Test when details is True
        workflows = reqMgrReader.getWorkflowSchemaByPrepId(
            self.prepIdParams.get("prep_id"), details=True
        )
        sameLen = len(workflows) == len(self.prepIdParams.get("workflows"))
        self.assertTrue(sameLen)

        isFound = False
        for workflow in workflows:
            if workflow not in self.prepIdParams.get("workflows"):
                break
        else:
            isFound = True

    def testGetWorkflowsByStatus(self):
        """getWorkflowsByStatus gets workflows for a given status"""
        reqMgrReader = ReqMgrReader()
        workflows = reqMgrReader.getWorkflowsByStatus(
            self.statusParams.get("status"),
            user=self.statusParams.get("user"),
            details=False,
            rtype=self.statusParams.get("rtype"),
            tries=1,
        )
        sameLen = len(workflows) == len(self.statusParams.get("workflows"))
        self.assertTrue(sameLen)

        isFound = all(
            workflow in self.statusParams.get("workflows") for workflow in workflows
        )
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
