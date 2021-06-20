#!/usr/bin/env python
"""
_ReqMgrReader_t_
Unit test for ReqMgr DBS helper class.
"""

import unittest

from Services.ReqMgr.ReqMgrReader import ReqMgrReader

DATASET_INVALID = '/ggXToJPsiJPsi_JPsiToMuMu_M6p2_JPCZeroMinusPlus_TuneCP5_13TeV-pythia8-JHUGen/RunIIFall17pLHE-93X_mc2017_realistic_v3-v2/LHE'


class ReqMgrReaderTest(unittest.TestCase):

    # There are many more workflows under this campaign, but that information could be updated as new workflows comes in
    # For now, just test for one of the workflows
    campaignParams = {
        "campaign": "Run3Winter21DRMiniAOD",
        "workflow": "cmsunified_task_TSG-Run3Winter21DRMiniAOD-00081__v1_T_210507_182332_1792"
    }

    def testGetWorkflowByCampaign(self):
        """getWorkflowByCampaign gets workflows for a given campaign"""
        reqMgrReader = ReqMgrReader()
        workflows = reqMgrReader.getWorkflowByCampaign(self.campaignParams.get("campaign"), details=True)
        isFound = False
        for workflow in workflows:
            if workflow["RequestName"] == self.campaignParams.get("workflow"):
                isFound = True
        self.assertTrue(isFound)


if __name__ == '__main__':
    unittest.main()