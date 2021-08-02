#!/usr/bin/env python
"""
_ReqMgrReader_t_
Unit test for ReqMgr DBS helper class.
"""

import unittest
from Services.ReqMgr.ReqMgrReader import ReqMgrReader

from WMCore.WMSpec.WMWorkload import WMWorkload


class ReqMgrReaderTest(unittest.TestCase):

    wfParams = {
        "workflow": "pdmvserv_SMP-RunIISummer15wmLHEGS-00016_00051_v0__160525_042701_9941",
        "campaign": "RunIISummer15wmLHEGS",
    }

    def setUp(self) -> None:
        self.reqMgrReader = ReqMgrReader()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetWorkloadSummary(self) -> None:
        """getWorkloadSummary gets the workload summary for a given workflow"""
        response = self.reqMgrReader.getWorkloadSummary(self.wfParams.get("workflow"))
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

        isFound = response.get("campaign") == self.wfParams.get("campaign")
        self.assertTrue(isFound)

    def testGetSpec(self) -> None:
        """getSpec gets the workflow spec"""
        response = self.reqMgrReader.getSpec(self.wfParams.get("workflow"))
        isWorkload = isinstance(response, WMWorkload)
        self.assertTrue(isWorkload)


if __name__ == "__main__":
    unittest.main()
