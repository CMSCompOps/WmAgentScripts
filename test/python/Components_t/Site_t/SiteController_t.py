#!/usr/bin/env python
"""
_SiteController_t_
Unit test for SiteController helper class.
"""

import unittest
from collections import defaultdict

from Components.Site.SiteController import SiteController


class SiteControllerTest(unittest.TestCase):
    # The sites status can change all the time. For now test functions with mock values.
    mockParams = {
        "sitesReady": {
            "T3_US_SDSC",
            "T3_US_NERSC",
            "T3_US_PSC",
            "T3_CH_CERN_HelixNebula",
            "T3_US_OSG",
            "T3_CH_CERN_HelixNebula_REHA",
            "T3_US_Colorado",
            "T3_US_TACC",
        },
        "disk": defaultdict(int, {"T1_US_FNAL_Disk": 0, "T2_CH_CERN": 0}),
    }

    # Output params for given mock values.
    sitesParam = {
        "production": "T2_CH_CERN",
        "vetoTransferSites": {"T1_US_FNAL_Disk", "T2_CH_CERN"},
        "cpuPledges": {"T1_US_FNAL_Disk": 0, "T2_CH_CERN": 0},
        "disk": {"T1_US_FNAL_Disk": 0, "T2_CH_CERN": 0},
        "se": "T1_US_FNAL_Disk",
        "ce": "T1_US_FNAL",
    }

    def setUp(self) -> None:
        self.siteController = SiteController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetSitesReadyInAgent(self) -> None:
        """getSitesReadyInAgent gets the sites ready in production"""
        result = self.siteController.getSitesReadyInAgent()
        isSet = isinstance(result, set)
        self.assertTrue(isSet)

        isSetOfStr = isinstance(list(result)[0], str)
        self.assertTrue(isSetOfStr)

        isFound = self.sitesParam in result
        self.assertTrue(isFound)

    def testGetTotalDisk(self) -> None:
        """getTotalDisk gets total disk"""
        self.siteController.sitesReady = self.mockParams.get("sitesReady")
        self.siteController.disk = self.mockParams.get("disk")
        result = self.siteController.getTotalDisk()
        isInt = isinstance(result, int)
        self.assertTrue(isInt)

        isZero = result == 0
        self.assertTrue(isZero)

    def testGetCpuPledgesAndDisk(self) -> None:
        """getCpuPledgesAndDisk gets the cpu pledges and disk"""
        self.siteController.allSites = self.mockParams.get("sitesReady")
        result = self.siteController.getCpuPledgesAndDisk()
        isTuple = isinstance(result, tuple)
        self.assertTrue(isTuple)

        isDict = isinstance(result[0], dict)
        self.assertTrue(isDict)
        isEqual = result[0] == self.sitesParam.get("cpuPledges")
        self.assertTrue(isEqual)

        isDict = isinstance(result[1], dict)
        self.assertTrue(isDict)
        isEqual = result[0] == self.sitesParam.get("disk")
        self.assertTrue(isEqual)

    def testGetTierSites(self) -> None:
        """getTierSites gets the tier sites"""
        self.siteController.sitesReady = self.mockParams.get("sitesReady")

        # Test when tier 0
        result = self.siteController.getTierSites(0)
        isSet = isinstance(result, set)
        self.assertTrue(isSet)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)

        # Test when tier 3
        result = self.siteController.getTierSites(3)
        isSet = isinstance(result, set)
        self.assertTrue(isSet)

        isSetOfStr = isinstance(list(result)[0], str)
        self.assertTrue(isSetOfStr)

        isEqual = result == self.mockParams.get("sitesReady")
        self.assertTrue(isEqual)

    def testGetVetoTransferSites(self) -> None:
        """getVetoTransferSites gets the veto transfer sites"""
        self.siteController.disk = self.mockParams.get("disk")
        result = self.siteController.getVetoTransferSites()
        isSet = isinstance(result, set)
        self.assertTrue(isSet)

        isSetOfStr = isinstance(list(result)[0], str)
        self.assertTrue(isSetOfStr)

        isEqual = result == self.sitesParam.get("vetoTransferSites")
        self.assertTrue(isEqual)

    def testSEToCE(self) -> None:
        """SEToCE maps SE to CE"""
        result = self.siteController.SEToCE(self.sitesParam.get("se"))
        isStr = isinstance(result, str)
        self.assertTrue(isStr)

        isFound = result == self.sitesParam.get("ce")
        self.assertTrue(isFound)

    def testSEToCEs(self) -> None:
        """SEToCEs maps SE to list of CE"""
        result = self.siteController.SEToCEs(self.sitesParam.get("se"))
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = result[0] == self.sitesParam.get("ce")
        self.assertTrue(isFound)

    def testCEToSE(self) -> None:
        """CEToSE maps CE to SE"""
        result = self.siteController.CEToSE(self.sitesParam.get("ce"))
        isStr = isinstance(result, str)
        self.assertTrue(isStr)

        isFound = result == self.sitesParam.get("se")
        self.assertTrue(isFound)

    def testCEToSEs(self) -> None:
        """CEToSEs maps CE to list of SE"""
        result = self.siteController.CEToSEs(self.sitesParam.get("ce"))
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
