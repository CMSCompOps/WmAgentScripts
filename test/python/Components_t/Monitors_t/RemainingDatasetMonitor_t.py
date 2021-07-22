#!/usr/bin/env python
"""
_RemainingDatasetMonitor_t_
Unit test for RemainingDatasetMonitor helper class.
"""

import unittest
from unittest.mock import patch
from pymongo.collection import Collection

from Components.Monitors.RemainingDatasetMonitor import RemainingDatasetMonitor


class RemainingDatasetMonitorTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "remainingDatasetInfo"}

    # There are lots of datasets under this site. For now, test for only one.
    params = {
        "site": "T2_CH_CERN",
        "dataset": "/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_Test-SC_MultiPU_Agent136_Val_Alanv19-v20/GEN-SIM",
        "docKeys": ["reasons", "size"],
        "doc": {"reasons": ["unlock"], "size": 0.5},
    }

    def setUp(self) -> None:
        self.remainingDatasetMonitor = RemainingDatasetMonitor()
        super().setUp()
        return

    @patch("Components.Monitors.RemainingDatasetMonitor.RemainingDatasetMonitor.purge")
    def tearDown(self, mockPurge) -> None:
        mockPurge.return_value = None
        del self.remainingDatasetMonitor
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.remainingDatasetMonitor.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.remainingDatasetMonitor.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.remainingDatasetMonitor.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGetSites(self):
        """getSites gets list of sites"""
        result = self.remainingDatasetMonitor.getSites()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.params.get("site") in result
        self.assertTrue(isFound)

    def testGet(self):
        """get gets the data of a given site"""
        result = self.remainingDatasetMonitor.get(self.params.get("site"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueDict = all(isinstance(v, dict) for v in result.values())
        self.assertTrue(isValueDict)

        isFound = False
        for k in result:
            if k == self.params.get("dataset"):
                isFound = True
                break
        self.assertTrue(isFound)

        hasAllKeys = all(k in result[self.params.get("dataset")] for k in self.params.get("docKeys"))
        self.assertTrue(hasAllKeys)

    @patch("Components.Monitors.RemainingDatasetMonitor.RemainingDatasetMonitor.get")
    def testTell(self, mockGet):
        """tell prints the data for a given site"""
        with self.assertLogs(self.remainingDatasetMonitor.logger, level="INFO") as result:
            mockGet.return_value = self.params.get("doc")
            self.remainingDatasetMonitor.tell(self.params.get("site"))
            toldOnce = len(result.output) == 1
            self.assertTrue(toldOnce)


if __name__ == "__main__":
    unittest.main()
