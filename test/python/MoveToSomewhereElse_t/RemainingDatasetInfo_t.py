#!/usr/bin/env python
"""
_RemainingDatasetInfo_t_
Unit test for RemainingDatasetInfo helper class.
"""

import unittest
from unittest.mock import patch
from pymongo.collection import Collection

from MoveToSomewhereElse.RemainingDatasetInfo import RemainingDatasetInfo


class RemainingDatasetInfoTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "remainingDatasetInfo"}

    # There are lots of datasets under this site. For now, test for only one.
    params = {
        "site": "T2_CH_CERN",
        "dataset": "/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_Test-SC_MultiPU_Agent136_Val_Alanv19-v20/GEN-SIM",
        "docKeys": ["reasons", "size"],
        "doc": {"reasons": ["unlock"], "size": 0.5},
        "tell": '{\n  "reasons": [\n    "unlock"\n  ], \n  "size": 0.5\n}',
    }

    def setUp(self) -> None:
        self.remainingDatasetInfo = RemainingDatasetInfo()
        super().setUp()
        return

    @patch("MoveToSomewhereElse.RemainingDatasetInfo.RemainingDatasetInfo.purge")
    def tearDown(self, mockPurge) -> None:
        mockPurge.return_value = None
        del self.remainingDatasetInfo
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.remainingDatasetInfo.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.remainingDatasetInfo.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.remainingDatasetInfo.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGetSites(self):
        """getSites gets list of sites"""
        result = self.remainingDatasetInfo.getSites()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.params.get("site") in result
        self.assertTrue(isFound)

    def testGet(self):
        """get gets the data of a given site"""
        result = self.remainingDatasetInfo.get(self.params.get("site"))
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

    @patch("MoveToSomewhereElse.RemainingDatasetInfo.RemainingDatasetInfo.get")
    def testTell(self, mockGet):
        """tell prints the data for a given site"""
        mockGet.return_value = self.params.get("doc")
        with self.assertLogs(self.remainingDatasetInfo.logger, level="INFO") as result:
            self.remainingDatasetInfo.tell(self.params.get("site"))
        isFound = self.params.get("tell") in result
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
