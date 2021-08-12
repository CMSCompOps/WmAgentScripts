#!/usr/bin/env python
"""
_RemainingDatasetController_t_
Unit test for RemainingDatasetController helper class.
"""

import unittest
from unittest.mock import patch, MagicMock
from time import struct_time, mktime, asctime
from pymongo.collection import Collection

from MongoControllers.RemainingDatasetController import RemainingDatasetController


class RemainingDatasetControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "remainingDatasetInfo"}

    # There are lots of datasets under this site. For now, test for only one.
    params = {
        "site": "T2_CH_CERN",
        "dataset": "/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_Test-SC_MultiPU_Agent136_Val_Alanv19-v20/GEN-SIM",
        "docKeys": ["reasons", "size"],
        "doc": {"reasons": ["unlock"], "size": 0.5},
        "now": struct_time((2021, 1, 1, 0, 0, 0, 0, 0, 0)),
    }

    def setUp(self) -> None:
        self.remainingDatasetController = RemainingDatasetController()
        super().setUp()
        return

    @patch("MongoControllers.RemainingDatasetController.RemainingDatasetController.purge")
    def tearDown(self, mockPurge: MagicMock) -> None:
        mockPurge.return_value = None
        del self.remainingDatasetController
        super().tearDown()
        return

    def testMongoSettings(self) -> None:
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.remainingDatasetController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.remainingDatasetController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.remainingDatasetController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testBuildMongoDocument(self) -> None:
        """_buildMongoDocument builds the document to store on Mongo"""
        result = self.remainingDatasetController._buildMongoDocument(
            self.params.get("site"), self.params.get("dataset"), self.params.get("doc"), now=self.params.get("now")
        )
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isSiteEqual = result.get("site") == self.params.get("site")
        self.assertTrue(isSiteEqual)

        isDatasetEqual = result.get("dataset") == self.params.get("dataset")
        self.assertTrue(isDatasetEqual)

        isReasonsEqual = result.get("reasons") == self.params.get("doc").get("reasons")
        self.assertTrue(isReasonsEqual)

        isSizeEqual = result.get("size") == self.params.get("doc").get("size")
        self.assertTrue(isSizeEqual)

        isTimeEqual = result.get("time") == mktime(self.params.get("now"))
        self.assertTrue(isTimeEqual)

        isDateEqual = result.get("date") == asctime(self.params.get("now"))
        self.assertTrue(isDateEqual)

    def testGetSites(self) -> None:
        """getSites gets list of sites"""
        result = self.remainingDatasetController.getSites()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.params.get("site") in result
        self.assertTrue(isFound)

    def testGet(self) -> None:
        """get gets the data of a given site"""
        result = self.remainingDatasetController.get(self.params.get("site"))
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

    @patch("MongoControllers.RemainingDatasetController.RemainingDatasetController.get")
    def testTell(self, mockGet: MagicMock) -> None:
        """tell prints the data for a given site"""
        with self.assertLogs(self.remainingDatasetController.logger, level="INFO") as result:
            mockGet.return_value = self.params.get("doc")
            self.remainingDatasetController.tell(self.params.get("site"))
            toldOnce = len(result.output) == 1
            self.assertTrue(toldOnce)


if __name__ == "__main__":
    unittest.main()
