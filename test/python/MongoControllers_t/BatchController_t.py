#!/usr/bin/env python
"""
_BatchController_t_
Unit test for BatchController helper class.
"""

import unittest
from pymongo.collection import Collection

from MongoControllers.BatchController import BatchController


class BatchControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "batchInfo"}

    params = {
        "batch": "CMSSW_9_4_11_cand2__fastSim_premix_nosignal-1541303294",
        "prepId": "CMSSW_9_4_11_cand2__fastSim_premix_nosignal-1541303294-FS_PREMIXUP15_PU25",
    }

    def setUp(self) -> None:
        self.batchController = BatchController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self) -> None:
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.batchController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.batchController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.batchController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testBuildMongoDocument(self) -> None:
        """_buildMongoDocument builds the document to store in Mongo"""
        result = self.batchController._buildMongoDocument(self.params.get("batch"), [self.params.get("prepId")])
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isNameEqual = result.get("name") == self.params.get("batch")
        self.assertTrue(isNameEqual)

        isFound = self.params.get("prepId") in result.get("ids")
        self.assertTrue(isFound)

    def testGet(self) -> None:
        """get gets the batches names and ids"""
        result = self.batchController.get()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

        isValueListOfStr = all(isinstance(v[0], str) for v in result.values())
        self.assertTrue(isValueListOfStr)

    def testGetBatches(self) -> None:
        """getBatches gets the all the batches names"""
        result = self.batchController.getBatches()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)


if __name__ == "__main__":
    unittest.main()
