#!/usr/bin/env python
"""
_BatchesController_t_
Unit test for BatchesController helper class.
"""

import unittest
from pymongo.collection import Collection

from Components.BatchesController import BatchesController


class BatchesControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "batchInfo"}

    def setUp(self) -> None:
        self.batchesController = BatchesController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.batchesController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.batchesController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.batchesController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the batches names and ids"""
        result = self.batchesController.get()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

        isValueListOfStr = all(isinstance(v[0], str) for v in result.values())
        self.assertTrue(isValueListOfStr)

    def testGetBatches(self):
        """getBatches gets the all the batches names"""
        result = self.batchesController.getBatches()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)


if __name__ == "__main__":
    unittest.main()
