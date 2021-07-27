#!/usr/bin/env python
"""
_BatchController_t_
Unit test for BatchController helper class.
"""

import unittest
from pymongo.collection import Collection

from Components.Campaign.BatchController import BatchController


class BatchControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "batchInfo"}

    def setUp(self) -> None:
        self.batchController = BatchController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.batchController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.batchController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.batchController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
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

    def testGetBatches(self):
        """getBatches gets the all the batches names"""
        result = self.batchController.getBatches()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)


if __name__ == "__main__":
    unittest.main()
