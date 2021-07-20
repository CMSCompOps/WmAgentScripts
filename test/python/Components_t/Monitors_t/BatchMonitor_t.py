#!/usr/bin/env python
"""
_BatchMonitor_t_
Unit test for BatchMonitor helper class.
"""

import unittest
from pymongo.collection import Collection

from Components.Monitors.BatchMonitor import BatchMonitor


class BatchMonitorTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "batchInfo"}

    def setUp(self) -> None:
        self.batchMonitor = BatchMonitor()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.batchMonitor.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.batchMonitor.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.batchMonitor.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the batches names and ids"""
        result = self.batchMonitor.get()
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
        result = self.batchMonitor.getBatches()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)


if __name__ == "__main__":
    unittest.main()
