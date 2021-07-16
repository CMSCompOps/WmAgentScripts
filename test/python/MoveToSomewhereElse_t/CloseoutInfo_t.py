#!/usr/bin/env python
"""
_CloseoutInfo_t_
Unit test for ReportInfo helper class.
"""

import unittest
from pymongo.collection import Collection

from Services.Mongo.MongoClient import MongoClient
from MoveToSomewhereElse.CloseoutInfo import CloseoutInfo


class MockMongoClient(MongoClient):
    def _setMongoCollection(self) -> Collection:
        return self.client.unified.closeoutInfo

    def _buildMongoDocument(self) -> None:
        pass


class CloseoutInfoTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "closeoutInfo"}

    # The data in CloseoutInfo is always changing.
    # For now, test the get method with a workflow got randomly from mongo.
    mockMongoClient = MockMongoClient()
    params = {
        "workflow": mockMongoClient._getOne()["name"],
        "dropKeys": ["name", "_id"],
    }

    def setUp(self) -> None:
        self.closeoutInfo = CloseoutInfo()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.closeoutInfo.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.closeoutInfo.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.closeoutInfo.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the closeout info for a given workflow"""
        # Test when workflow exists
        result = self.closeoutInfo.get(self.params.get("workflow"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        noDropKeys = all(k not in result for k in self.params.get("dropKeys"))
        self.assertTrue(noDropKeys)

        isRecorded = self.params.get("workflow") in self.closeoutInfo.record
        self.assertTrue(isRecorded)

        # Test when workflow does not exist
        result = self.closeoutInfo.get("test")
        isNone = result is None
        self.assertTrue(isNone)

    def testGetWorkflows(self):
        """get gets all workflow names"""
        result = self.closeoutInfo.getWorkflows()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.params.get("workflow") in result
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
