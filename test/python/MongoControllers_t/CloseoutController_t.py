#!/usr/bin/env python
"""
_CloseoutController_t_
Unit test for CloseoutController helper class.
"""

import unittest
from pymongo.collection import Collection

from Databases.Mongo.MongoClient import MongoClient
from MongoControllers.CloseoutController import CloseoutController


class MockMongoClient(MongoClient):
    def _setMongoCollection(self) -> Collection:
        return self.client.unified.closeoutInfo

    def _buildMongoDocument(self) -> None:
        pass


class CloseoutControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "closeoutInfo"}

    # CloseoutInfo is always changing.
    # For now, test the get method with a workflow got randomly from mongo.
    mockMongoClient = MockMongoClient()
    params = {
        "workflow": mockMongoClient._getOne()["name"],
        "dropKeys": ["name", "_id"],
    }

    def setUp(self) -> None:
        self.closeoutController = CloseoutController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.closeoutController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.closeoutController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.closeoutController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testBuildMongoDocument(self) -> None:
        """_buildMongoDocument builds the document to store in Mongo"""
        result = self.closeoutController._buildMongoDocument(self.params.get("workflow"), {"test": "ok"})
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isNameEqual = result.get("name") == self.params.get("workflow")
        self.assertTrue(isNameEqual)

        isFound = result.get("test") == "ok"
        self.assertTrue(isFound)

    def testGet(self):
        """get gets the closeout info for a given workflow"""
        # Test when workflow exists
        result = self.closeoutController.get(self.params.get("workflow"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        noDropKeys = all(k not in result for k in self.params.get("dropKeys"))
        self.assertTrue(noDropKeys)

        isRecorded = self.params.get("workflow") in self.closeoutController.record
        self.assertTrue(isRecorded)

        # Test when workflow does not exist
        result = self.closeoutController.get("test")
        isNone = result is None
        self.assertTrue(isNone)

    def testGetWorkflows(self):
        """get gets all workflow names"""
        result = self.closeoutController.getWorkflows()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.params.get("workflow") in result
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
