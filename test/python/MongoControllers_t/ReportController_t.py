#!/usr/bin/env python
"""
_ReportController_t_
Unit test for ReportController helper class.
"""

import unittest
from time import struct_time, mktime, asctime
from pymongo.collection import Collection

from Databases.Mongo.MongoClient import MongoClient
from MongoControllers.ReportController import ReportController


class MockMongoClient(MongoClient):
    def _setMongoCollection(self) -> Collection:
        return self.client.unified.reportInfo

    def _buildMongoDocument(self) -> None:
        pass


class ReportControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "reportInfo"}

    # The data in ReportInfo is always changing.
    # For now, test the get method with a workflow got randomly from mongo.
    mockMongoClient = MockMongoClient()
    params = {
        "workflow": mockMongoClient._getOne()["workflow"],
        "dropKey": "_id",
        "dateTimeKeys": ["time", "date"],
        "now": struct_time((2021, 1, 1, 0, 0, 0, 0, 0, 0)),
    }

    def setUp(self) -> None:
        self.reportController = ReportController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.reportController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.reportController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.reportController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testConvertValues(self):
        """_convertValues converts Mongo document values to required types"""
        # Test when value is set
        result = self.reportController._convertValues({"test"})
        isList = isinstance(result, list)
        self.assertTrue(isList)

        # Test when value is dict
        result = self.reportController._convertValues({"test": {"ok"}})
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

        isFound = result.get("test")[0] == "ok"
        self.assertTrue(isFound)

        # Test when value is not set nor dict
        result = self.reportController._convertValues("test")
        isStr = isinstance(result, str)
        self.assertTrue(isStr)

    def testBuildMongoDocument(self) -> None:
        """_buildMongoDocument builds the document to store on Mongo"""
        result = self.reportController._buildMongoDocument({"test": "ok"}, now=self.params.get("now"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isTimeEqual = result.get("time") == mktime(self.params.get("now"))
        self.assertTrue(isTimeEqual)

        isDateEqual = result.get("date") == asctime(self.params.get("now"))
        self.assertTrue(isDateEqual)

        isFound = result.get("test") == "ok"
        self.assertTrue(isFound)

    def testGet(self):
        """get gets the report info for a given workflow"""
        # Test when the workflow exists
        result = self.reportController.get(self.params.get("workflow"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        noDropKey = self.params.get("dropKey") not in result
        self.assertTrue(noDropKey)

        hasDateTimeKeys = all(k in result for k in self.params.get("dateTimeKeys"))
        self.assertTrue(hasDateTimeKeys)

        # Test when the worklfow does not exist
        result = self.reportController.get("test")
        isNone = result is None
        self.assertTrue(isNone)


if __name__ == "__main__":
    unittest.main()
