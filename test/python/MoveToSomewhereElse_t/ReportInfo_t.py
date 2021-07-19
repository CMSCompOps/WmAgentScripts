#!/usr/bin/env python
"""
_ReportInfo_t_
Unit test for ReportInfo helper class.
"""

import unittest
from pymongo.collection import Collection

from Services.Mongo.MongoClient import MongoClient
from MoveToSomewhereElse.ReportInfo import ReportInfo


class MockMongoClient(MongoClient):
    def _setMongoCollection(self) -> Collection:
        return self.client.unified.reportInfo

    def _buildMongoDocument(self) -> None:
        pass


class ReportInfoTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "reportInfo"}

    # The data in ReportInfo is always changing.
    # For now, test the get method with a workflow got randomly from mongo.
    mockMongoClient = MockMongoClient()
    params = {"workflow": mockMongoClient._getOne()["workflow"], "dropKey": "_id", "dateTimeKeys": ["time", "date"]}

    def setUp(self) -> None:
        self.reportInfo = ReportInfo()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.reportInfo.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.reportInfo.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.reportInfo.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the report info for a given workflow"""
        #Test when the workflow exists
        result = self.reportInfo.get(self.params.get("workflow"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        noDropKey = self.params.get("dropKey") not in result
        self.assertTrue(noDropKey)

        hasDateTimeKeys = all(k in result for k in self.params.get("dateTimeKeys"))
        self.assertTrue(hasDateTimeKeys)

        #Test when the worklfow does not exist
        result = self.reportInfo.get("test")
        isNone = result is None
        self.assertTrue(isNone)

if __name__ == "__main__":
    unittest.main()

