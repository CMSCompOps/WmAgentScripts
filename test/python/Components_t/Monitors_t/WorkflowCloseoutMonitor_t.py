#!/usr/bin/env python
"""
_WorkflowCloseoutMonitor_t_
Unit test for WorkflowCloseoutMonitor helper class.
"""

import unittest
from pymongo.collection import Collection

from Databases.Mongo.MongoClient import MongoClient
from Components.Monitors.WorkflowCloseoutMonitor import WorkflowCloseoutMonitor


class MockMongoClient(MongoClient):
    def _setMongoCollection(self) -> Collection:
        return self.client.unified.closeoutInfo

    def _buildMongoDocument(self) -> None:
        pass


class WorkflowCloseoutMonitorTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "closeoutInfo"}

    # CloseoutInfo is always changing.
    # For now, test the get method with a workflow got randomly from mongo.
    mockMongoClient = MockMongoClient()
    params = {
        "workflow": mockMongoClient._getOne()["name"],
        "dropKeys": ["name", "_id"],
    }

    def setUp(self) -> None:
        self.workflowCloseoutMonitor = WorkflowCloseoutMonitor()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.workflowCloseoutMonitor.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.workflowCloseoutMonitor.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.workflowCloseoutMonitor.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the closeout info for a given workflow"""
        # Test when workflow exists
        result = self.workflowCloseoutMonitor.get(self.params.get("workflow"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        noDropKeys = all(k not in result for k in self.params.get("dropKeys"))
        self.assertTrue(noDropKeys)

        isRecorded = self.params.get("workflow") in self.workflowCloseoutMonitor.record
        self.assertTrue(isRecorded)

        # Test when workflow does not exist
        result = self.workflowCloseoutMonitor.get("test")
        isNone = result is None
        self.assertTrue(isNone)

    def testGetWorkflows(self):
        """get gets all workflow names"""
        result = self.workflowCloseoutMonitor.getWorkflows()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.params.get("workflow") in result
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
