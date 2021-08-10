#!/usr/bin/env python
"""
_WorkflowsTrafficController_t_
Unit test for WorkflowsTrafficController helper class.
"""

import unittest
from pymongo.collection import Collection

from Components.Workflow.WorkflowsTrafficController import WorkflowsTrafficController


class WorkflowsTrafficControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "wtcInfo"}

    def setUp(self) -> None:
        self.workflowsTrafficController = WorkflowsTrafficController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.workflowsTrafficController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.workflowsTrafficController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.workflowsTrafficController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGetHold(self):
        """getHold gets all data in hold"""
        result = self.workflowsTrafficController.getHold()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGetBypass(self):
        """getBypass gets all data in bypass"""
        result = self.workflowsTrafficController.getBypass()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGetForce(self):
        """getForce gets all data in force"""
        result = self.workflowsTrafficController.getForce()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGet(self):
        """get gets all data with a given action"""
        # Test when action is invalid, since valid actions are tested by the other get methods
        result = self.workflowsTrafficController.get("test")
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
