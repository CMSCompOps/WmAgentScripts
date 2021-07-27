#!/usr/bin/env python
"""
_WorkflowTrafficController_t_
Unit test for WorkflowTrafficController helper class.
"""

import unittest
from pymongo.collection import Collection

from Components.Workflow.WorkflowTrafficController import WorkflowTrafficController


class WorkflowTrafficControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "wtcInfo"}

    def setUp(self) -> None:
        self.workflowTrafficController = WorkflowTrafficController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.workflowTrafficController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.workflowTrafficController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.workflowTrafficController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGetHold(self):
        """getHold gets all data in hold"""
        result = self.workflowTrafficController.getHold()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGetBypass(self):
        """getBypass gets all data in bypass"""
        result = self.workflowTrafficController.getBypass()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGetForce(self):
        """getForce gets all data in force"""
        result = self.workflowTrafficController.getForce()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGet(self):
        """get gets all data with a given action"""
        # Test when action is invalid, since valid actions are tested by the other get methods
        result = self.workflowTrafficController.get("test")
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
