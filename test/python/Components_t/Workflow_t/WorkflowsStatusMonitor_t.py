#!/usr/bin/env python
"""
_WorkflowsStatusMonitor_t_
Unit test for WorkflowsStatusMonitor helper class.
"""

import unittest
from pymongo.collection import Collection

from Components.Workflow.WorkflowsStatusMonitor import WorkflowsStatusMonitor


class WorkflowsStatusMonitorTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "statusHistory"}

    # StatusHistory is always changing
    # For now, test only output types and date/time keys
    params = {"dateTimeKeys": ["time", "date"]}

    def setUp(self) -> None:
        self.workflowsStatusMonitor = WorkflowsStatusMonitor()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.workflowsStatusMonitor.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.workflowsStatusMonitor.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.workflowsStatusMonitor.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the data in status history"""
        result = self.workflowsStatusMonitor.get()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyFloat = all(isinstance(k, int) for k in result)
        self.assertTrue(isKeyFloat)

        isValueDict = all(isinstance(v, dict) for v in result.values())
        self.assertTrue(isValueDict)

        hasDateTimeKeys = False
        for v in result.values():
            if any(k not in v for k in self.params.get("dateTimeKeys")):
                break
        else:
            hasDateTimeKeys = True
        self.assertTrue(hasDateTimeKeys)


if __name__ == "__main__":
    unittest.main()
