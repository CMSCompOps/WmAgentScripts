#!/usr/bin/env python
"""
_WorkflowStatusMonitor_t_
Unit test for WorkflowStatusMonitor helper class.
"""

import unittest
from pymongo.collection import Collection

from Components.Monitors.WorkflowStatusMonitor import WorkflowStatusMonitor


class WorkflowStatusMonitorTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "statusHistory"}

    # StatusHistory is always changing
    # For now, test only output types and date/time keys
    params = {"dateTimeKeys": ["time", "date"]}

    def setUp(self) -> None:
        self.workflowStatusMonitor = WorkflowStatusMonitor()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.workflowStatusMonitor.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.workflowStatusMonitor.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.workflowStatusMonitor.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the data in status history"""
        result = self.workflowStatusMonitor.get()
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
