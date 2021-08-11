#!/usr/bin/env python
"""
_StatusHistoryController_t_
Unit test for StatusHistoryController helper class.
"""

import unittest
from pymongo.collection import Collection

from MongoControllers.StatusHistoryController import StatusHistoryController


class StatusHistoryControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "statusHistory"}

    # StatusHistory is always changing
    # For now, test only output types and date/time keys
    params = {"dateTimeKeys": ["time", "date"]}

    def setUp(self) -> None:
        self.statusHistoryController = StatusHistoryController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.statusHistoryController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.statusHistoryController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.statusHistoryController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the data in status history"""
        result = self.statusHistoryController.get()
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
