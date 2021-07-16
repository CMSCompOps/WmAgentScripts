#!/usr/bin/env python
"""
_StatusHistory_t_
Unit test for StatusHistory helper class.
"""

import unittest
from pymongo.collection import Collection

from MoveToSomewhereElse.StatusHistory import StatusHistory


class StatusHistoryTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "statusHistory"}

    # StatusHistory is always changing
    # For now, test only output types and date/time keys
    params = {"dateTimeKeys": ["time", "date"]}

    def setUp(self) -> None:
        self.statusHistory = StatusHistory()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.statusHistory.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.statusHistory.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.statusHistory.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the data in status history"""
        result = self.statusHistory.get()
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
