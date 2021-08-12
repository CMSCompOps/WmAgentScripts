#!/usr/bin/env python
"""
_StatusHistoryController_t_
Unit test for StatusHistoryController helper class.
"""

import unittest
from time import struct_time, mktime, asctime
from pymongo.collection import Collection

from MongoControllers.StatusHistoryController import StatusHistoryController


class StatusHistoryControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "statusHistory"}

    # StatusHistory is always changing
    # For now, test only output types and date/time keys
    params = {"dateTimeKeys": ["time", "date"], "now": struct_time((2021, 1, 1, 0, 0, 0, 0, 0, 0))}

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

    def testBuildMongoDocument(self) -> None:
        """_buildMongoDocument builds the document to store on Mongo"""
        result = self.statusHistoryController._buildMongoDocument({"test": "ok"}, self.params.get("now"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isTimeEqual = result.get("time") == mktime(self.params.get("now"))
        self.assertTrue(isTimeEqual)

        isDateEqual = result.get("date") == asctime(self.params.get("now"))
        self.assertTrue(isDateEqual)

        isFound = result.get("test") == "ok"
        self.assertTrue(isFound)

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
