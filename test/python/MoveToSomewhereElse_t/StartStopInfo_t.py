#!/usr/bin/env python
"""
_StartStopInfo_t_
Unit test for StartStopInfo helper class.
"""

import unittest
from pymongo.collection import Collection

from MoveToSomewhereElse.StartStopInfo import StartStopInfo


class StartStopInfoTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "startStopTime"}

    # StartStopInfo is always changing
    # For now, test only output types for a unified component
    params = {"component": "htmlor"}

    def setUp(self) -> None:
        self.startStopInfo = StartStopInfo()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.startStopInfo.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.startStopInfo.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.startStopInfo.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the start/stop info"""
        # Test valid component and metric
        result = self.startStopInfo.get(self.params.get("component"))
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfInt = isinstance(result[0], int)
        self.assertTrue(isListOfInt)

        # Test invalid component
        result = self.startStopInfo.get("test")
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)

        # Test invalid metric
        result = self.startStopInfo.get(self.params.get("component"), metric="test")
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
