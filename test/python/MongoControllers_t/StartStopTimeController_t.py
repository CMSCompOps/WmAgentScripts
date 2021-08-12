#!/usr/bin/env python
"""
_StartStopTimeController_t_
Unit test for StartStopTimeController helper class.
"""

import unittest
from pymongo.collection import Collection

from MongoControllers.StartStopTimeController import StartStopTimeController


class StartStopTimeControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "startStopTime"}

    # StartStopInfo is always changing.
    # For now, only test output types for a unified component.
    params = {"component": "htmlor"}

    def setUp(self) -> None:
        self.startStopTimeController = StartStopTimeController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.startStopTimeController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.startStopTimeController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.startStopTimeController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the start/stop info"""
        # Test valid component and metric
        result = self.startStopTimeController.get(self.params.get("component"))
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfInt = isinstance(result[0], int)
        self.assertTrue(isListOfInt)

        # Test invalid component
        result = self.startStopTimeController.get("test")
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)

        # Test invalid metric
        result = self.startStopTimeController.get(self.params.get("component"), metric="test")
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
