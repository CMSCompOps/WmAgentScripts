#!/usr/bin/env python
"""
_StartStopTimeController_t_
Unit test for StartStopTimeController helper class.
"""

import unittest
from time import struct_time, mktime
from pymongo.collection import Collection

from MongoControllers.StartStopTimeController import StartStopTimeController


class StartStopTimeControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "startStopTime"}

    # StartStopInfo is always changing.
    # For now, only test output types for a unified component.
    params = {
        "component": "htmlor",
        "start": mktime(struct_time((2021, 1, 1, 0, 0, 0, 0, 0, 0))),
        "stop": mktime(struct_time((2021, 12, 1, 0, 0, 0, 0, 0, 0))),
    }

    def setUp(self) -> None:
        self.startStopTimeController = StartStopTimeController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self) -> None:
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.startStopTimeController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.startStopTimeController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.startStopTimeController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testBuildMongoDocument(self) -> None:
        """_buildMongoDocument builds the document to store on Mongo"""
        result = self.startStopTimeController._buildMongoDocument(
            self.params.get("component"), self.params.get("start"), self.params.get("stop")
        )
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isComponentEqual = result.get("component") == self.params.get("component")
        self.assertTrue(isComponentEqual)

        isStartEqual = result.get("start") == self.params.get("start")
        self.assertTrue(isStartEqual)

        isStopEqual = result.get("stop") == self.params.get("stop")
        self.assertTrue(isStopEqual)

        hasLapKey = "lap" in result
        self.assertTrue(hasLapKey)

    def testGet(self) -> None:
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
