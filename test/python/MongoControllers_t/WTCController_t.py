#!/usr/bin/env python
"""
_WTCController_t_
Unit test for WTCController helper class.
"""

import unittest
from time import struct_time, mktime, asctime
from pymongo.collection import Collection

from MongoControllers.WTCController import WTCController


class WTCControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "wtcInfo"}

    params = {"action": "hold", "user": "test", "keyword": "ok", "now": struct_time((2021, 1, 1, 0, 0, 0, 0, 0, 0))}

    def setUp(self) -> None:
        self.wtcController = WTCController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self) -> None:
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.wtcController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.wtcController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.wtcController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testBuildMongoDocument(self) -> None:
        """_buildMongoDocument builds the document to store on Mongo"""
        result = self.wtcController._buildMongoDocument(
            self.params.get("action"), self.params.get("keyword"), self.params.get("user"), self.params.get("now")
        )
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isUserEqual = result.get("user") == self.params.get("user")
        self.assertTrue(isUserEqual)

        isKeywordEqual = result.get("keyword") == self.params.get("keyword")
        self.assertTrue(isKeywordEqual)

        isActionEqual = result.get("action") == self.params.get("action")
        self.assertTrue(isActionEqual)

        isTimeEqual = result.get("time") == mktime(self.params.get("now"))
        self.assertTrue(isTimeEqual)

        isDateEqual = result.get("date") == asctime(self.params.get("now"))
        self.assertTrue(isDateEqual)

    def testGetHold(self) -> None:
        """getHold gets all data in hold"""
        result = self.wtcController.getHold()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGetBypass(self) -> None:
        """getBypass gets all data in bypass"""
        result = self.wtcController.getBypass()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGetForce(self) -> None:
        """getForce gets all data in force"""
        result = self.wtcController.getForce()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGet(self) -> None:
        """get gets all data with a given action"""
        # Test when action is invalid, since valid actions are tested by the other get methods
        result = self.wtcController.get("test")
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
