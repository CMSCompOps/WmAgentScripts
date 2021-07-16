#!/usr/bin/env python
"""
_WtcInfo_t_
Unit test for WtcInfo helper class.
"""

import unittest
from pymongo.collection import Collection

from MoveToSomewhereElse.WtcInfo import WtcInfo


class WtcInfoTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "wtcInfo"}

    def setUp(self) -> None:
        self.wtcInfo = WtcInfo()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.wtcInfo.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.wtcInfo.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.wtcInfo.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGetHold(self):
        """getHold gets all data in hold"""
        result = self.wtcInfo.getHold()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGetBypass(self):
        """getBypass gets all data in bypass"""
        result = self.wtcInfo.getBypass()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGetForce(self):
        """getForce gets all data in force"""
        result = self.wtcInfo.getForce()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

    def testGet(self):
        """get gets all data with a given action"""
        # Test when action is invalid, since valid actions are tested by the other get methods
        result = self.wtcInfo.get("test")
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
