#!/usr/bin/env python
"""
_ModuleLock_t_
Unit test for ModuleLock helper class.
"""

import unittest
from unittest.mock import patch
from pymongo.collection import Collection

from MoveToSomewhereElse.ModuleLock import ModuleLock


class ModuleLockTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "moduleLock"}

    # ModuleLock is always changing
    # For now, test only output types and content keys
    params = {"docKeys": ["component", "pid", "host", "time", "date"]}

    def setUp(self) -> None:
        self.moduleLock = ModuleLock()
        super().setUp()
        return

    @patch("MoveToSomewhereElse.ModuleLock.ModuleLock.clean")
    def tearDown(self, mockClean) -> None:
        mockClean.return_value = None
        del self.moduleLock
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.moduleLock.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.moduleLock.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.moduleLock.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the module locks"""
        result = self.moduleLock.get()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfDict = isinstance(result[0], dict)
        self.assertTrue(isListOfDict)

        hasAllKeys = False
        for doc in result:
            if any(k not in doc for k in self.params.get("docKeys")):
                break
        else:
            hasAllKeys = True
        self.assertTrue(hasAllKeys)

    @patch("MoveToSomewhereElse.ModuleLock.ModuleLock.get")
    def testGo(self, mockGet):
        """go checks if a module is locked or not"""
        # Test when there is no locks
        mockGet.return_value = []
        result = self.moduleLock.go()
        isBool = isinstance(result, bool)
        self.assertTrue(isBool)

        isTrue = result is True
        self.assertTrue(isTrue)

        # Test when there are locks
        mockGet.return_value = ["lock1", "lock2"]
        result = self.moduleLock.go()
        isBool = isinstance(result, bool)
        self.assertTrue(isBool)

        isFalse = result is False
        self.assertTrue(isFalse)

        # Test when locking is False
        mockGet.return_value = []
        self.moduleLock.locking = False
        result = self.moduleLock.go()
        isBool = isinstance(result, bool)
        self.assertTrue(isBool)

        isTrue = result is True
        self.assertTrue(isTrue)


if __name__ == "__main__":
    unittest.main()
