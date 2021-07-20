#!/usr/bin/env python
"""
_ModuleLocker_t_
Unit test for ModuleLocker helper class.
"""

import unittest
from unittest.mock import patch
from pymongo.collection import Collection

from Components.Lockers.ModuleLocker import ModuleLocker


class ModuleLockerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "moduleLock"}

    # ModuleLock is always changing.
    # For now, only test output types and content keys.
    params = {"docKeys": ["component", "pid", "host", "time", "date"]}

    def setUp(self) -> None:
        self.moduleLocker = ModuleLocker()
        super().setUp()
        return

    @patch("Components.Lockers.ModuleLocker.ModuleLocker.clean")
    def tearDown(self, mockClean) -> None:
        mockClean.return_value = None
        del self.moduleLocker
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.moduleLocker.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.moduleLocker.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.moduleLocker.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the module locks"""
        result = self.moduleLocker.get()
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

    @patch("Components.Lockers.ModuleLocker.ModuleLocker.get")
    def testGo(self, mockGet):
        """go checks if a module is locked or not"""
        # Test when there is no locks
        mockGet.return_value = []
        result = self.moduleLocker.go()
        isBool = isinstance(result, bool)
        self.assertTrue(isBool)

        isTrue = result is True
        self.assertTrue(isTrue)

        # Test when there are locks
        mockGet.return_value = ["lock1", "lock2"]
        result = self.moduleLocker.go()
        isBool = isinstance(result, bool)
        self.assertTrue(isBool)

        isFalse = result is False
        self.assertTrue(isFalse)

        # Test when locking is False
        mockGet.return_value = []
        self.moduleLocker.locking = False
        result = self.moduleLocker.go()
        isBool = isinstance(result, bool)
        self.assertTrue(isBool)

        isTrue = result is True
        self.assertTrue(isTrue)


if __name__ == "__main__":
    unittest.main()
