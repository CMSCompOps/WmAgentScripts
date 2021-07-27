#!/usr/bin/env python
"""
_ModuleLockController_t_
Unit test for ModuleLockController helper class.
"""

import unittest
from unittest.mock import patch
from pymongo.collection import Collection

from Components.Module.ModuleLockController import ModuleLockController


class ModuleLockControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "moduleLock"}

    # ModuleLock is always changing.
    # For now, only test output types and content keys.
    params = {"docKeys": ["component", "pid", "host", "time", "date"]}

    def setUp(self) -> None:
        self.moduleLockController = ModuleLockController()
        super().setUp()
        return

    @patch("Components.Module.ModuleLockController.ModuleLockController.clean")
    def tearDown(self, mockClean) -> None:
        mockClean.return_value = None
        del self.moduleLockController
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.moduleLockController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.moduleLockController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.moduleLockController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the module locks"""
        result = self.moduleLockController.get()
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

    @patch("Components.Module.ModuleLockController.ModuleLockController.get")
    def testGo(self, mockGet):
        """go checks if a module is locked or not"""
        # Test when there is no locks
        mockGet.return_value = []
        result = self.moduleLockController.go()
        isBool = isinstance(result, bool)
        self.assertTrue(isBool)

        isTrue = result is True
        self.assertTrue(isTrue)

        # Test when there are locks
        mockGet.return_value = ["lock1", "lock2"]
        result = self.moduleLockController.go()
        isBool = isinstance(result, bool)
        self.assertTrue(isBool)

        isFalse = result is False
        self.assertTrue(isFalse)

        # Test when locking is False
        mockGet.return_value = []
        self.moduleLockController.locking = False
        result = self.moduleLockController.go()
        isBool = isinstance(result, bool)
        self.assertTrue(isBool)

        isTrue = result is True
        self.assertTrue(isTrue)


if __name__ == "__main__":
    unittest.main()
