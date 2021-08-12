#!/usr/bin/env python
"""
_GlobalLockController_t_
Unit test for GlobalLockController helper class.
"""

import unittest
from unittest.mock import patch, MagicMock

from WorkflowMgmt.GlobalLockController import GlobalLockController


class GlobalLockControllerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.globalLockController = GlobalLockController(acquire=False)
        super().setUp()
        return

    @patch("WorkflowMgmt.GlobalLockController.GlobalLockController.release")
    def tearDown(self, mockRelease: MagicMock) -> None:
        mockRelease.return_value = True
        del self.globalLockController
        super().tearDown()
        return

    def testIsLocked(self) -> None:
        """isLocked checks if an item is locked"""
        response = self.globalLockController.isLocked("test")
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

    def testGetItems(self) -> None:
        """getItems gets all items"""
        response = self.globalLockController.getItems()
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(response[0], str)
        self.assertTrue(isListOfStr)


if __name__ == "__main__":
    unittest.main()
