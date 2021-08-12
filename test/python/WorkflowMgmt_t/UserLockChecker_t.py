#!/usr/bin/env python
"""
_UserLockChecker_t_
Unit test for UserLockChecker helper class.
"""

import unittest
from unittest.mock import patch, MagicMock

from WorkflowMgmt.UserLockChecker import UserLockChecker


class UserLockCheckerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.userLockChecker = UserLockChecker(component="test")
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    @patch("os.path.isfile")
    def testGo(self, mockIsFile: MagicMock) -> None:
        """go checks if a component is allowed to go"""
        # Test bahavior when lock file exists
        mockIsFile.return_value = True
        response = self.userLockChecker.go()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

        # Test behavior when lock file does not exist
        mockIsFile.side_effect = [False] * len(self.userLockChecker.lockers)
        response = self.userLockChecker.go()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

    @patch("os.path.isfile")
    def testIsLocked(self, mockIsFile: MagicMock) -> None:
        """go checks if a component is locked by user or not"""
        # Test bahavior when lock file exists
        mockIsFile.return_value = True
        response = self.userLockChecker.isLocked()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

        # Test behavior when lock file does not exist
        mockIsFile.side_effect = [False] * len(self.userLockChecker.lockers)
        response = self.userLockChecker.isLocked()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = response
        self.assertTrue(isFalse)

if __name__ == "__main__":
    unittest.main()
