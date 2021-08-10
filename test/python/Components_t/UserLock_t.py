#!/usr/bin/env python
"""
_UserLock_t_
Unit test for UserLock helper class.
"""

import unittest
from unittest.mock import patch

from Components.UserLock import UserLock


class UserLockTest(unittest.TestCase):
    def setUp(self) -> None:
        self.userLock = UserLock(component="test")
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    @patch("os.path.isfile")
    def testGo(self, mockIsFile) -> None:
        """go checks if a component is blocked by a user or not"""
        # Test bahavior when lock file exists
        mockIsFile.return_value = True
        response = self.userLock.go()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

        # Test behavior when lock file does not exist
        mockIsFile.side_effect = [False] * len(self.userLock.lockers)
        response = self.userLock.go()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)


if __name__ == "__main__":
    unittest.main()
