#!/usr/bin/env python
"""
_UserLockController_t_
Unit test for UserLockController helper class.
"""

import unittest
from unittest.mock import patch

from Components.Module.UserLockController import UserLockController

class UserLockControllerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.userLockController = UserLockController(component="test")
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
        response = self.userLockController.go()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isFalse = not response
        self.assertTrue(isFalse)

        
        # Test behavior when lock file does not exist
        mockIsFile.side_effect = [False] * len(self.userLockController.lockers)
        response = self.userLockController.go()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

if __name__ == "__main__":
    unittest.main()

