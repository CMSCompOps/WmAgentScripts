#!/usr/bin/env python
"""
_EOSReader_t_
Unit test for EOSReader helper class.
"""

import os
import json
import unittest
from unittest.mock import patch, mock_open

from Storage.EOS.EOSReader import EOSReader


class EOSReaderTest(unittest.TestCase):
    params = {
        "validFile": "/eos/user/test/test.txt",
        "content": {"test": "file"},
        "invalidFile": "invalidTest.txt",
    }

    def setUp(self) -> None:
        self.eosReader = EOSReader(self.params.get("validFile"))
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testInit(self):
        """__init__ initilizes EOSReader if filename is valid"""
        # Test when filename is not valid
        with self.assertRaises(Exception):
            _ = EOSReader(self.params.get("invalidFile"))

        # Test when filename is valid
        isInitialized = self.eosReader.filename == self.params.get("validFile")
        self.assertTrue(isInitialized)

    @patch("os.system")
    @patch("builtins.open", create=True)
    def testRead(self, mockOpen, mockSystem):
        """read gets the content of an EOS file"""
        # Test behavior when file is found locally
        mockOpen.return_value = mock_open(read_data=json.dumps(self.params.get("content"))).return_value
        result = self.eosReader.read()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEqual = result == self.params.get("content")
        self.assertTrue(isEqual)

        # Test behavior when file is not found at all
        mockOpen.return_value = mock_open(read_data=None).return_value
        mockSystem.return_value = 99
        result = self.eosReader.read()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
