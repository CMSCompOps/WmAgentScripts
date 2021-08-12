#!/usr/bin/env python
"""
_EOSReader_t_
Unit test for EOSReader helper class.
"""

import json
import unittest
from unittest.mock import patch, mock_open, MagicMock

from Services.EOS.EOSReader import EOSReader


class EOSReaderTest(unittest.TestCase):
    params = {
        "validFilename": "/eos/user/test/test.txt",
        "invalidFilename": "invalidTest.txt",
        "mockContent": {"test": "file"},
    }

    def setUp(self) -> None:
        self.eosReader = EOSReader(self.params.get("validFilename"))
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testFilename(self) -> None:
        """filename must be a valid eos path"""
        # Test when filename is valid
        isValid = self.eosReader.filename == self.params.get("validFilename")
        self.assertTrue(isValid)

        # Test when filename is not valid
        with self.assertRaises(Exception):
            self.eosReader.filename = self.params.get("invalidFilename")
            _ = EOSReader(self.params.get("invalidFilename"))

    @patch("builtins.open", mock_open(read_data=json.dumps(params.get("mockContent"))))
    def testReadWithFile(self) -> None:
        """read gets the content of an EOS file"""
        # Test behavior when file is found locally
        result = self.eosReader.read()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEqual = result == self.params.get("mockContent")
        self.assertTrue(isEqual)

    @patch("os.system")
    @patch("builtins.open", mock_open(read_data=None))
    def testReadWithoutFile(self, mockSystem: MagicMock) -> None:
        """read gets the content of an EOS file"""
        # Test behavior when file is not found at all
        mockSystem.return_value = 99
        result = self.eosReader.read()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
