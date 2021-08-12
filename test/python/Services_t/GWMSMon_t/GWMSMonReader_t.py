#!/usr/bin/env python
"""
_GWMSMonReader_t_
Unit test for GWMSMonReader helper class.
"""

import unittest

from Services.GWMSMon.GWMSMonReader import GWMSMonReader


class GWMSMonReaderTest(unittest.TestCase):
    sitesParam = {"mcore": "T2_CH_CERN"}

    def setUp(self) -> None:
        self.gwmsmonReader = GWMSMonReader()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetMCoreReady(self) -> None:
        """getMCoreReady gets the mcore sites"""
        result = self.gwmsmonReader.getMCoreReady()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.sitesParam.get("mcore") in result
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
