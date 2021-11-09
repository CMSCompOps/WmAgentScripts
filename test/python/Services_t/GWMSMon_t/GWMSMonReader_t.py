#!/usr/bin/env python
"""
_GWMSMonReader_t_
Unit test for GWMSMonReader helper class.
"""

import unittest

from Services.GWMSMon.GWMSMonReader import GWMSMonReader


class GWMSMonReaderTest(unittest.TestCase):
    sitesParam = {"key": "sites_for_mcore", "mcore": "T2_CH_CERN"}

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
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        hasKey = self.sitesParam.get("key") in result
        self.assertTrue(hasKey)

        isValueList = isinstance(result.get(self.sitesParam.get("key")), list)
        self.assertTrue(isValueList)

        isListOfStr = isinstance(result.get(self.sitesParam.get("key"))[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.sitesParam.get("mcore") in result.get(self.sitesParam.get("key"))
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
