#!/usr/bin/env python
"""
_CRICReader_t_
Unit test for CRICReader helper class.
"""

import unittest

from Services.CRIC.CRICReader import CRICReader


class CRICReaderTest(unittest.TestCase):
    siteStorageParam = ['T2_CH_CERNBOX', 'T2_CH_CERN']

    def setUp(self) -> None:
        self.cricReader = CRICReader()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetSiteStorage(self) -> None:
        """getSiteStorage gets the site-storage pairs"""
        result = self.cricReader.getSiteStorage()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfList = isinstance(result[0], list)
        self.assertTrue(isListOfList)

        isFound = self.siteStorageParam in result
        self.assertTrue(isFound)

if __name__ == "__main__":
    unittest.main()