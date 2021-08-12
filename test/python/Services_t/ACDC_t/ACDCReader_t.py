#!/usr/bin/env python
"""
_ACDCReader_t_
Unit test for ACDCReader helper class.
"""

import unittest
from Services.ACDC.ACDCReader import ACDCReader


class ACDCReaderTest(unittest.TestCase):
    # For now only test if the request is working
    params = {"workflow": "pdmvserv_task_BPH-RunIIFall18GS-00350__v1_T_201021_154340_8354"}

    def setUp(self) -> None:
        self.acdcReader = ACDCReader()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetRecoveryDocs(self) -> None:
        """getRecoveryDocs gets the recovery docs for a given workflow"""
        docs = self.acdcReader.getRecoveryDocs(self.params.get("workflow"))
        isList = isinstance(docs, list)
        self.assertTrue(isList)


if __name__ == "__main__":
    unittest.main()
