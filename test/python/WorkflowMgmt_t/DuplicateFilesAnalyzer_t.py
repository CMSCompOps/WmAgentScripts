#!/usr/bin/env python
"""
_DuplicateFilesAnalyzer_t_
Unit test for DuplicateFilesAnalyzer helper class.
"""

import unittest

from WorkflowMgmt.DuplicateFilesAnalyzer import DuplicateFilesAnalyzer


class FilesAnalyzerTest(unittest.TestCase):
    # Dataset parameters to use for testing
    datasetParams = {
        "dataset": "/MET/Run2018A-12Nov2019_UL2018-v3/MINIAOD",
        "filesPerLumis": {
            (315257, 63): [
                "/store/data/Run2018A/MET/MINIAOD/12Nov2019_UL2018-v3/230000/49FA5771-4B0A-494C-BDAB-39B0C0FD4D6E.root"
            ],
            (315257, 44): [
                "/store/data/Run2018A/MET/MINIAOD/12Nov2019_UL2018-v3/230000/49FA5771-4B0A-494C-BDAB-39B0C0FD4D6E.root"
            ],
            (315257, 33): [
                "/store/data/Run2018A/MET/MINIAOD/12Nov2019_UL2018-v3/230000/49FA5771-4B0A-494C-BDAB-39B0C0FD4D6E.root"
            ],
            (315257, 84): [
                "/store/data/Run2018A/MET/MINIAOD/12Nov2019_UL2018-v3/230000/49FA5771-4B0A-494C-BDAB-39B0C0FD4D6E.root"
            ],
            (315257, 22): [
                "/store/data/Run2018A/MET/MINIAOD/12Nov2019_UL2018-v3/230000/49FA5771-4B0A-494C-BDAB-39B0C0FD4D6E.root"
            ],
            (315257, 73): [
                "/store/data/Run2018A/MET/MINIAOD/12Nov2019_UL2018-v3/230000/49FA5771-4B0A-494C-BDAB-39B0C0FD4D6E.root"
            ],
            (315257, 11): [
                "/store/data/Run2018A/MET/MINIAOD/12Nov2019_UL2018-v3/230000/49FA5771-4B0A-494C-BDAB-39B0C0FD4D6E.root"
            ],
            (315257, 58): [
                "/store/data/Run2018A/MET/MINIAOD/12Nov2019_UL2018-v3/230000/49FA5771-4B0A-494C-BDAB-39B0C0FD4D6E.root"
            ],
        },
    }

    def setUp(self) -> None:
        self.analyzer = DuplicateFilesAnalyzer()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetFilesWithDuplicateLumis(self) -> None:
        """getFilesWithDuplicateLumis gets a list of files with duplicate lumis"""
        result = self.analyzer.getFilesWithDuplicateLumis(self.datasetParams.get("filesPerLumis"))
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
