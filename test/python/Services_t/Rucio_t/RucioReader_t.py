#!/usr/bin/env python
"""
_RucioReader_t_
Unit test for RucioReader helper class.
"""

import os
import unittest

from Services.Rucio.RucioReader import RucioReader


class RucioReaderTest(unittest.TestCase):
    datasetParams = {
        "dataset": "/TT_Mtt-1000toInf_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM",
        "file": "/store/mc/RunIIFall17MiniAODv2/TT_Mtt-1000toInf_TuneCP5_PSweights_13TeV-powheg-pythia8/MINIAODSIM/PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/230000/D8AEFF5C-0728-EB11-AA43-0CC47A5FBDC1.root",
        "nFilesInDataset": 522,
        "block": "/TT_Mtt-1000toInf_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM#01f05879-f811-45c8-b3c1-b22705fd93a5",
        "nBlocksInDataset": 143,
        "nFilesInBlock": 1,
        "account": "transfer_ops",
    }

    def setUp(self) -> None:
        self.rucio = RucioReader(account=os.getenv("RUCIO_ACCOUNT"))
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testCountDatasetFiles(self) -> None:
        """countDatasetFiles gets the number of files for a given dataset"""
        response = self.rucio.countDatasetFiles(self.datasetParams.get("dataset"))
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.datasetParams.get("nFilesInDataset")
        self.assertTrue(isEqual)

    def testCountDatasetFilesPerBlock(self) -> None:
        """countDatasetFilesPerBlock gets the number of files per block for a given dataset"""
        response = self.rucio.countDatasetFilesPerBlock(self.datasetParams.get("dataset"))
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isListOfTuple = isinstance(response[0], tuple)
        self.assertTrue(isListOfTuple)

        sameLen = len(response) == self.datasetParams.get("nBlocksInDataset")
        self.assertTrue(sameLen)

        isFound = False
        for block, files in response:
            if block == self.datasetParams.get("block"):
                isFound = files == self.datasetParams.get("nFilesInBlock")
                break
        self.assertTrue(isFound)

    def testCountBlockFiles(self) -> None:
        """countBlockFiles gets the number of files for a given block"""
        response = self.rucio.countBlockFiles(self.datasetParams.get("block"))
        isInt = isinstance(response, int)
        self.assertTrue(isInt)

        isEqual = response == self.datasetParams.get("nFilesInBlock")
        self.assertTrue(isEqual)

    def testGetDatasetFileNames(self) -> None:
        """getDatasetFileNames gets the file names for a given dataset"""
        files = self.rucio.getDatasetFileNames(self.datasetParams.get("dataset"))
        isList = isinstance(files, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(files[0], str)
        self.assertTrue(isListOfStr)

        sameLen = len(files) == self.datasetParams.get("nFilesInDataset")
        self.assertTrue(sameLen)

        isFound = self.datasetParams.get("file") in files
        self.assertTrue(isFound)

    def testGetDatasetBlockNames(self) -> None:
        """getDatasetBlockNames gets the block names for a given dataset"""
        blocks = self.rucio.getDatasetBlockNames(self.datasetParams.get("dataset"))
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(blocks[0], str)
        self.assertTrue(isListOfStr)

        sameLen = len(blocks) == self.datasetParams.get("nBlocksInDataset")
        self.assertTrue(sameLen)

        isFound = self.datasetParams.get("block") in blocks
        self.assertTrue(isFound)

    def testGetDatasetLocationsByAccount(self) -> None:
        """getDatasetLocationsByAccount gets the dataset location of an account"""
        response = self.rucio.getDatasetLocationsByAccount(
            self.datasetParams.get("dataset"), self.datasetParams.get("account")
        )
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isEmpty = len(response) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
