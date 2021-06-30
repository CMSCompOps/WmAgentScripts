#!/usr/bin/env python
"""
_DBSReader_t_
Unit test for the DBS helper class.
"""

import unittest
from Services.DBS.DBSReader import DBSReader


class DBSReaderTest(unittest.TestCase):
    # There are many more blocks and files under these datasets
    # For now, test just one of each one
    invalidDataset = {
        "dataset": "/ggXToJPsiJPsi_JPsiToMuMu_M6p2_JPCZeroMinusPlus_TuneCP5_13TeV-pythia8-JHUGen/RunIIFall17pLHE-93X_mc2017_realistic_v3-v2/LHE",
        "status": "INVALID",
        "logical_file_name": "/store/data/RunIIFall17pLHE/ggXToJPsiJPsi_JPsiToMuMu_M6p2_JPCZeroMinusPlus_TuneCP5_13TeV-pythia8-JHUGen/LHE/93X_mc2017_realistic_v3-v2/00000/F63EBE9E-297A-EB11-A3D1-FA163E30235E.root",
    }

    validDataset = {
        "dataset": "/TT_Mtt-1000toInf_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM",
        "status": "VALID",
        "run": 1,
        "block_name": "/TT_Mtt-1000toInf_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM#f3b36c1a-9618-4b58-b4ed-d2d4ff5a281a",
        "logical_file_name": "/store/mc/RunIIFall17MiniAODv2/TT_Mtt-1000toInf_TuneCP5_PSweights_13TeV-powheg-pythia8/MINIAODSIM/PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/230000/F29916D2-2910-EB11-91DC-FEC1FD6C28DA.root",
        "logical_file_name_base": "/store/mc",
    }

    def setUp(self):
        """
        _setUp_
        Initialize the API to point at the test server.
        """

        self.url = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
        super(DBSReaderTest, self).setUp()
        return

    def tearDown(self):
        """
        _tearDown_
        """
        super(DBSReaderTest, self).tearDown()
        return

    def testGetDBSStatus(self):
        """getDBSStatus gets DBS Status of a dataset"""
        dbsReader = DBSReader(self.url)
        status = dbsReader.getDBSStatus(self.invalidDataset.get("dataset"))
        isStr = isinstance(status, str)
        self.assertTrue(isStr)

        isFound = status == self.invalidDataset.get("status")
        self.assertTrue(isFound)

    def testGetFilesWithLumiInRun(self):
        """getFilesWithLumiInRun gets DBS files with lumi of a dataset and run"""
        dbsReader = DBSReader(self.url)
        files = dbsReader.getFilesWithLumiInRun(
            self.validDataset.get("dataset"), self.validDataset.get("run")
        )
        isList = isinstance(files, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(files[0], dict)
        self.assertTrue(isListOfDicts)

        isFound = False
        for file in files:
            if self.validDataset.get("logical_file_name") == file["logical_file_name"]:
                isFound = True
                break
        self.assertTrue(isFound)

    def testgetBlockName(self):
        """getBlockName gets the block name of a file"""
        dbsReader = DBSReader(self.url)
        block = dbsReader.getBlockName(self.validDataset.get("logical_file_name"))
        isStr = isinstance(block, str)
        self.assertTrue(isStr)

        isFound = block == self.validDataset.get("block_name")
        self.assertTrue(isFound)

    def testGetDatasetFiles(self):
        """getDatasetFiles gets files of a dataset"""
        # Test when details is False and validFileOnly is False
        dbsReader = DBSReader(self.url)
        files = dbsReader.getDatasetFiles(self.invalidDataset.get("dataset"))
        isList = isinstance(files, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(files[0], dict)
        self.assertTrue(isListOfDicts)

        noDetails = False
        for file in files:
            if any(k not in ["logical_file_name", "is_file_valid"] for k in file):
                break
        else:
            noDetails = True
        self.assertTrue(noDetails)

        isFound = False
        for file in files:
            if (
                self.invalidDataset.get("logical_file_name")
                == file["logical_file_name"]
            ):
                isFound = True
                break
        self.assertTrue(isFound)

        # Test when details is False and validFileOnly is True
        files = dbsReader.getDatasetFiles(
            self.invalidDataset.get("dataset"), validFileOnly=True
        )
        isList = isinstance(files, list)
        self.assertTrue(isList)

        isEmpty = len(files) == 0
        self.assertTrue(isEmpty)

    def testGetDatasetBlockNamesByRuns(self):
        """getDatasetBlockNamesByRuns gets the blocks names for a dataset filtered by runs"""
        dbsReader = DBSReader(self.url)
        blocks = dbsReader.getDatasetBlockNamesByRuns(
            self.validDataset.get("dataset"), [self.validDataset.get("run")]
        )
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(blocks[0], str)
        self.assertTrue(isListOfStr)

        isFound = False
        for block in blocks:
            if block == self.validDataset.get("block_name"):
                isFound = True
                break
        self.assertTrue(isFound)

    def testGetDatasetBlockNames(self):
        """getDatasetBlockNames gets the blocks names for a dataset"""
        dbsReader = DBSReader(self.url)
        blocks = dbsReader.getDatasetBlockNames(self.validDataset.get("dataset"))
        isList = isinstance(blocks, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(blocks[0], str)
        self.assertTrue(isListOfStr)

        isFound = False
        for block in blocks:
            if block == self.validDataset.get("block_name"):
                isFound = True
                break
        self.assertTrue(isFound)

    def testGetDatasetSize(self):
        """getDatasetSize gets the size of a dataset"""
        dbsReader = DBSReader(self.url)
        size = dbsReader.getDatasetSize(self.validDataset.get("dataset"))
        isFloat = isinstance(size, float)
        self.assertTrue(isFloat)

    def testGetDatasetEventsAndLumis(self):
        """getDatasetEventsAndLumis gets number of events and lumis of a dataset"""
        dbsReader = DBSReader(self.url)
        events, lumis = dbsReader.getDatasetEventsAndLumis(
            self.validDataset.get("dataset")
        )
        for i in [events, lumis]:
            isInt = isinstance(i, int)
            self.assertTrue(isInt)

    def testGetBlocksEventsAndLumis(self):
        """getBlocksEventsAndLumis gets number of events and lumis of blocks"""
        dbsReader = DBSReader(self.url)
        events, lumis = dbsReader.getBlocksEventsAndLumis(
            [self.validDataset.get("block_name")]
        )
        for i in [events, lumis]:
            isInt = isinstance(i, int)
            self.assertTrue(isInt)

    def testGetDatasetRuns(self):
        """getDatasetRuns gets the runs of a dataset"""
        dbsReader = DBSReader(self.url)
        runs = dbsReader.getDatasetRuns(self.validDataset.get("dataset"))
        isList = isinstance(runs, list)
        self.assertTrue(isList)

        isListOfInts = isinstance(runs[0], int)
        self.assertTrue(isListOfInts)

        isFound = runs[0] == self.validDataset.get("run")
        self.assertTrue(isFound)

    def testGetDatasetParent(self):
        """getDatasetParent gets the parents of a dataset"""
        dbsReader = DBSReader(self.url)
        parents = dbsReader.getDatasetRuns(self.validDataset.get("dataset"))
        isList = isinstance(parents, list)
        self.assertTrue(isList)

        isEmpty = len(parents) == 0
        self.assertTrue(isEmpty)

    def testGetDatasetNames(self):
        """getDatasetNames gets the name of a dataset"""
        dbsReader = DBSReader(self.url)
        names = dbsReader.getDatasetNames(self.validDataset.get("dataset"))
        isList = isinstance(names, list)
        self.assertTrue(isList)

        isListOfDicts = isinstance(names[0], dict)
        self.assertTrue(isListOfDicts)

        isFound = False
        for name in names:
            if name["dataset"] == self.validDataset.get("dataset"):
                isFound = True
                break
        self.assertTrue(isFound)

    def testGetLFNBase(self):
        """getLFNBase gets the base of the filenames of a dataset"""
        dbsReader = DBSReader(self.url)
        name = dbsReader.getLFNBase(self.validDataset.get("dataset"))
        isStr = isinstance(name, str)
        self.assertTrue(isStr)

        isFound = name == self.validDataset.get("logical_file_name_base")
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
