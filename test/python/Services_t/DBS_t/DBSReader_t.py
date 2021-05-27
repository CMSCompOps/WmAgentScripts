#!/usr/bin/env python
"""
_DBSReader_t_
Unit test for the DBS helper class.
"""

import unittest

from Services.DBS.DBSReader import DBSReader

DATASET_INVALID = '/ggXToJPsiJPsi_JPsiToMuMu_M6p2_JPCZeroMinusPlus_TuneCP5_13TeV-pythia8-JHUGen/RunIIFall17pLHE-93X_mc2017_realistic_v3-v2/LHE'

class DBSReaderTest():
    def setUp(self):
        """
        _setUp_
        Initialize the API to point at the test server.
        """

        self.endpoint = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'
        self.dbs = None
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
        self.dbs = DBSReader(self.endpoint)
        self.assertEqual(self.dbs.getDBSStatus(DATASET_INVALID), 'INVALID')

if __name__ == '__main__':
    unittest.main()