#!/usr/bin/env python
"""
_CampaignInfo_t_
Unit test for CampaignInfo helper class.
"""

import unittest
from pymongo.collection import Collection

from MoveToSomewhereElse.CampaignInfo import CampaignInfo


class CampaignInfoTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "campaignsConfiguration"}

    # Some campaign params for testing
    campaignParam = {
        "campaign": "Run3Winter21DRMiniAOD",
        "resize": "auto",
        "secondaries": "/MinBias_TuneCP5_14TeV-pythia8/Run3Winter21GS-112X_mcRun3_2021_realistic_v15-v1/GEN-SIM",
    }
    campaignWithTypeParam = {
        "campaign": "CMSSW_12_0_0_pre4__PDMVRELVALS-109_CosmicRun-1626011964",
        "type": "relval",
        "parameters": {
            "SiteWhitelist": ["T1_US_FNAL"],
            "MergedLFNBase": "/store/relval",
            "NonCustodialGroup": "RelVal",
            "Team": "relval",
        },
    }

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        campaignInfo = CampaignInfo()
        isCollection = isinstance(campaignInfo.collection, Collection)
        self.assertTrue(isCollection)

        rightName = campaignInfo.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = campaignInfo.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGet(self):
        """get gets the campaigns info"""
        campaignInfo = CampaignInfo()
        result = campaignInfo.get()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isDictOfDict = all(isinstance(v, dict) for v in result.values())
        self.assertTrue(isDictOfDict)

        isFound = False
        for k in result:
            if k == self.campaignParam.get("campaign"):
                isFound = True
                break
        self.assertTrue(isFound)

    def testGetCampaigns(self):
        """getCampaigns gets the campaigns names"""
        # Test when campaign type is empty
        campaignInfo = CampaignInfo()
        result = campaignInfo.getCampaigns()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.campaignParam.get("campaign") in result
        self.assertTrue(isFound)

        # Test when campaign type is given
        result = campaignInfo.getCampaigns(self.campaignWithTypeParam.get("type"))
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.campaignWithTypeParam.get("campaign") in result
        self.assertTrue(isFound)

    def testGetCampaignValue(self):
        """getCampaignValue gets the value of a given campaign"""
        # Test when the key exists, will use resize in this case
        campaignInfo = CampaignInfo()
        result = campaignInfo.getCampaignValue(self.campaignParam.get("campaign"), "resize", "default")
        isStr = isinstance(result, str)
        self.assertTrue(isStr)

        isFound = result == self.campaignParam.get("resize")
        self.assertTrue(isFound)

        # Test when key does not exists
        campaignInfo = CampaignInfo()
        result = campaignInfo.getCampaignValue(self.campaignParam.get("campaign"), "invalidKey", "default")
        isStr = isinstance(result, str)
        self.assertTrue(isStr)

        isDefault = result == "default"
        self.assertTrue(isDefault)

    def testGetCampaignParameters(self):
        """getCampaignParameters gets the parameters of a given campaign"""
        campaignInfo = CampaignInfo()
        result = campaignInfo.getCampaignParameters(self.campaignWithTypeParam.get("campaign"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isFound = result == self.campaignWithTypeParam.get("parameters")
        self.assertTrue(isFound)

    def testGetSecondaries(self):
        """getSecondaries gets the campaigns secondaries"""
        campaignInfo = CampaignInfo()
        result = campaignInfo.getSecondaries()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.campaignParam.get("secondaries") in result
        self.assertTrue(isFound)


if __name__ == "__main__":
    unittest.main()
