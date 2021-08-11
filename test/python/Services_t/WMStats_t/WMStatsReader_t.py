#!/usr/bin/env python
"""
_WMStatsReader_t_
Unit test for WMStatsReader helper class.
"""

import unittest

from Services.WMStats.WMStatsReader import WMStatsReader


class WMStatsReaderTest(unittest.TestCase):
    params = {
        "team": "production",
        "agent_url": "vocms0283.cern.ch",
    }

    def setUp(self) -> None:
        self.wmstatsReader = WMStatsReader()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetAgents(self) -> None:
        """getAgents get all agents by team"""
        result = self.wmstatsReader.getAgents()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueList = all(isinstance(v, list) for v in result.values())
        self.assertTrue(isValueList)

        isFound = False
        for k, v in result.items():
            if k == self.params.get("team"):
                isFound = True
                break
        self.assertTrue(isFound)

    def testGetProductionAgents(self) -> None:
        """getProductionAgents gets all agents in the production team"""
        result = self.wmstatsReader.getProductionAgents()
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isKeyStr = all(isinstance(k, str) for k in result)
        self.assertTrue(isKeyStr)

        isValueDict = all(isinstance(v, dict) for v in result.values())
        self.assertTrue(isValueDict)

        isFound = self.params.get("agent_url") in result
        self.assertTrue(isFound)

if __name__ == "__main__":
    unittest.main()
