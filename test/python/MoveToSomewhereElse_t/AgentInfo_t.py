#!/usr/bin/env python
"""
_AgentInfo_t_
Unit test for AgentInfo helper class.
"""

import unittest
from unittest.mock import patch
from pymongo.collection import Collection

from MoveToSomewhereElse.AgentInfo import AgentInfo


class AgentInfoTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "agentInfo"}

    params = {"agent": "vocms0284.cern.ch", "docKeys": ["status", "version", "update", "date"]}

    @patch("MoveToSomewhereElse.AgentInfo.AgentInfo.syncToProduction")
    def setUp(self, mockSync) -> None:
        mockSync.return_value = True
        self.agentInfo = AgentInfo()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.agentInfo.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.agentInfo.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.agentInfo.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGetAgents(self):
        """get gets the agents names"""
        result = self.agentInfo.getAgents()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.params.get("agent") in result
        self.assertTrue(isFound)

    def testGet(self):
        """get gets the info of a given agent"""
        # Test when agent exists
        result = self.agentInfo.get(self.params.get("agent"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        hasAllKeys = all(k in result for k in self.params.get("docKeys"))
        self.assertTrue(hasAllKeys)

        isFound = result["name"] == self.params.get("agent")
        self.assertTrue(isFound)

        # Test when agent does not exist
        result = self.agentInfo.get("test")
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
