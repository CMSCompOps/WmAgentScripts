#!/usr/bin/env python
"""
_AgentStatusController_t_
Unit test for AgentStatusController helper class.
"""

import unittest
from unittest.mock import patch
from pymongo.collection import Collection

from Components.Agent.AgentStatusController import AgentStatusController


class AgentStatusControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "agentInfo"}

    # AgentInfo is always changing.
    # For now, only test output types and content keys for a given agent.
    params = {"agent": "vocms0284.cern.ch", "docKeys": ["status", "version", "update", "date"]}

    @patch("Components.Agent.AgentStatusController.AgentStatusController.syncToProduction")
    @patch("Services.Trello.TrelloClient.TrelloClient.__init__")
    def setUp(self, mockTrello, mockSync) -> None:
        mockSync.return_value = True
        mockTrello.return_value = None
        self.agentStatusController = AgentStatusController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.agentStatusController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.agentStatusController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.agentStatusController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGetAgents(self):
        """get gets the agents names"""
        result = self.agentStatusController.getAgents()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.params.get("agent") in result
        self.assertTrue(isFound)

    def testGet(self):
        """get gets the info of a given agent"""
        # Test when agent exists
        result = self.agentStatusController.get(self.params.get("agent"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        hasAllKeys = all(k in result for k in self.params.get("docKeys"))
        self.assertTrue(hasAllKeys)

        isFound = result["name"] == self.params.get("agent")
        self.assertTrue(isFound)

        # Test when agent does not exist
        result = self.agentStatusController.get("test")
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
