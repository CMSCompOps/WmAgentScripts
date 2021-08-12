#!/usr/bin/env python
"""
_AgentController_t_
Unit test for AgentController helper class.
"""

import unittest
from unittest.mock import patch, MagicMock
from pymongo.collection import Collection
from time import struct_time, mktime, asctime

from MongoControllers.AgentController import AgentController


class AgentControllerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "agentInfo"}

    # AgentInfo is always changing.
    # For now, only test output types and content keys for a given agent.
    params = {
        "agent": "vocms0284.cern.ch",
        "docKeys": ["status", "version", "update", "date"],
        "now": struct_time((2021, 1, 1, 0, 0, 0, 0, 0, 0)),
    }

    @patch("MongoControllers.AgentController.AgentController.syncToProduction")
    @patch("Services.Trello.TrelloClient.TrelloClient.__init__")
    def setUp(self, mockTrello: MagicMock, mockSync: MagicMock) -> None:
        mockSync.return_value = True
        mockTrello.return_value = None
        self.agentController = AgentController()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self) -> None:
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.agentController.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.agentController.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.agentController.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testBuildMongoDocument(self) -> None:
        """_buildMongoDocument builds the document to store on Mongo"""
        result = self.agentController._buildMongoDocument(
            self.params.get("agent"), {"test": "ok"}, now=self.params.get("now")
        )
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isNameEqual = result.get("name") == self.params.get("agent")
        self.assertTrue(isNameEqual)

        isUpdateEqual = result.get("update") == mktime(self.params.get("now"))
        self.assertTrue(isUpdateEqual)

        isDateEqual = result.get("date") == asctime(self.params.get("now"))
        self.assertTrue(isDateEqual)

        isFound = result.get("test") == "ok"
        self.assertTrue(isFound)

    def testGetAgents(self) -> None:
        """get gets the agents names"""
        # Test without query params
        result = self.agentController.getAgents()
        isList = isinstance(result, list)
        self.assertTrue(isList)

        isListOfStr = isinstance(result[0], str)
        self.assertTrue(isListOfStr)

        isFound = self.params.get("agent") in result
        self.assertTrue(isFound)

        # Test with query params
        result = self.agentController.getAgents(speeddrain=True)
        isList = isinstance(result, list)
        self.assertTrue(isList)

    def testGet(self) -> None:
        """get gets the info of a given agent"""
        # Test when agent exists
        result = self.agentController.get(self.params.get("agent"))
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        hasAllKeys = all(k in result for k in self.params.get("docKeys"))
        self.assertTrue(hasAllKeys)

        isFound = result["name"] == self.params.get("agent")
        self.assertTrue(isFound)

        # Test when agent does not exist
        result = self.agentController.get("test")
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        isEmpty = len(result) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
