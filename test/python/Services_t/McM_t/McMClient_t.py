#!/usr/bin/env python
"""
_McMClient_t_
Unit test for McMClient helper class.
"""

import unittest
from pycurl import Curl

from Services.McM.McMClient import McMClient


class McMClientTest(unittest.TestCase):
    params = {
        "wf": "cmsunified_task_TSG-Run3Winter21DRMiniAOD-00081__v1_T_210507_182332_1792",
        "dbName": "batches",
        "endpoint": "/restapi/requests/forcecomplete",
    }

    def setUp(self) -> None:
        self.mcmClient = McMClient(dev=True)
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testGetConnection(self) -> None:
        """_getConnection gets the connection to McM"""
        isCurl = isinstance(self.mcmClient.connection, Curl)
        self.assertTrue(isCurl)

    def testGet(self) -> None:
        """get gets the values of a given request"""
        response = self.mcmClient.get(self.params.get("endpoint"))
        isDict = isinstance(response, dict)
        self.assertTrue(isDict)

    def testSearch(self) -> None:
        """search gets the values of a given request"""
        response = self.mcmClient.search(self.params.get("dbName"), query=f"contains={self.params.get('wf')}")
        isList = isinstance(response, list)
        self.assertTrue(isList)

        isEmpty = len(response) == 0
        self.assertTrue(isEmpty)


if __name__ == "__main__":
    unittest.main()
