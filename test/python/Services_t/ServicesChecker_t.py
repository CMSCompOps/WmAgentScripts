"""
_ServicesChecker_t_
Unit test for ServicesChecker helper class.
"""

import unittest
from unittest.mock import patch, MagicMock

from Services.ServicesChecker import ServicesChecker


class ServicesCheckerTess(unittest.TestCase):
    def setUp(self) -> None:
        self.servicesChecker = ServicesChecker()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testCheckOracle(self) -> None:
        """checkOracle checks if the service is working properly"""
        response = self.servicesChecker.checkOracle()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

    def testCheckReqMgr(self) -> None:
        """checkReqMgr checks if the service is working properly"""
        response = self.servicesChecker.checkReqMgr()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

    def testCheckDBS(self) -> None:
        """checkDBS checks if the service is working properly"""
        response = self.servicesChecker.checkDBS()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

    @patch("MongoControllers.AgentController.AgentController.syncToProduction")
    @patch("Services.Trello.TrelloClient.TrelloClient.__init__")
    def testCheckMongo(self, mockTrello: MagicMock, mockSync: MagicMock) -> None:
        """checkMongo checks if the service is working properly"""
        mockSync.return_value = True
        mockTrello.return_value = None
        response = self.servicesChecker.checkMongo()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

    @patch("MongoControllers.AgentController.AgentController.syncToProduction")
    @patch("Services.Trello.TrelloClient.TrelloClient.__init__")
    @patch("Services.ServicesChecker.ServicesChecker.checkEOS")
    def testCheck(self, mockEOS: MagicMock, mockTrello: MagicMock, mockSync: MagicMock) -> None:
        """check checks if the services are working properly"""
        mockSync.return_value = True
        mockTrello.return_value = None
        mockEOS.return_value = True
        response = self.servicesChecker.check()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)


if __name__ == "__main__":
    unittest.main()
