"""
_ServicesChecker_t_
Unit test for ServicesChecker helper class.
"""

import unittest
from unittest.mock import patch, MagicMock

from Services.ServicesChecker import ServicesChecker


class ServicesCheckerTess(unittest.TestCase):
    @patch("MongoControllers.AgentController.AgentController.syncToProduction")
    @patch("Services.Trello.TrelloClient.TrelloClient.__init__")
    def setUp(self, mockTrello: MagicMock, mockSync: MagicMock) -> None:
        mockSync.return_value = True
        mockTrello.return_value = None
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

    def testCheckMongo(self) -> None:
        """checkMongo checks if the service is working properly"""
        response = self.servicesChecker.checkMongo()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

    @patch("Services.ServicesChecker.ServicesChecker.checkEOS")
    def testCheck(self, mockEOS: MagicMock) -> None:
        """check checks if the services are working properly"""
        mockEOS.return_value = True
        response = self.servicesChecker.check()
        isBool = isinstance(response, bool)
        self.assertTrue(isBool)

        isTrue = response
        self.assertTrue(isTrue)

if __name__ == "__main__":
    unittest.main()
