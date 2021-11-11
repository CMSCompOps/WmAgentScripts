import os
import socket
import json
from logging import Logger
from time import sleep

from Databases.Oracle.OracleClient import OracleClient
from Databases.Oracle.OracleDB import Workflow
from MongoControllers.AgentController import AgentController
from Services.EOS.EOSWriter import EOSWriter
from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Services.DBS.DBSReader import DBSReader
from Utilities.Decorators import runWithMultiThreading
from Utilities.Logging import getLogger
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional, List


class ServicesChecker(object):
    """
    _ServicesChecker_
    General API for checking the services
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.block = kwargs.get("block") or True
            self.keepTrying = kwargs.get("keepTrying") or False
            self.softServices = kwargs.get("soft") or ["mcm", "wtc", "mongo", "jira"]

            self.code = 0
            self.go = False
            self.checking = None
            self.host = socket.gethostname()

            self.status = {
                "ReqMgr": False,
                "McM": False,
                "DBS": False,
                "Oracle": False,
                "WTC": False,
                "EOS": False,
                "Mongo": False,
                "Jira": False,
            }

        except Exception as error:
            raise Exception(f"Error initializing ServicesChecker\n{str(error)}")

    def checkOracle(self) -> bool:
        """
        The function to check Oracle
        :return: True if ok, False o/w
        """
        try:
            oracleClient = OracleClient()
            _ = oracleClient.session.query(Workflow).filter(Workflow.name.contains("1")).all()
            return True

        except Exception as error:
            self.logger.error("Failed to check Oracle")
            self.logger.error(str(error))
            return False

    def checkReqMgr(self) -> bool:
        """
        The function to check ReqMgr
        :return: True if ok, False o/w
        """
        try:
            reqmgrReader = ReqMgrReader()
            _ = reqmgrReader.getReqmgrInfo()
            return True

        except Exception as error:
            self.logger.error("Failed to check ReqMgr")
            self.logger.error(str(error))
            return False

    def checkMcM(self) -> bool:
        """
        The function to check McM
        :return: True if ok, False o/w
        """
        try:
            # TODO
            return True

        except Exception as error:
            self.logger.error("Failed to check McM")
            self.logger.error(str(error))
            return False

    def checkDBS(self) -> bool:
        """
        The function to check DBS
        :return: True if ok, False o/w
        """
        try:
            dbsReader = DBSReader()
            return dbsReader.check()

        except Exception as error:
            self.logger.error("Failed to check DBS")
            self.logger.error(str(error))
            return False

    def checkWTC(self) -> bool:
        """
        The function to check WTC
        :return: True if ok, False o/w
        """
        try:
            # TODO
            return True

        except Exception as error:
            self.logger.error("Failed to check WTC")
            self.logger.error(str(error))
            return False

    def checkEOS(self) -> bool:
        """
        The function to check EOS
        :return: True if ok, False o/w
        """
        try:
            configurationHandler = ConfigurationHandler()
            eosDirectory = configurationHandler.get("base_eos_dir")
            eosFile = f"{eosDirectory}/{os.getpid()}-testfile"

            eosWriter = EOSWriter(eosFile)
            eosWriter.write("Testing I/O on eos")
            saved = eosWriter.save()

            if saved:
                response = os.system(f"env EOS_MGM_URL=root://eoscms.cern.ch eos rm {eosFile}")
                if response != 0:
                    raise Exception("Failed to I/O on eos")

            return True

        except Exception as error:
            self.logger.error("Failed to check EOS")
            self.logger.error(str(error))
            return False

    def checkMongo(self) -> bool:
        """
        The function to check Mongo
        :return: True if ok, False o/w
        """
        try:
            agentController = AgentController()
            _ = agentController.getAgents()
            return True

        except Exception as error:
            self.logger.error("Failed to check Mongo")
            self.logger.error(str(error))
            return False

    def checkJira(self) -> bool:
        """
        The function to check Jira
        :return: True if ok, False o/w
        """
        try:
            # TODO
            return True

        except Exception as error:
            self.logger.error("Failed to check Jira")
            self.logger.error(str(error))
            return False

    @runWithMultiThreading(mtParam="services", timeout=120, wait=10)
    def _checkService(self, services: List[str]) -> bool:
        """
        The function to check a given service
        :param services: services name
        :return: True if ok, False o/w
        """
        try:
            self.logger.info("Checking on %s", services)
            self.checking = services

            isOk = getattr(self, f"check{services}")()
            if isOk:
                self.status[services] = True
                self.logger.info("%s is reachable", services)
                return True

            self.logger.critical("The service %s is unreachable from %s", services, self.host)

            if self.keepTrying:
                self.logger.info("Re-checking on %s", services)
                sleep(30)
                return self._checkService(services=[services])
            if self.block and services.lower() not in self.softServices:
                return False

            return True

        except Exception as error:
            self.logger.error("Failed to check component")
            self.logger.error(str(error))
            return False

    def check(self) -> bool:
        """
        The function to check all services
        :return: True if ok, False o/w
        """
        try:
            checks = self._checkService(services=sorted(self.status))
            if not all(checks):
                self.code = 120 + sum(checks)
                self.go = False
                return False

            self.logger.info(json.dumps(self.status, indent=2))

            self.go = True
            return True

        except Exception as error:
            self.logger.error("Failed to check components")
            self.logger.error(str(error))
            return False
