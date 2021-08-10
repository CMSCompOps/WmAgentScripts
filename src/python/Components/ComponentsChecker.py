import os
import sys
import socket
import json
from logging import Logger
from time import sleep

from Databases.Oracle.OracleClient import OracleClient
from Databases.Oracle.OracleDB import Workflow
from Components.Agent.AgentStatusController import AgentStatusController
from Storage.EOS.EOSWriter import EOSWriter
from Services.ReqMgr.ReqMgrReader import ReqMgrReader
from Services.DBS.DBSReader import DBSReader

from Utilities.Decorators import runWithMultiThreading
from Utilities.Logging import getLogger
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional, List


class ComponentsChecker(object):
    """
    _ComponentsChecker_
    General API for checking the components
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.block = kwargs.get("block") or True
            self.keepTrying = kwargs.get("keepTrying") or False
            self.softComponents = kwargs.get("soft") or ["mcm", "wtc", "mongo", "jira"]

            self.code = 0
            self.go = False
            self.checking = None
            self.host = socket.gethostname()

            self.status = {
                "ReqMgr": False,
                "McM": False,
                "DBS": False,
                "CMSr": False,
                "Wtc": False,
                "EOS": False,
                "Mongo": False,
                "Jira": False,
            }

        except Exception as error:
            raise Exception(f"Error initializing ComponentsChecker\n{str(error)}")

    def checkCMSr(self) -> bool:
        """
        The function to check CMSr
        :return: True if ok, False o/w
        """
        try:
            oracleClient = OracleClient()
            _ = oracleClient.session.query(Workflow).filter(Workflow.name.contains("1")).all()
            return True

        except Exception as error:
            self.logger.error("Failed to check CMSr")
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

    def checkWtc(self) -> bool:
        """
        The function to check Wtc
        :return: True if ok, False o/w
        """
        try:
            # TODO
            return True

        except Exception as error:
            self.logger.error("Failed to check Wtc")
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
            agentStatusController = AgentStatusController()
            _ = agentStatusController.getAgents()
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

    @runWithMultiThreading(mtParam="components", timeout=120, wait=10)
    def _checkComponent(self, components: List[str]) -> bool:
        """
        The function to check a given component
        :param components: components name
        :return: True if ok, False o/w
        """
        try:
            self.logger.info("Checking on %s", components)
            self.checking = components
            sys.stdout.flush()

            isOk = getattr(self, f"check{components}")()
            if isOk:
                self.status[components] = True
                return True

            self.logger.critical("The %s component is unreachable from %s", components, self.host)

            if self.keepTrying:
                self.logger.info("Re-checking on %s", components)
                sleep(30)
                return self._checkComponent(components=[components])
            if self.block and components not in self.softComponents:
                return False

            return True

        except Exception as error:
            self.logger.error("Failed to check component")
            self.logger.error(str(error))
            return False

    def check(self) -> bool:
        """
        The function to check all components
        :return: True if ok, False o/w
        """
        try:
            checks = self._checkComponent(components=sorted(self.status))
            if not all(checks):
                self.code = 120 + sum(checks)
                self.go = False
                return False

            self.logger.info(json.dumps(self.status, indent=2))

            sys.stdout.flush()
            self.go = True
            return True

        except Exception as error:
            self.logger.error("Failed to check components")
            self.logger.error(str(error))
            return False
