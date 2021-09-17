import os
import logging
from logging import Logger

from Utilities.WebTools import sendResponse
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional


class ReqMgrWriter(object):
    """
    _ReqMgrWriter_
    General API for writing data in ReqMgr
    """

    def __init__(self, logger: Optional[Logger] = None, **contact):
        try:
            super().__init__()
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.reqmgrEndpoint = {"agentConfig": "/reqmgr2/data/wmagentconfig/", "request": "/reqmgr2/data/request"}

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing ReqMgrWriter\n{str(error)}")

    def setAgentConfig(self, agent: str, config: dict) -> bool:
        """
        The function to set the configuration for a given agent
        :param agent: agent name
        :param config: agent configuration params
        :return: True if succeeded, False o/w
        """
        try:
            result = sendResponse(
                url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["agentConfig"] + agent, param=config
            )
            return result["result"][0]["ok"]

        except Exception as error:
            self.logger.error("Failed to set configuration in reqmgr for agent %s", agent)
            self.logger.error(str(error))

    def submitWorkflow(self, wfSchema: dict) -> bool:
        """
        The function to submit a workflow (for cloning or resubmition)
        :param wfSchema: workflow schema
        :return: True if succeeded, False o/w
        """
        try:
            result = sendResponse(url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["request"], param=wfSchema)
            return result["result"][0]["ok"]

        except Exception as error:
            self.logger.error("Failed to submit workflow in reqmgr")
            self.logger.error(str(error))

    def approveWorkflow(self, wf: str) -> bool:
        """
        The function to approve a workflow
        :param wf: workflow name
        :return: True if succeeded, False o/w
        """
        try:
            result = sendResponse(
                url=self.reqmgrUrl, endpoint=f"{self.reqmgrEndpoint['request']}/{wf}", param={"RequestStatus": "assignment-approved"}
            )
            return result["result"][0]["ok"]

        except Exception as error:
            self.logger.error("Failed to approve workflow in reqmgr")
            self.logger.error(str(error))
