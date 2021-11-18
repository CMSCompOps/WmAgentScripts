import os
from logging import Logger

from Utilities.WebTools import sendResponse
from Utilities.Logging import getLogger
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional
import traceback


class ReqMgrWriter(object):
    """
    _ReqMgrWriter_
    General API for writing data in ReqMgr
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.reqmgrEndpoint = {"agentConfig": "/reqmgr2/data/wmagentconfig/", "request": "/reqmgr2/data/request/"}

        except Exception as error:
            raise Exception(f"Error initializing ReqMgrWriter\n{str(error)}")

    def invalidateWorkflow(self, wf: str, currentStatus: str, cascade: bool = False) -> bool:
        """
        The function to invalidate a workflow
        :param wf: workflow name
        :param currentStatus: current workflow status
        :param cascade: if to cascade the info or not
        :return: True if invalidation succeeded, False o/w
        """
        try:
            if currentStatus in ["aborted", "rejected", "aborted-completed", "aborted-archived", "rejected-archived"]:
                self.logger.info("%s already %s, no action required", wf, currentStatus)
                return True

            param = {"RequestStatus": "aborted", "cascade": str(cascade)}
            if currentStatus in ["assignment-approved", "new", "completed", "closed-out", "announced", "failed"]:
                param = {"RequestStatus": "rejected", "cascade": str(cascade)}
            elif currentStatus == "normal-archived":
                param = {"RequestStatus": "rejected-archived"}

            return self.setWorkflowParam(wf, param)

        except Exception as error:
            self.logger.error("Failed to invalidate %s", wf)
            self.logger.error(str(error))
            return False

    def forceCompleteWorkflow(self, wf: str) -> bool:
        """
        The function to force complete a workflow
        :param wf: workflow name
        :return: True if completion succeeded, False o/w
        """
        return self.setWorkflowParam(wf, {"RequestStatus": "force-complete"})

    def setWorkflowParam(self, wf: str, param: dict) -> bool:
        """
        The function set some params to a given workflow
        :param wf: workflow name
        :param param: workflow param
        :return: True if succeeded, False o/w
        """
        try:
            result = sendResponse(method="PUT", url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["request"] + wf, param=param)
            return any(item.get(wf) == "OK" for item in result["result"])

        except Exception as error:
            self.logger.error("Failed to set %s for %s", param, wf)
            self.logger.error(str(error))
            self.logger.error(traceback.format_exc())
            return False

    def setAgentConfig(self, agent: str, config: dict) -> bool:
        """
        The function to set the configuration for a given agent
        :param agent: agent name
        :param config: agent configuration params
        :return: True if succeeded, False o/w
        """
        try:
            result = sendResponse(
                method= "PUT", url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["agentConfig"] + agent, param=config
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
            self.logger.info(f"Type in submit workflow: {type(wfSchema)}")
            result = sendResponse(method= "POST", url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["request"], param=wfSchema)
            self.logger.info(f"result: {result}")
            return result['result'][0]['request']

        except Exception as error:
            self.logger.error("Failed to submit workflow in reqmgr")
            self.logger.error(str(error))
            self.logger.error(traceback.format_exc())

    def approveWorkflow(self, wf: str) -> bool:
        """
        The function to approve a workflow
        :param wf: workflow name
        :return: True if succeeded, False o/w
        """
        try:
            result = sendResponse(
                method="PUT", url=self.reqmgrUrl, endpoint=f"{self.reqmgrEndpoint['request']}/{wf}", param={"RequestStatus": "assignment-approved"}
            )
            return result["result"][0]["ok"]

        except Exception as error:
            self.logger.error("Failed to approve workflow in reqmgr")
            self.logger.error(str(error))
