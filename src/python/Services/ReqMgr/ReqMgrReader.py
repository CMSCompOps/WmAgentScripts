"""
File       : ReqMgrReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from ReqMgr
"""

import logging
from logging import Logger
import os
import copy
from Utilities.WebTools import getResponse
from Utilities.IteratorTools import mapKeys, filterKeys
from Utilities.Decorators import runWithRetries
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import List, Optional, Union


class ReqMgrReader(object):
    """
    _ReqMgrReader_
    General API for reading data from ReqMgr
    """

    def __init__(self, logger: Optional[Logger] = None, **contact):
        try:
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.reqmgrEndpoint = {
                "request": "/reqmgr2/data/request/",
                "info": "/reqmgr2/data/info/",
                "agentConfig": "/reqmgr2/data/wmagentconfig/",
                "splitting": "/reqmgr2/data/splitting/",
            }

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing ReqMgrReader\n{str(error)}")

    def getWorkflowSchema(self, wf: str, makeCopy: bool = False) -> dict:
        """
        The function to get the schema for a given workflow
        :param wf: workflow name
        :param makeCopy: if True, return a copy of the schema
        :return: workflow schema
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl,
                endpoint=self.reqmgrEndpoint["request"],
                param={"name": wf},
            )
            data = result["result"][0][wf]
            return copy.deepcopy(data) if makeCopy else data

        except Exception as error:
            self.logger.error("Failed to get workload from reqmgr for workflow %s", wf)
            self.logger.error(str(error))

    def getWorkflowsByCampaign(self, campaign: str, details: bool = False) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given campaign
        :param campaign: campaign name
        :param details: if True, it returns details for each workflow, o/w, just workflow names
        :return: list of dicts if details True, list of strings o/w
        """
        try:
            return self.getWorkflowsByParam({"campaign": campaign}, details)

        except Exception as error:
            self.logger.error("Failed to get workflows from reqmgr for campaign %s", campaign)
            self.logger.error(str(error))

    def getWorkflowsByOutput(self, dataset: str, details: bool = False) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given output
        :param dataset: dateset name
        :param details: if True, it returns details for each workflow, o/w just workflows names
        :return: a list of dicts if details is True, list of strings o/w
        """
        try:
            return self.getWorkflowsByParam({"outputdataset": dataset}, details)

        except Exception as error:
            self.logger.error("Failed to get workflows from reqmgr for dataset %s", dataset)
            self.logger.error(str(error))

    def getWorkflowsByPrepId(self, pid: str, details: bool = False) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given prep id
        :param pid: prep id
        :param details: if True, it returns details for each workflow, o/w just workflows names
        :return: a list of dicts if details is True, list of strings o/w
        """
        try:
            return self.getWorkflowsByParam({"prep_id": pid}, details)

        except Exception as error:
            self.logger.error("Failed to get workflows from reqmgr for id %s", pid)
            self.logger.error(str(error))

    def getWorkflowsByStatus(self, status: str, details: bool = False, **extraParam) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given status (and user/request type/priority)
        :param status: workflow status
        :param details: if True, it returns it returns details for each workflow, o/w just workflows names
        :param extraParam: user, requestType and/or priority, if any
        :return: a list of dicts if details is True, list of strings o/w
        """
        try:
            extraParamMap = {
                "user": "requestor",
                "requestType": "request_type",
                "priority": "initialpriority",
            }
            extraParam = filterKeys(extraParamMap, extraParam)
            extraParam = mapKeys(lambda k: extraParamMap[k], extraParam)
            return self.getWorkflowsByParam({**{"status": status}, **extraParam}, details=details)

        except Exception as error:
            self.logger.error(
                "Failed to get workflows from reqmgr for status %s (and other param: %s)",
                status,
                extraParam,
            )
            self.logger.error(str(error))

    def getWorkflowsByNames(self, names: Union[str, List[str]], details: bool = False) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given name (or names)
        :param names: workflow name(s)
        :param details: if True, it returns it returns details for each workflow, o/w just workflows names
        :return: a list of dicts if details is True, list of strings o/w
        """
        try:
            return self.getWorkflowsByParam({"name": names}, details=details)

        except Exception as error:
            self.logger.error("Failed to get workflows from reqmgr for %s", names)
            self.logger.error(str(error))

    @runWithRetries(tries=2)
    def getWorkflowsByParam(self, param: dict, details: bool = False) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for given key/value pairs of params
        :param param: key/value
        :param details: if True, it returns details for each workflow, o/w just workflows names
        :return: a list of dicts if details is True, list of strings o/w
        """
        result = getResponse(
            url=self.reqmgrUrl,
            endpoint=self.reqmgrEndpoint["request"],
            param={**param, **{"detail": str(details)}},
        )
        data = result["result"]

        if details and data:
            data = [*data[0].values()]

        self.logger.info("%s workflows retrieved for %s", len(data), param)

        return data

    def getReqmgrInfo(self) -> List[dict]:
        """
        The function to get the reqmgr info
        :return: a list of dicts
        """
        try:
            result = getResponse(url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["info"])
            return result["result"]

        except Exception as error:
            self.logger.error("Failed to get reqmgr info")
            self.logger.error(str(error))

    def getAgentConfig(self, agent: str) -> dict:
        """
        The function to get the configuration for a given agent
        :param agent: agent name
        :return: a dict
        """
        try:
            result = getResponse(url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["agentConfig"] + agent)
            return result["result"][-1]

        except Exception as error:
            self.logger.error("Failed to get configuration from reqmgr for agent %s", agent)
            self.logger.error(str(error))

    def getSplittingsSchema(self, wf: str) -> List[dict]:
        """
        The function to get splittings for a given workflow name
        :param wf: workflow name
        :return: a list of dicts
        """
        try:
            result = getResponse(url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["splitting"] + wf)
            return result["result"]

        except Exception as error:
            self.logger.error("Failed to get splittings from reqmgr for %s", wf)
            self.logger.error(str(error))
