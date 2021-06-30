"""
File       : ReqMgrReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from ReqMgr
"""

import logging
import os
import copy
from Utils.WebTools import getResponse
from Utils.Decorators import runWithRetries
from Utils.ConfigurationHandler import ConfigurationHandler

from typing import List, Optional, Union


class ReqMgrReader(object):
    """
    _ReqMgrReader_
    General API for reading data from ReqMgr
    """

    def __init__(self, logger: Optional[logging.Logger] = None, **contact):
        try:
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv(
                "REQMGR_URL", configurationHandler.get("reqmgr_url")
            )
            self.reqmgrEndpoint = {
                "request": "/reqmgr2/data/request/",
                "info": "/reqmgr2/data/info/",
                "agentConfig": "/reqmgr2/data/wmagentconfig/",
                "splitting": "/reqmgr2/data/splitting/",
            }

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            msg = "Error initializing ReqMgrReader\n"
            msg += f"{error}\n"
            raise Exception(msg)

    def getWorkflowSchema(self, wf: str, makeCopy: bool = False) -> dict:
        """
        The function to get the schema for a given workflow
        :param wf: workflow name
        :param makeCopy: if True, return a copy of the schema
        :return: a dict
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl,
                endpoint=self.reqmgrEndpoint["request"],
                param={"name": wf},
            )
            if makeCopy:
                return copy.deepcopy(result["result"][0][wf])
            return result["result"][0][wf]

        except Exception as error:
            self.logger.error("Failed to get workload from reqmgr for workflow %s", wf)
            self.logger.error(str(error))

    def getWorkflowsByCampaign(
        self, campaign: str, details: bool = False
    ) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given campaign
        :param campaign: campaign name
        :param details: if True, it returns details for each workflow, o/w, just workflow names
        :return: list of dicts if details True, list of strings o/w
        """
        try:
            return self.getWorkflowsByParam({"campaign": campaign}, details)

        except Exception as error:
            self.logger.error(
                "Failed to get workflows from reqmgr for campaign %s", campaign
            )
            self.logger.error(str(error))

    def getWorkflowsByOutput(
        self, dataset: str, details: bool = False
    ) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given output
        :param dataset: dateset name
        :param details: if True, it returns details for each workflow, o/w just workflows names
        :return: a list of dicts if details is True, list of strings o/w
        """
        try:
            return self.getWorkflowsByParam({"outputdataset": dataset}, details)

        except Exception as error:
            self.logger.error(
                "Failed to get workflows from reqmgr for dataset %s", dataset
            )
            self.logger.error(str(error))

    def getWorkflowsByPrepId(
        self, pid: str, details: bool = False
    ) -> Union[List[dict], List[str]]:
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

    def getWorkflowsByStatus(
        self,
        status: str,
        details: bool = False,
        user: Optional[str] = None,
        rtype: Optional[str] = None,
        priority: Optional[str] = None,
    ) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given status (and user/request type/priority)
        :param status: workflow status
        :param details: if True, it returns it returns details for each workflow, o/w just workflows names
        :param user: request user, if any
        :param type: request type, if any
        :param priority: request priority, if any
        :return: a list of dicts if details is True, list of strings o/w
        """
        try:
            param = {"status": status}
            if user:
                param["requestor"] = user.split(",")
            if rtype:
                param["request_type"] = rtype
            if priority:
                # TODO: does this work? if not, can I remove this?
                param["initialpriority"] = priority
                self.logger.info("Priority %s is requested", priority)

            return self.getWorkflowsByParam(param, details=details)

        except Exception as error:
            self.logger.error(
                "Failed to get workflows from reqmgr for %s (user=%s, type=%s, priority=%s)",
                status,
                user,
                rtype,
                priority,
            )
            self.logger.error(str(error))

    def getWorkflowsByNames(
        self, names: Union[str, List[str]], details: bool = False
    ) -> Union[List[dict], List[str]]:
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
    def getWorkflowsByParam(
        self, param: dict, details: bool = False
    ) -> Union[List[dict], List[str]]:
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
            result = getResponse(
                url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["info"]
            )
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
            result = getResponse(
                url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["agentConfig"] + agent
            )
            return result["result"][-1]

        except Exception as error:
            self.logger.error(
                "Failed to get configuration from reqmgr for agent %s", agent
            )
            self.logger.error(str(error))

    def getSplittingsSchema(
        self, wf: str, strip: bool = False, allTasks: bool = False
    ) -> List[dict]:
        """
        The function to get splittings for a given workflow name
        :param wf: workflow name
        :param strip: if True, it will drop some split params, o/w it will keep all params
        :param allTasks: if True, it will keep all tasks types, o/w it will keep only production, processing and skim tasks
        :return: a list of dicts
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["splitting"] + wf
            )
            data = result["result"]

            if not allTasks:
                data = self._filterSplittingsTaskTypes(data)
            if strip:
                data = self._stripSplittingsParam(data)
            return data

        except Exception as error:
            self.logger.error("Failed to get splittings from reqmgr for %s", wf)
            self.logger.error(str(error))

    def _filterSplittingsTaskTypes(self, splittings: List[dict]) -> List[dict]:
        """
        The function to filter tasks types in splittings schema
        :param splittings: workflow name
        :return: a list of dicts where task types are production, processing or skim
        """
        tasksToKeep = ["Production", "Processing", "Skim"]
        return [splt for splt in splittings if splt["taskType"] in tasksToKeep]

    def _stripSplittingsParam(self, splittings: List[dict]) -> List[dict]:
        """
        The function to drop params from splittings schema
        :param splittings: workflow name
        :return: a list of dicts
        """
        paramsToDrop = [
            "algorithm",
            "trustPUSitelists",
            "trustSitelists",
            "deterministicPileup",
            "type",
            "include_parents",
            "lheInputFiles",
            "runWhitelist",
            "runBlacklist",
            "collectionName",
            "group",
            "couchDB",
            "couchURL",
            "owner",
            "initial_lfn_counter",
            "filesetName",
            "runs",
            "lumis",
        ]
        lumiBasedParamsToDrop = ["events_per_job", "job_time_limit"]

        cleanSplittings = []
        for splt in splittings:
            for param in paramsToDrop:
                splt["splitParams"].pop(param, None)

            if splt["splitAlgo"] is "LumiBased":
                for param in lumiBasedParamsToDrop:
                    splt["splitParams"].pop(param, None)
            cleanSplittings.append(splt)
        return cleanSplittings
