"""
File       : ReqMgrReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from ReqMgr
"""

import logging
import os
import time
import copy
from Utils.WebTools import getResponse
from Utils.ConfigurationHandler import ConfigurationHandler

from typing import List, Optional, Union, Any, Callable


class ReqMgrReader(object):
    """
    _ReqMgrReader_
    General API for reading data from ReqMgr
    """

    def __init__(
        self, url: str = None, logger: Optional[logging.Logger] = None, **contact
    ) -> None:
        # TODO: url is not being used
        try:
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv(
                "REQMGR_URL", configurationHandler.get("reqmgr_url")
            )
            self.reqmgrEndpoint = {
                "req": "/reqmgr2/data/request/",
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

    def _runWithRetries(
        self,
        fcn: Callable,
        fcn_pargs: list,
        fcn_args: dict = {},
        default: Optional[Any] = None,
        tries: int = 10,
        wait: int = 5,
    ) -> Any:
        # TODO: is there a better place to put this function?
        """
        The function to run a given function with retries
        :param fcn: function
        :param fcn_pargs: function arguments
        :param fnc_args: optional function arguments
        :default: default value to return in case all the tries have failed, raise Exception o/w
        :tries: number of tries
        :wait: wait time between tries
        :return: function output, deafault value o/w
        """
        for i in range(tries):
            try:
                return fcn(*fcn_pargs, **fcn_args)

            except Exception as error:
                self.logger.error(
                    f"Failed to get to run function {fcn.__name__} with arguments {fcn_pargs} and {fcn_args} on try #{i+1} of {tries}"
                )
                self.logger.error(str(error))
                time.sleep(wait)

        if default:
            return default
        raise Exception("NoDefaultValue")

    def getWorkflowByCampaign(
        self, campaign: str, details: bool = False
    ) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given campaign
        :param campaign: campaign name
        :param details: if True, it returns details for each workflow, o/w, just workflow names
        :return: list of dicts if details True, list of strings o/w
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl,
                endpoint=self.reqmgrEndpoint["req"],
                param={"campaign": campaign, "detail": str(details)},
            )

            data = result["result"]
            if details:
                ## list of dict
                r = []
                for it in data:
                    r.extend(it.values())
                return r
            else:
                return data

        except Exception as error:
            self.logger.error(
                f"Failed to get workflows from reqmgr for campaign {campaign}"
            )
            self.logger.error(str(error))

    def getWorkflowsByName(
        self,
        names: Union[str, List[str]],
        details: bool = False,
        tries: int = 5,
        wait: int = 5,
    ):
        # TODO: what does this function return?
        """
        The function to get (???) for given workflow name(s)
        :param names: workflow name(s)
        :param details: if True, it returns (???)
        :param tries: number of tries
        :param wait: wait time between tries
        :return: (???)
        """
        try:
            return self._runWithRetries(
                self._getWorkflowsByName,
                [names],
                {"details": details},
                tries=tries,
                wait=wait,
            )

        except Exception as error:
            self.logger.error(f"Failed to get workflows from reqmgr for {names}")
            self.logger.error(str(error))

    def _getWorkflowsByName(self, names: Union[str, List[str]], details: bool = False):
        # TODO: what does this function return?
        """
        The function to get (???) for given workflow name(s)
        :param names: workflow name(s)
        :param details: if True, it returns (???)
        :return: (???)
        """
        if isinstance(names, str):
            names = [names]

        result = getResponse(
            url=self.reqmgrUrl,
            endpoint=self.reqmgrEndpoint["req"],
            param={"name": names, "detail": str(details)},
        )
        data = result["result"]
        if details and data:
            workflows = data[0].values()
        else:
            workflows = data

        self.logger.info(
            f"{len(workflows)} retrieved for {len(names)} workflow names, {'with' if details else 'without'} details"
        )
        return workflows

    def getWorkflowsByStatus(
        self,
        status: str,
        user: Optional[str] = None,
        details: bool = False,
        rtype: Optional[str] = None,
        priority: Optional[str] = None,
        tries: int = 5,
        wait: int = 5,
    ):
        # TODO: what does this function return?
        """
        The function to get (???) for given a workflow status
        :param status: workflow status
        :param user: request user, if any
        :param details: if True, it returns (???)
        :param type: request type, if any
        :param priority: request priority, if any
        :param tries: number of tries
        :param wait: wait time between tries
        :return: (???)
        """
        try:
            return self._runWithRetries(
                self._getWorkflowsByStatus,
                [status],
                {
                    "user": user,
                    "details": details,
                    "rtype": rtype,
                    "priority": priority,
                },
                tries=tries,
                wait=wait,
            )

        except Exception as error:
            self.logger.error(
                f"Failed to get workflows from reqmgr for {status} (user={user}, type={rtype}, priority={priority})"
            )
            self.logger.error(str(error))

    def _getWorkflowsByStatus(
        self,
        status: str,
        user: Optional[str] = None,
        details: bool = False,
        rtype: Optional[str] = None,
        priority: Optional[str] = None,
    ):
        # TODO: what does this function return?
        """
        The function to get (???) for given a workflow status
        :param status: workflow status
        :param user: request user, if any
        :param details: if True, it returns (???)
        :param type: request type, if any
        :param priority: request priority, if any
        :return: (???)
        """
        param = {"status": status, "detail": str(details)}
        if user:
            param["requestor"] = user.split(",")
        if rtype:
            param["request_type"] = rtype
        if priority:
            # TODO: does this work?
            param["initialpriority"] = priority
            self.logger.info(f"Priority {priority} is requested")

        result = getResponse(
            self.reqmgrUrl, endpoint=self.reqmgrEndpoint["req"], param=param
        )
        data = result["result"]

        workflows = []
        for item in data:
            if details:
                workflows.extend([v for k, v in item.items()])

        self.logger.info(
            f"{len(workflows)} retrieved for {len(status)} status (user={user}, type={rtype}, priority={priority})"
        )

    def getWorkflowByOutput(self, dataset: str, details: bool = False):
        # TODO: what does this function return?
        """
        The function to get (???) for a given dataset
        :param dataset: dateset name
        :param details: if True, it returns (???)
        :return: list of (???)
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl,
                endpoint=self.reqmgrEndpoint["req"],
                param={"outputdataset": dataset, "detail": str(details)},
            )
            data = result["rows"]
            key = "doc" if details else "id"
            return [item[key] for item in data]

        except Exception as error:
            self.logger.error(
                f"Failed to get workflows from reqmgr for dataset {dataset}"
            )
            self.logger.error(str(error))

    def getWorkflowById(self, pid: str, details: bool = False):
        # TODO: what does this function return?
        """
        The function to get (???) for a given id
        :param pid: id
        :param details: if True, it returns (???)
        :return: list of (???)
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl,
                endpoint=self.reqmgrEndpoint["req"],
                param={"prep_id": pid, "detail": str(details)},
            )
            data = result["rows"]
            key = "doc" if details else "id"
            return [item[key] for item in data]

        except Exception as error:
            self.logger.error(f"Failed to get workflows from reqmgr for id {pid}")
            self.logger.error(str(error))

    def getWorkLoad(self, wf: str, tries: int = 2, try_cache: bool = False):
        # TODO: what does this function return?
        # TODO: this function does the same as request of line 3814, in the workflowInfo.__init__.
        # I added the try_cache param so that this function can deal with what is done in the workflowInfo
        """
        The function to get the workload for a given workflow
        :param wf: workflow name
        :param tries: number of tries for gettting a request response
        :return: (???)
        """
        try:
            self._runWithRetries(self._getWorkLoad, [wf], tries=tries, wait=0)

        except Exception as error:
            self.logger.error(f"Failed to get workload from reqmgr for workflow {wf}")
            self.logger.error(str(error))
            if try_cache:
                self.logger.info("Will try to get workload cache")
                return self._getWorkLoadCache(wf)

    def _getWorkLoad(self, wf: str):
        # TODO: what does this function return?
        """
        The function to get the workload for a given workflow
        :param wf: workflow name
        :return: (???)
        """
        result = getResponse(
            url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["req"] + wf
        )
        return result["result"][0][wf]

    def _getWorkLoadCache(self, wf: str):
        # TODO: what does this function return?
        # TODO: is this name ok? it was in workflowInfo.__init__, after line 3823
        """
        The function to get cache info for a given workflow
        :param wf: workflow
        :return: (???)
        """
        try:
            # TODO: confirm call against couchdb
            result = getResponse(
                url=self.reqmgrUrl,
                endpoint="/couchdb/reqmgr_workload_cache/" + wf,
            )
            return result["result"][0][wf]

        except Exception as error:
            self.logger.error(f"Failed to get workload cache for workflow {wf}")
            self.logger.error(str(error))

    def getReqmgrInfo(self):
        # TODO: what does this function return?
        """
        The function to get the reqmgr info
        :return: (???)
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["info"]
            )
            return result["result"]

        except Exception as error:
            self.logger.error("Failed to get reqmgr info")
            self.logger.error(str(error))

    def getAgentConfig(self, agent: str):
        # TODO: what does this function return?
        """
        The function to get the configuration for a given agent
        :param agent: agent name
        :return: (???)
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["agentConfig"] + agent
            )
            return result["result"][-1]

        except Exception as error:
            self.logger.error(
                f"Failed to get configuration from reqmgr for agent {agent}"
            )
            self.logger.error(str(error))

    def getSchema(self, name: str) -> dict:
        # TODO: what does this function return?
        """
        The function to get the schema for a given request name
        :param name: agent name
        :return: a dict of (???)
        """
        try:
            result = self._getWorkflowsByName(name)
            # TODO: is the deepcopy necessary?
            data = copy.deepcopy(result[0][name])
            return {k: v for k, v in data.items() if v not in [None, "None"]}

        except Exception as error:
            self.logger.error(f"Failed to get schema from reqmgr for {name}")
            self.logger.error(str(error))

    def getSplittingsNew(self, name: str, strip: bool = False, all_tasks: bool = False):
        # TODO: what does this function return?
        # TODO: is this a new version of getSplittings?
        """
        The function to get splittings for a given request name
        :param name: request name
        :return: (???)
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["splitting"] + name
            )
            data = result["result"]

            splittings = []
            for splt in data:
                if not all_tasks and not splt["taskType"] in [
                    "Production",
                    "Processing",
                    "Skim",
                ]:
                    continue

                if strip:
                    for drop in [
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
                    ]:
                        splt["splitParams"].pop(drop, None)

                    if splt["splitAlgo"] is "LumiBased":
                        for drop in ["events_per_job", "job_time_limit"]:
                            splt["splitParams"].pop(drop, None)

                splittings.append(splt)

            return splittings

        except Exception as error:
            self.logger.error(f"Failed to get splittings from reqmgr for {name}")
            self.logger.error(str(error))
