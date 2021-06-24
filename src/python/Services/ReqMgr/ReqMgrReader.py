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
                "couchdb": "/couchdb/reqmgr_workload_cache/",
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

    def getWorkflowSchema(
        self,
        wf: str,
        drop_null: bool = False,
        make_copy: bool = False,
        tries: int = 2,
        try_cache: bool = False,
        wait: int = 0,
    ) -> dict:
        """
        The function to get the schema for a given workflow
        :param wf: workflow name
        :param drop_null: if True, drop key with null values from the schema
        :param make_copy: if True, return a copy of the schema
        :param tries: number of tries for gettting a request response
        :return: a dict
        """
        try:
            return self._runWithRetries(
                self._getWorkflowSchema,
                [wf],
                {"drop_null": drop_null, "make_copy": make_copy},
                tries=tries,
                wait=wait,
            )

        except Exception as error:
            self.logger.error(f"Failed to get workload from reqmgr for workflow {wf}")
            self.logger.error(str(error))
            if try_cache:
                self.logger.info("Will try to get workload cache")
                return self._getWorkflowSchema(
                    wf, drop_null=drop_null, make_copy=make_copy, from_cache=True
                )

    def _getWorkflowSchema(
        self,
        wf: str,
        drop_null: bool = False,
        make_copy: bool = False,
        from_cache: bool = False,
    ) -> dict:
        """
        The function to get the schema for a given workflow
        :param wf: workflow name
        :param drop_null: if True, drop key with null values from the schema
        :param make_copy: if True, return a copy of the schema
        :param from_cache: if True, get schema from cache
        :return: a dict
        """
        result = getResponse(
            url=self.reqmgrUrl,
            endpoint=self.reqmgrEndpoint["couchdb" if from_cache else "req"] + wf,
        )
        if make_copy:
            data = copy.deepcopy(result["result"][0][wf])
        else:
            data = result["result"][0][wf]
            
        if drop_null:
            return {k: v for k, v in data.items() if v not in [None, "None"]}
        return data

    def getWorkflowSchemaByCampaign(
        self, campaign: str, details: bool = False
    ) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given campaign
        :param campaign: campaign name
        :param details: if True, it returns details for each workflow, o/w, just workflow names
        :return: list of dicts if details True, list of strings o/w
        """
        try:
            return self._getWorkflowSchemaByParam({"campaign": campaign}, details)

        except Exception as error:
            self.logger.error(
                f"Failed to get workflows from reqmgr for campaign {campaign}"
            )
            self.logger.error(str(error))

    def getWorkflowSchemaByOutput(
        self, dataset: str, details: bool = False
    ) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given output
        :param dataset: dateset name
        :param details: if True, it returns details for each workflow, o/w just workflows names
        :return: a list of dicts if details is True, list of strings o/w
        """
        try:
            return self._getWorkflowSchemaByParam({"outputdataset": dataset}, details)

        except Exception as error:
            self.logger.error(
                f"Failed to get workflows from reqmgr for dataset {dataset}"
            )
            self.logger.error(str(error))

    def getWorkflowSchemaByPrepId(
        self, pid: str, details: bool = False
    ) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given prep id
        :param pid: id
        :param details: if True, it returns details for each workflow, o/w just workflows names
        :return: a list of dicts if details is True, list of strings o/w
        """
        try:
            return self._getWorkflowSchemaByParam({"prep_id": pid}, details)

        except Exception as error:
            self.logger.error(f"Failed to get workflows from reqmgr for id {pid}")
            self.logger.error(str(error))

    def getWorkflowsByStatus(
        self,
        status: str,
        user: Optional[str] = None,
        details: bool = False,
        rtype: Optional[str] = None,
        priority: Optional[str] = None,
        tries: int = 5,
        wait: int = 5,
    ) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given status (and user/request type/priority)
        :param status: workflow status
        :param user: request user, if any
        :param details: if True, it returns it returns details for each workflow, o/w just workflows names
        :param type: request type, if any
        :param priority: request priority, if any
        :param tries: number of tries
        :param wait: wait time between tries
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
                self.logger.info(f"Priority {priority} is requested")

            return self._runWithRetries(
                self._getWorkflowSchemaByParam,
                [param],
                {"details": details},
                tries=tries,
                wait=wait,
            )

        except Exception as error:
            self.logger.error(
                f"Failed to get workflows from reqmgr for {status} (user={user}, type={rtype}, priority={priority})"
            )
            self.logger.error(str(error))

    def getWorkflowSchemaByName(
        self,
        names: Union[str, List[str]],
        details: bool = False,
        tries: int = 5,
        wait: int = 5,
    ) -> Union[List[dict], List[str]]:
        """
        The function to get the list of workflows for a given name (or names)
        :param names: workflow name(s)
        :param details: if True, it returns it returns details for each workflow, o/w just workflows names
        :param tries: number of tries
        :param wait: wait time between tries
        :return: a list of dicts if details is True, list of strings o/w
        """
        try:
            return self._runWithRetries(
                self._getWorkflowSchemaByParam,
                [{"name": names}],
                {"details": details},
                tries=tries,
                wait=wait,
            )

        except Exception as error:
            self.logger.error(f"Failed to get workflows from reqmgr for {names}")
            self.logger.error(str(error))

    def _getWorkflowSchemaByParam(
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
            endpoint=self.reqmgrEndpoint["req"],
            param={**param, **{"detail": str(details)}},
        )
        data = result["result"]

        if details and data:
            data = [*data[0].values()]

        self.logger.info(f"{len(data)} workflows retrieved for {param}")

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
                f"Failed to get configuration from reqmgr for agent {agent}"
            )
            self.logger.error(str(error))

    def getSplittingsNew(
        self, wf: str, strip: bool = False, all_tasks: bool = False
    ) -> List[dict]:
        # TODO: rename
        """
        The function to get splittings for a given workflow name
        :param wf: workflow name
        :param strip: if True, it will drop some split params, o/w it will keep all params
        :param all_tasks: if True, it will keep all tasks types, o/w it will keep only production, processing and skim tasks
        :return: a list of dicts
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl, endpoint=self.reqmgrEndpoint["splitting"] + wf
            )
            data = result["result"]
            if not strip and all_tasks:
                return data

            keepingTasksOfType = ["Production", "Processing", "Skim"]
            dropingParams = [
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
            dropingLumiBasedParams = ["events_per_job", "job_time_limit"]

            cleanData = []
            for splt in data:
                if not all_tasks and splt["taskType"] not in keepingTasksOfType:
                    continue

                if strip:
                    for drop in dropingParams:
                        splt["splitParams"].pop(drop, None)

                    if splt["splitAlgo"] is "LumiBased":
                        for drop in dropingLumiBasedParams:
                            splt["splitParams"].pop(drop, None)

                cleanData.append(splt)
            return cleanData

        except Exception as error:
            self.logger.error(f"Failed to get splittings from reqmgr for {wf}")
            self.logger.error(str(error))
