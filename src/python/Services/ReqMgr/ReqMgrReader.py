"""
File       : ReqMgrReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from ReqMgr
"""

import logging
import os

from Utilities.WebTools import getResponse
from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Decorators import runWithRetries


class ReqMgrReader(object):
    """
    _ReqMgrReader_
    General API for reading data from ReqMgr
    """

    def __init__(self, url=None, logger=None, **contact):

        try:
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)
        except Exception as e:
            msg = "Error initializing ReqMgrReader\n"
            msg += "%s\n" % format(e)
            raise Exception(msg)

    def getWorkflowByCampaign(self, campaign, details=False):
        """
        The function to get the list of workflows for a given campaign
        :param campaign: campaign name
        :param details: if True, it returns details for each workflow, o/w, just workflow names
        :return: list of dicts if details True, list of strings o/w
        """

        try:
            result = getResponse(
                url=self.reqmgrUrl,
                endpoint="/reqmgr2/data/request/",
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
            self.logger.error("Failed to get workflows from reqmgr for campaign %s " % campaign)
            self.logger.error(str(error))

    @runWithRetries(tries=2, wait=1, default=False)
    def getSpec(self, wf: str) -> dict:
        # TODO: check endpoint, call against couchdb
        """
        The function to get the specification for a given workflow
        :param wf: workflow name
        :return: specification
        """
        try:
            return getResponse(url=self.reqmgrUrl, endpoint=f"/couchdb/reqmgr_workload_cache/{wf}/spec", isJson=False)

        except Exception as error:
            print("Failed to get workflow specification")
            print(str(error))

    def getWorkloadSummary(self, wf: str):
        # TODO: check endpoint, call against couchdb
        """
        The function to get the workload summary for a given workflow
        :param wf: workflow name
        :return: workload summary
        """
        try:
            return getResponse(url=self.reqmgrUrl, endpoint="/couchdb/workloadsummary/" + wf)

        except Exception as error:
            print("Failed to get workflow summary")
            print(str(error))
