"""
File       : WMStatsReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from WMStats
"""

import os
import logging
from logging import Logger
from collections import defaultdict

from Utilities.WebTools import getResponse
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional


class WMStatsReader(object):
    """
    _WMStats_
    General API for reading data from WMStats
    """

    def __init__(self, logger: Optional[Logger] = None, **contact):
        try:
            super().__init__()
            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.wmstatsEndpoint = {
                "request": "/wmstatsserver/data/request/",
                "jobdetail": "/wmstatsserver/data/jobdetail/",
                "agentInfo": "/couchdb/wmstats/_design/WMStats/_view/agentInfo?stale=update_after",
            }

        except Exception as error:
            raise Exception(f"Error initializing WMStatsReader\n{str(error)}")

    def getWMStats(self, wf: str) -> dict:
        """
        The function to get the WMStats for a given workflow
        :param wf: workflow name
        :return: WMStats
        """
        try:
            result = getResponse(self.reqmgrUrl, self.wmstatsEndpoint["request"] + wf)
            return result["result"][0].get(wf, {})

        except Exception as error:
            self.logger.error("Failed to get wmstats for %s", wf)
            self.logger.error(str(error))

    def getWMErrors(self, wf: str) -> dict:
        """
        The function to get the WMErrors for a given workflow
        :param wf: workflow name
        :return: WMErrors
        """
        try:
            result = getResponse(url=self.reqmgrUrl, endpoint=self.wmstatsEndpoint["jobdetail"] + wf)
            return result["result"][0].get(wf, {})

        except Exception as error:
            self.logger.error("Failed to get wmerrors for %s", wf)
            self.logger.error(str(error))

    def getAgents(self) -> dict:
        """
        The function to get all agents by team
        :return: agents
        """
        try:
            result = getResponse(self.reqmgrUrl, self.wmstatsEndpoint["agentInfo"])

            data = defaultdict(list)
            for item in [row["value"] for row in result["rows"]]:
                data[item["agent_team"]].append(item)
            return dict(data)

        except Exception as error:
            self.logger.error("Failed to get agents from wmstats")
            self.logger.error(str(error))

    def getProductionAgents(self) -> dict:
        """
        The function to get all agents in production
        :return: agents
        """
        try:
            agents = self.getAgents().get("production")
            return dict((agent["agent_url"].split(":")[0], agent) for agent in agents)

        except Exception as error:
            self.logger.error("Failed to get production agents from wmstats")
            self.logger.error(str(error))
