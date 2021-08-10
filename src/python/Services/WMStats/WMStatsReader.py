"""
File       : WMStatsReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from WMStats
"""

import os
from logging import Logger

from Utilities.WebTools import getResponse
from Utilities.Logging import getLogger
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
            self.logger = logger or getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.wmstatsEndpoint = {
                "request": "/wmstatsserver/data/request/",
                "filteredRequest": "/wmstatsserver/data/filtered_requests/",
                "cache": "/wmstatsserver/data/requestcache/",
                "jobdetail": "/wmstatsserver/data/jobdetail/",
            }

        except Exception as error:
            raise Exception(f"Error initializing WMStatsReader\n{str(error)}")

    def getCachedWMStats(self) -> dict:
        """
        The function to get the cached WMStats
        :return: WMStats
        """
        try:
            result = getResponse(self.reqmgrUrl, self.wmstatsEndpoint["cache"])
            return result["result"][0]

        except Exception as error:
            self.logger.error("Failed to get cached wmstats")
            self.logger.error(str(error))

    def getFailedJobs(self, task: str) -> int:
        """
        The function to get the number of failed jobs
        :param task: task name
        :return: number of failed jobs
        """
        try:
            wf = task.split("/")[1]

            result = getResponse(
                self.reqmgrUrl,
                self.wmstatsEndpoint["filteredRequest"],
                param={"RequestName": wf, "mask": ["PrepID", "AgentJobInfo"]},
            )
            data = result["result"]

            failedJobs = 0
            for item in data:
                for _, agent in item.get('AgentJobInfo', {}).items():
                    taskInfo = agent.get("tasks", {}).get(task, {})
                    for _, nFailures in taskInfo.get("status", {}).get("failure", {}).items():
                        failedJobs += nFailures
            
            return failedJobs

        except Exception as error:
            self.logger.error("Failed to get failed jobs")
            self.logger.error(str(error))
