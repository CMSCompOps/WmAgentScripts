"""
File       : WMStatsReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from WMStats
"""
import os
import logging
from logging import Logger

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
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.wmstatsEndpoint = {
                "request": "/wmstatsserver/data/request/",
                "jobdetail": "/wmstatsserver/data/jobdetail/",
            }

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing WMStatsReader\n{str(error)}")

    def getWMStats(self, wf: str):
        """
        The function to get the WMStats for a given workflow
        :param wf: workflow name
        :return: WMStats
        """
        try:
            result = getResponse(self.reqmgrUrl, self.wmstatsEndpoint["request"] + wf)
            return result["result"][0][wf]

        except Exception as error:
            self.logger.error("Failed to get wmstats for %s", wf)
            self.logger.error(str(error))

    def getWMErrors(self, wf: str):
        """
        The function to get the WMErrors for a given workflow
        :param wf: workflow name
        :return: WMErrors
        """
        try:
            result = getResponse(self.reqmgrUrl, self.wmstatsEndpoint["jobdetail"] + wf)
            return result["result"][0][wf]

        except Exception as error:
            self.logger.error("Failed to get wmerrors for %s", wf)
            self.logger.error(str(error))
