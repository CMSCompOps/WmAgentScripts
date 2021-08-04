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
            super().__init__()
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.wmstatsEndpoint = {
                "request": "/wmstatsserver/data/request/",
                "cache": "/wmstatsserver/data/requestcache",
                "jobdetail": "/wmstatsserver/data/jobdetail/",
            }

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

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
