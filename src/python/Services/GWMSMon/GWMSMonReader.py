import os
import json
from logging import Logger

from Utilities.Logging import getLogger
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional


class GWMSMonReader(object):
    """
    _GWMSMonReader_
    General API for reading data from GWMSMon
    """

    def __init__(self, logger: Optional[Logger] = None):
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.gwmsUrl = configurationHandler.get("gwmsmon_url")
            self.gwmsEndpoint = {"prodView": "/prodview/json/", "poolView": "/poolview/json/"}

        except Exception as error:
            raise Exception(f"Error initializing WorkQueueReader\n{str(error)}")

    def getRequestSummary(self, wf: str) -> dict:
        """
        The function to get the summary for a given workflow
        :param wf: workflow name
        :return: summary
        """
        return self.getViewByKey("prod", f"{wf}/summary")

    def getViewByKey(self, view: str, key: str) -> dict:
        """
        The function to get the view data from GWMSMon for a given key
        :param view: view name — pool or prod
        :param key: key name
        :return: data
        """
        try:
            endpoint = f"{self.gwmsEndpoint[f'{view}View']}{key}"
            with os.popen(f"curl -s {self.gwmsUrl}{endpoint}") as file:
                data = json.loads(file.read())
            return data

        except Exception as error:
            self.logger.error("Failed to get GWMSMon %s from %s view", key, view)
            self.logger.error(str(error))

    def getMCoreReady(self) -> dict:
        """
        The function to get a list of mcore sites
        :return: mcore sites
        """
        try:
            with os.popen(
                "curl --retry 5 -s http://cmsgwms-frontend-global.cern.ch/vofrontend/stage/mcore_siteinfo.json"
            ) as file:
                data = json.loads(file.read())
            return data

        except Exception as error:
            self.logger.error("Failed to get mcore ready")
            self.logger.error(str(error))
