import os
import json
import logging
from logging import Logger

from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional


class GWMSMonReader(object):
    """
    _GWMSMonReader_
    General API for reading data from GWMSMon
    """

    def __init__(self, logger: Optional[Logger] = None, **contact):
        try:
            super().__init__()
            configurationHandler = ConfigurationHandler()
            self.gwmsUrl = configurationHandler.get("gwmsmon_url")
            self.gwmsEndpoint = {"prodView": "/prodview/json/", "poolView": "/poolview/json/"}

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

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
            print("Failed to get GWMSMon %s from %s view", key, view)
            print(str(error))

    def getMCoreReady(self) -> list:
        """
        The function to get a list of mcore sites
        :return: mcore sites
        """
        try:
            with os.popen(
                "curl --retry 5 -s http://cmsgwms-frontend-global.cern.ch/vofrontend/stage/mcore_siteinfo.json"
            ) as file:
                data = json.loads(file.read())
            return data.get("sites_for_mcore", [])

        except Exception as error:
            print("Failed to get mcore ready")
            print(str(error))
