import os
import json
import logging
from logging import Logger

from typing import Optional


class GWMSMonReader(object):
    """
    _GWMSMonReader_
    General API for reading data from GWMSMon
    """

    def __init__(self, logger: Optional[Logger] = None):
        try:
            self.gwmsUrl = "https://cms-gwmsmon.cern.ch"
            self.gwmsEndpoint = {
                "prodView": "/prodview/json/",
            }

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
        try:
            endpoint = f"{self.gwmsEndpoint['prodView']}{wf}/summary"
            with os.popen(f"curl -s {self.gwmsUrl}{endpoint}") as file:
                data = json.loads(file.read())
            return data

        except Exception as error:
            self.logger.error("Failed to get summary for workflow %s", wf)
            self.logger.error(str(error))
