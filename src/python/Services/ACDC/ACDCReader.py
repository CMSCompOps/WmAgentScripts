import os
import logging
from logging import Logger

from Utilities.WebTools import getResponse
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional, List


class ACDCReader(object):
    """
    _ACDCReader_
    General API for reading data from ACDC server
    """

    def __init__(self, logger: Optional[Logger] = None, **contact):
        try:
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.acdcEndpoint = {
                "collection": "/couchdb/acdcserver/_design/ACDC/_view/byCollectionName",
            }  # TODO: check endpoint, call against couchdb

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing ACDCReader\n{str(error)}")

    def getRecoveryDocs(self, wf: str) -> List[dict]:
        """
        The function to get the recovery docs for a given workflow
        :param wf: workflow name
        :return: recovery docs
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl,
                endpoint=self.acdcEndpoint["collection"],
                param={"key": wf, "include_docs": True, "reduce": False},
            )
            return [item["doc"] for item in result["rows"]]

        except Exception as error:
            print("Failed to get the recovery docs")
            print(str(error))
