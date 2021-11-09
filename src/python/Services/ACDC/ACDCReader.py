import os
from logging import Logger

from Utilities.Logging import getLogger
from Utilities.WebTools import getResponse
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional, List


class ACDCReader(object):
    """
    _ACDCReader_
    General API for reading data from ACDC server
    """

    def __init__(self, logger: Optional[Logger] = None):
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.acdcEndpoint = "/couchdb/acdcserver/_design/ACDC/_view/"

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
                endpoint=self.acdcEndpoint + f'byCollectionName?key="{wf}"&include_docs=true&reduce=false',
            )
            return [item["doc"] for item in result["rows"]]

        except Exception as error:
            print("Failed to get the recovery docs")
            print(str(error))
