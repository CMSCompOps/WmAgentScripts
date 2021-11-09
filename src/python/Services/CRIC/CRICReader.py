from logging import Logger

from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.WebTools import getResponse
from Utilities.Logging import getLogger

from typing import Optional, List


class CRICReader(object):
    """
    __CRICReader__
    General API for reading data from CMS CRIC system
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.cricUrl = configurationHandler.get("cric_url")
            self.cricEndpoint = "/api/cms/site/query/"

        except Exception as error:
            raise Exception(f"Error initializing CRICReader\n{str(error)}")

    def getSiteStorage(self) -> List[list]:
        """
        The function to get the site storage
        :return: list of site-storage pairs
        """
        try:
            result = getResponse(self.cricUrl, endpoint=self.cricEndpoint + "?json&preset=data-processing")
            return result["result"]

        except Exception as error:
            self.logger.error("Failed to get site storage")
            self.logger.error(str(error))
