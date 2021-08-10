from logging import Logger

from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Logging import getLogger
from Utilities.WebTools import getResponse

from typing import Optional


class DynamoWriter(object):
    """
    _DynamoWriter_
    General API for writing data on dynamo service
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.dynamoUrl = configurationHandler.get("dynamo_url")

            self.invalidationEndpoint = "/registry/invalidation/invalidate"

        except Exception as error:
            raise Exception(f"Error initializing DynamoWriter\n{str(error)}")

    def invalidateFiles(self, files: list) -> bool:
        """
        The function to invalidate a given list of files
        :param files: files to invalidate
        :return: True if all succeeded, False o/w
        """
        try:
            for file in files:
                response = getResponse(self.dynamoUrl, self.invalidationEndpoint, param={"item": file})

                if response["result"] == "OK":
                    self.logger.info("%s set for invalidation", file)
                else:
                    self.logger.info("Could not set %s for invalidation", file)
                    return False

            return True

        except Exception as error:
            self.logger.error("Failed to invalidate files")
            self.logger.error(str(error))
