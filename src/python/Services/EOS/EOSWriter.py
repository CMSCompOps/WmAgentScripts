import os
import logging
from logging import Logger

from typing import Optional

from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Decorators import runWithRetries


class EOSWriter(object):
    """
    _EOSWriter_
    General API for writing data in EOS
    """

    def __init__(self, filename: str, logger: Optional[Logger] = None) -> None:
        try:
            if not filename.startswith("/eos/"):
                raise ValueError(f"{filename} is not an EOS path")

            configurationHandler = ConfigurationHandler()
            self.cacheDirectory = configurationHandler.get("cache_dir")
            self.cache = (self.cacheDirectory + "/" + filename.replace("/", "_")).replace("//", "/")
            self.filename = filename.replace("//", "/")

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as e:
            msg = "Error initializing EOSWriter\n"
            msg += f"{e}\n"
            raise Exception(msg)

    def write(self, content: str) -> None:
        """
        The function to write content in EOS file
        :param content: file content
        """
        try:
            with open(self.cache, "w") as file:
                file.write(content)

        except Exception as error:
            self.logger.error("Failed to write in EOS file")
            self.logger.error(str(error))

    @runWithRetries(tries=5, wait=30, default=False)
    def save(self) -> bool:
        """
        The function to save file in EOS
        """
        self.logger.info("Moving %s to %s", self.cache, self.filename)
        response = os.system(f"env EOS_MGM_URL=root://eoscms.cern.ch eos cp {self.cache} {self.filename}")
        if response == 0 or os.path.getsize(self.filename) > 0:
            return True

        raise Exception(f"Not able to move {self.filename} to EOS, with code {response}")
