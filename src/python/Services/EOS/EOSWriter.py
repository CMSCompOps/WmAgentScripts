import os
from logging import Logger

from Utilities.Logging import getLogger
from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Decorators import runWithRetries

from typing import Optional


class EOSWriter(object):
    """
    _EOSWriter_
    General API for writing data in EOS
    """

    def __init__(self, filename: str, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.cacheDirectory = configurationHandler.get("cache_dir")
            self.cache = (self.cacheDirectory + "/" + filename.replace("/", "_")).replace("//", "/")

            self._filename = filename

        except Exception as error:
            raise Exception(f"Error initializing EOSWriter\n{str(error)}")

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, value: str):
        if not value.replace("//", "/").startswith("/eos/"):
            raise ValueError(f"{value} is not an EOS path")
        self._filename = value.replace("//", "/")

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
        self.logger.info("Moving %s to %s", self.cache, self._filename)
        response = os.system(f"env EOS_MGM_URL=root://eoscms.cern.ch eos cp {self.cache} {self._filename}")
        if response == 0 and os.path.getsize(self._filename) > 0:
            return True

        raise Exception(f"Not able to move {self._filename} to EOS, with code {response}")
