import os
import json
import logging
from logging import Logger

from typing import Optional

from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Decorators import runWithRetries


class EOSReader(object):
    """
    _EOSReader_
    General API for reading data from EOS
    """

    def __init__(self, filename: str, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()

            configurationHandler = ConfigurationHandler()
            self.cacheDirectory = configurationHandler.get("cache_dir")
            self.cache = (self.cacheDirectory + "/" + filename.replace("/", "_")).replace("//", "/")

            self._filename = filename.replace("//", "/")

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing EOSReader\n{str(error)}")

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, value: str):
        if not value.replace("//", "/").startswith("/eos/"):
            raise ValueError(f"{value} is not an EOS path")
        self._filename = value.replace("//", "/")

    @runWithRetries(tries=5, wait=2, default={})
    def read(self) -> dict:
        """
        The function to read the content in the EOS file
        :return: file content
        """
        try:
            with open(self.filename, "r") as file:
                content = file.read()
            return json.loads(content)

        except Exception as error:
            self.logger.error("Failed to read EOS file: %s", str(error))
            response = os.system(f"env EOS_MGM_URL=root://eoscms.cern.ch eos cp {self.filename} {self.cache}")
            if response == 0:
                with open(self.cache, "r") as file:
                    content = file.read()
                return json.loads(content)

        raise Exception(f"Not able to read {self.filename} in EOS")
