import os
import json
import pycurl
from io import BytesIO
from logging import Logger
#from logging import getLogger
import logging
from Utilities.Logging import getLogger

from typing import Optional


class McMClient(object):
    """
    _McMClient_
    General API for reading/storing data in McM
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__()
            logging.basicConfig(level=logging.INFO)
            self.logger = logger or getLogger(self.__class__.__name__)

            self.response = BytesIO()
            self.url = f"cms-pdmv.cern.ch/mcm/"

            self._setCookie(kwargs.get("cookie"))
            self._setConnection()

        except Exception as error:
            raise Exception(f"Error initializing McMClient\n{str(error)}")

    def _setCookie(self, cookie: Optional[str] = None) -> None:
        """
        The function to set the sso cookie file
        :param cookie: optional cookie filename
        """
        self.cookie = cookie or os.environ.get("MCM_SSO_COOKIE")
        if self.cookie is None:
            raise Exception(f"There is no McM client cookie, make sure that you ran /src/bash/authenticate.sh")

        self.logger.info(f"Using sso cookie file: {self.cookie}")

    def _setConnection(self) -> None:
        """
        The function to set the connection to McM
        """
        self.connection = pycurl.Curl()
        self.connection.setopt(pycurl.COOKIEFILE, self.cookie)
        self.connection.setopt(pycurl.SSL_VERIFYPEER, 1)
        self.connection.setopt(pycurl.SSL_VERIFYHOST, 2)
        self.connection.setopt(pycurl.CAPATH, "/etc/pki/tls/certs")
        self.connection.setopt(pycurl.WRITEFUNCTION, self.response.write)

    def _getResponse(self) -> dict:
        """
        The function to get the response from request
        :return: response
        """
        response = self.response.getvalue()
        self.response = BytesIO()
        self.connection.setopt(pycurl.WRITEFUNCTION, self.response.write)

        return json.loads(response)

    def search(self, name: str, page: int = -1, query: str = "") -> Optional[dict]:
        """
        The function to search data
        :param name: db name
        :param page: page number
        :param query: optional query params
        :return: response, if any
        """
        return (self.get(f"search/?db_name={name}&page={page}&{query}") or {}).get("results")

    def get(self, endpoint: str) -> dict:
        """
        The function to get data from a given McM endpoint
        :param endpoint: endpoint name
        :return: response
        """
        try:
            self.connection.setopt(pycurl.HTTPGET, 1)
            self.connection.setopt(pycurl.URL, "https://" + f"{self.url}{endpoint}".replace("//", "/"))
            self.connection.perform()

            return self._getResponse()

        except Exception as error:
            self.logger.error("Failed to get data from %s", f"{self.url}{endpoint}")
            self.logger.error(str(error))

    def set(self, endpoint: str, data: dict) -> dict:
        """
        The function to set data to a given McM endpoint
        :param endpoint: endpoint name
        :param data: data dict
        :return: response
        """
        try:
            self.connection.setopt(pycurl.URL, "https://" + f"{self.url}{endpoint}".replace("//", "/"))
            self.connection.setopt(pycurl.UPLOAD, 1)
            self.connection.setopt(pycurl.READFUNCTION, BytesIO(json.dumps(data).encode()).read)
            self.connection.perform()

            return self._getResponse()

        except Exception as error:
            self.logger.error("Failed to set data in %s", endpoint)
            self.logger.error(str(error))

    def clean(self, endpoint: str) -> dict:
        """
        The function to clean all data in a given McM endpoint
        :param endpoint: endpoint name
        :return: response
        """
        try:
            self.connection.setopt(pycurl.CUSTOMREQUEST, "DELETE")
            self.connection.setopt(pycurl.URL, "https://" + f"{self.url}{endpoint}".replace("//", "/"))
            self.connection.perform()

            return self._getResponse()

        except Exception as error:
            self.logger.error("Failed to clean data in %s", endpoint)
            self.logger.error(str(error))
