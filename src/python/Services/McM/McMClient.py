import os
import json
import pycurl
from pycurl import Curl
from http.client import HTTPConnection, HTTPSConnection
from io import BytesIO
from logging import Logger

from Utilities.Authenticate import getX509Conn
from logging import getLogger  # from Utilities.Logging import getLogger

from typing import Optional, Union


class McMClient(object):
    """
    _McMClient_
    General API for reading/storing data in McM
    """

    def __init__(self, logger: Optional[Logger] = None, **kwargs) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.devMode = os.getenv("UNIFIED_MCM") == "dev" or kwargs.get("dev") or False
            self.intMode = kwargs.get("int") or False

            self.url = f"cms-pdmv{'-dev' if self.devMode else '-int' if self.intMode else ''}.cern.ch/mcm/"
            self.id = kwargs.get("id") or "sso"
            self.cookie = kwargs.get("cookie")

            self.response = BytesIO()
            self.connection = self._getConnection()

        except Exception as error:
            raise Exception(f"Error initializing McMClient\n{str(error)}")

    def _getConnection(self) -> Union[HTTPConnection, HTTPSConnection, Curl]:
        """
        The function to set the connection to McM
        :return: curl if id is sso, http connection o/w
        """
        if self.id == "sso":
            self.cookie = self._getSSOCookie()
            self.logger.info(f"Using sso cookie file: {self.cookie}")

            curl = pycurl.Curl()
            curl.setopt(pycurl.COOKIEFILE, self.cookie)
            curl.setopt(pycurl.SSL_VERIFYPEER, 1)
            curl.setopt(pycurl.SSL_VERIFYHOST, 2)
            curl.setopt(pycurl.CAPATH, "/etc/pki/tls/certs")
            curl.setopt(pycurl.WRITEFUNCTION, self.response.write)
            return curl

        return getX509Conn(self.url) if self.id == "cert" else HTTPConnection(self.url)

    def _getSSOCookie(self) -> str:
        """
        The function to get the sso cookie file
        :return: sso cookie file name
        """
        cookie = self.cookie or os.environ.get("MCM_SSO_COOKIE")
        if cookie is None:
            cookie = f"{os.getenv('HOME')}/private/{'dev' if self.devMode else 'int' if self.intMode else 'prod'}-cookie.txt"

        if not os.path.isfile(cookie):
            self.logger.info("The required sso cookie file does not exist. Trying to make one")
            os.system(f"cern-get-sso-cookie -u https://{self.url} -o {cookie} --krb")
            if not os.path.isfile(cookie):
                raise ValueError("The required sso cookie file cannot be made")

        return cookie

    def _getResponse(self) -> dict:
        if self.id == "sso":
            response = self.response.getvalue()
            self.response = BytesIO()
            self.connection.setopt(pycurl.WRITEFUNCTION, self.response.write)

        else:
            response = self.connection.getresponse().read()

        return json.loads(response)

    def search(self, name: str, page: int = -1, query: str = "") -> Optional[dict]:
        """
        The function to search data
        :param name: db name
        :param page: page number
        :param query: query params
        :return: response, if any
        """
        try:
            data = self.get(f"search/?db_name={name}&page={page}&{query}") or {}
            return data.get("results")

        except Exception as error:
            self.logger.error("Failed to search %s", name)
            self.logger.error(str(error))

    def get(self, endpoint: str) -> dict:
        """
        The function to get data from a given McM endpoint
        :param endpoint: endpoint name
        :return: response
        """
        try:
            if self.id == "sso":
                self.connection.setopt(pycurl.HTTPGET, 1)
                self.connection.setopt(pycurl.URL, "https://" + f"{self.url}{endpoint}".replace("//", "/"))
                self.connection.perform()

            else:
                self.connection.request("GET", endpoint, headers={})

            return self._getResponse()

        except Exception as error:
            self.logger.error("Failed to get data from %s", endpoint)
            self.logger.error(str(error))

    def set(self, endpoint: str, data: dict) -> dict:
        """
        The function to set data to a given McM endpoint
        :param endpoint: endpoint name
        :param data: data dict
        :return: response
        """
        try:
            data = json.dumps(data)

            if self.id == "sso":
                self.connection.setopt(pycurl.URL, "https://" + f"{self.url}{endpoint}".replace("//", "/"))
                self.connection.setopt(pycurl.UPLOAD, 1)
                self.connection.setopt(pycurl.READFUNCTION, BytesIO(data).read)
                self.connection.perform()

            else:
                self.connection.request("PUT", endpoint, data, headers={})

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
            if self.id == "sso":
                self.connection.setopt(pycurl.CUSTOMREQUEST, "DELETE")
                self.connection.setopt(pycurl.URL, "https://" + f"{self.url}{endpoint}".replace("//", "/"))
                self.connection.perform()

            else:
                self.connection.request("DELETE", endpoint, headers={})

            return self._getResponse()

        except Exception as error:
            self.logger.error("Failed to clean data in %s", endpoint)
            self.logger.error(str(error))
