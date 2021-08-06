"""
File       : Authenticate.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: Useful functions while interacting different services
"""

import os
import json

from typing import Dict, Optional, Union

from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Authenticate import getX509Conn

# Get necessary parameters
configurationHandler = ConfigurationHandler()
reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))


def getResponse(url: str, endpoint: str, param: Union[str, dict] = "", headers: Optional[dict] = None) -> Any:
    """
    The function to get the response for a given request
    :param url: url
    :param endpoint: endpoint
    :param param: optional request params
    :param headers: optional request headers
    :return: request response
    """

    if headers == None:
        headers = {"Accept": "application/json"}

    if isinstance(param, dict):
        _param = []
        for k, v in param.items():
            if isinstance(v, str):
                _param += ["=".join([k, v])]
            elif isinstance(v, list):
                _param += ["=".join([k, vi]) for vi in v]
        param = "?" + "&".join(_param)

    try:
        conn = getX509Conn(url)
        _ = conn.request("GET", endpoint + param, headers=headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        return data

    except Exception as error:
        print(f"Failed to get response from {url + endpoint + param}\n{str(error)}")


def sendResponse(url: str, endpoint: str, param: Union[str, dict] = "", headers: Optional[dict] = None) -> dict:
    """
    The function to send data to a given url
    :param url: request url
    :param endpoint: request endpoint
    :param param: data params
    :param headers: request headers
    :return: request response
    """

    if headers is None:
        headers = {"Accept": "application/json", "Content-type": "application/json", "Host": "cmsweb.cern.ch"}

    if isinstance(param, dict):
        param = json.dumps(param)

    try:
        conn = getX509Conn(url)
        _ = conn.request("PUT", endpoint, param, headers=headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        return data

    except Exception as error:
        print(f"Failed to send response to {url + endpoint + param}\n{str(error)}")
