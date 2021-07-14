"""
File       : Authenticate.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: Useful functions while interacting different services
"""

import os
import http.client
import json
from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Authenticate import getX509Conn

from typing import Union, Optional, Dict

# Get necessary parameters
configurationHandler = ConfigurationHandler()
reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))


def getResponse(
    url: str,
    endpoint: str,
    param: Union[str, dict] = "",
    headers: Optional[dict] = None,
):

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
        request = conn.request("GET", endpoint + param, headers=headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        return data

    except Exception as e:
        print(f"Failed to get response from {url + endpoint + param}")
        print(str(e))
