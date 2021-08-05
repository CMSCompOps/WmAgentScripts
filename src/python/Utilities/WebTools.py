"""
File       : Authenticate.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: Useful functions while interacting different services
"""

import os
import json
from Utilities.Authenticate import getX509Conn

from typing import Union, Optional, Any


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