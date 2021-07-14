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

# Get necessary parameters
configurationHandler = ConfigurationHandler()
reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))


def getResponse(url, endpoint, param="", headers=None):

    if headers == None:
        headers = {"Accept": "application/json"}

    if type(param) == dict:
        _param = "&".join(["=".join([k, v]) for k, v in param.items()])
        param = "?" + _param

    try:
        conn = getX509Conn(url)
        request = conn.request("GET", endpoint + param, headers=headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        return data
    except Exception as e:
        print("Failed to get response from %s" % url + endpoint + param)
        print(str(e))
