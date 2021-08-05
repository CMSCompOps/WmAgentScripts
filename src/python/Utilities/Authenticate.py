"""
File       : Authenticate.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: Module which takes care of all authentication to different services
"""

import os
import http.client
from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Decorators import runWithRetries

# Get necessary parameters
configurationHandler = ConfigurationHandler()
reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))


@runWithRetries(tries=5, wait=0, default=False)
def getX509Conn(url=reqmgrUrl):
    """
    The function to get the X509 http connection
    :param url: url
    :return: http connection
    """
    conn = http.client.HTTPSConnection(
        url,
        cert_file=os.getenv("X509_USER_PROXY"),
        key_file=os.getenv("X509_USER_PROXY"),
    )
    return conn
