"""
File       : Authenticate.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: Module which takes care of all authentication to different services
"""

import os
from http.client import HTTPSConnection

from Utilities.Decorators import runWithRetries


@runWithRetries(tries=5, wait=0, default=False)
def getX509Conn(url: str) -> HTTPSConnection:
    """
    The function to get the X509 http connection
    :param url: url
    :return: https connection
    """
    conn = HTTPSConnection(url, cert_file=os.getenv("X509_USER_PROXY"), key_file=os.getenv("X509_USER_PROXY"))
    return conn
