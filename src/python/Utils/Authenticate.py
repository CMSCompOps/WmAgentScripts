"""
File       : Authenticate.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: Module which takes care of all authentication to different services
"""

import os
import http.client
import json
from Utils.ConfigurationHandler import ConfigurationHandler

# Get necessary parameters
configurationHandler = ConfigurationHandler()
reqmgrUrl = os.getenv('REQMGR_URL', configurationHandler.get('reqmgr_url'))

def getX509Conn(url=reqmgrUrl,max_try=5):
    tries = 0
    while tries<max_try:
        try:
            conn = http.client.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            return conn
        except:
            tries+=1
            pass
    return None
