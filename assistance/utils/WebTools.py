"""
File       : WebTools.py
Author     : Hasan Ozturk <haozturk AT cern dot com>

Description: Class which contains helper functions for web interaction

"""

import os
import httplib
from UnifiedConfiguration import UnifiedConfiguration

# Get necessary parameters
SC = UnifiedConfiguration('serviceConfiguration.json')
reqmgr_url = os.getenv('UNIFIED_REQMGR', SC.get('reqmgr_url'))

def getX509Conn(url=reqmgr_url,max_try=5):
    tries = 0
    while tries<max_try:
        try:
            conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            return conn
        except:
            tries+=1
            pass
    return None

def getResponse(url, endpoint, param='', headers=None):

    if headers == None:
        headers = {"Accept":"application/json"}

    try:
        conn = getX509Conn(url)
        request= conn.request("GET",endpoint+param,headers=headers)
        response=conn.getresponse()
        data = json.loads(response.read())
        return data
    except Exception as e:
        print "Failed to get response from %s" % url+endpoint+param
        print str(e)
