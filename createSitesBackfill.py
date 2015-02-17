#!/usr/bin/env python
"""
Creates and assigns several backfill workflows for sites on a given lists.
This is useful for testing site stability and debug workflow-related
issues.
Usage:
    python createSitesBackfill.py JSON_FILE SITE1,SITE2,...
    JSON_FILE: the json file with the request to
 
"""

import reqmgr
import json
from reqmgr import ReqMgrClient
import sys


class Config:
    def __init__(self, info):
        self.requestArgs = info
        self.requestNames = []
        self.cert = None
        self.key = None
        self.assignRequests = True
        self.changeSplitting = True
        self.assignRequest = True


def main():
    #url = 'cmsweb.cern.ch'
    url = 'https://cmsweb-testbed.cern.ch'
    if len(sys.argv) != 3:
        print "Usage:  python createSitesBackfill.py JSON_FILE SITE1,SITE2,..."
        sys.exit(0)
    jsonFile = sys.argv[1]
    siteList = sys.argv[2].split(',')
    #read request params
    configJson = json.load(open(jsonFile, 'r'))
    #wrap config in an object
    config = Config(configJson)
    
    reqMgrClient = ReqMgrClient(url, config)

    #set up common stuff
    config.requestArgs["createRequest"]["Campaign"] += "SiteBackfill" 
    reqStr = config.requestArgs["createRequest"]["RequestString"]
    
    config.requestArgs["assignRequest"]["AcquisitionEra"] += "SiteBackfill"
    config.requestArgs["createRequest"]["Campaign"] += "SiteBackfill" 


    #create a request for each site
    for site in siteList:
        
        #setup creation stuff
        config.requestArgs["createRequest"]["RequestString"] = reqStr+"-"+site
        config.requestArgs["createRequest"]["PrepID"] = None

        r = reqMgrClient.createRequest(config)
        print "Created:", r
        reqMgrClient.changeSplitting(config)
        print "Changed splitting"
        
        #change assignment stuff
        config.requestArgs["assignRequest"]["SiteWhitelist"] = [site]        
        config.requestArgs["assignRequest"]["ProcessingString"] = "SITE_TEST_"+site
        #assign
        reqMgrClient.assignRequests(config)
        print "Assigned: ", r ,"to",site
    


if __name__ == '__main__':
    main()

