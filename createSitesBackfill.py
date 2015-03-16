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
    #url = 'https://cmsweb.cern.ch'
    url = 'https://cmsweb-testbed.cern.ch'
    #url = 'https://alan-cloud1.cern.ch'
    if len(sys.argv) < 3:
        print "Usage:  python createSitesBackfill.py JSON_FILE [-t TEAM] SITE1,SITE2,..."
        sys.exit(0)
    
    jsonFile = sys.argv[1]
    
    team = None
    i = 2
    if sys.argv[i] == '-t':
        i += 1
        team = sys.argv[i]
        i += 1
    siteList = sys.argv[i].split(',')

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
    
    if team:
        config.requestArgs["assignRequest"]["Team"] = team

    #create a request for each site
    for site in siteList:
        
        #setup creation stuff
        config.requestArgs["createRequest"]["RequestString"] = reqStr+"-"+site+"Backfill"
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

