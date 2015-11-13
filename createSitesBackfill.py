#!/usr/bin/env python
"""
Creates and assigns several backfill workflows for sites on a given lists.
This is useful for testing site stability and debug workflow-related
issues.
Usage:
    python createSitesBackfill.py [options] JSON_FILE SITE1,SITE2,...
    JSON_FILE: the json file with the request to
 
"""
import json, sys
from optparse import OptionParser
from reqmgr import ReqMgrClient

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

    url = 'https://cmsweb.cern.ch'
    testbed_url = 'https://cmsweb-testbed.cern.ch'
    #url = 'https://alan-cloud1.cern.ch'

    #Create option parser
    usage = "usage: %prog [options] JSON_FILE SITE1,SITE2,..."
    parser = OptionParser(usage=usage)

    parser.add_option("-t","--team", dest="team", default=None,
                        help="Team for assigning, if empty, the one on the Json file")
    parser.add_option("--testbed",action="store_true", dest="testbed", default=False,
                        help="Use testbed ReqMgr instead of production ReqMgr.")
    parser.add_option("-m", "--multi",action="store_true", dest="multi", default=False,
                        help="Assign to all sites")

    (options, args) = parser.parse_args()
    
    if len(args) != 2:
        parser.error("Provide the JSON file and the site list")
        sys.exit(1)
    
    #the input options
    jsonFile = args[0]
    siteList = args[1].split(',')
    if options.testbed:
        url = testbed_url

    #read request params and wrap
    configJson = json.load(open(jsonFile, 'r'))
    config = Config(configJson)
    reqMgrClient = ReqMgrClient(url, config)

    #set up common stuff
    config.requestArgs["createRequest"]["Campaign"] += "SiteBackfill" 
    reqStr = config.requestArgs["createRequest"]["RequestString"]
    
    config.requestArgs["assignRequest"]["AcquisitionEra"] += "SiteBackfill"
    config.requestArgs["createRequest"]["Campaign"] += "SiteBackfill" 
    
    if options.team:
        config.requestArgs["assignRequest"]["Team"] = options.team

    #create a request for each site
    if not options.multi:
        for site in siteList:
            
            #setup creation stuff
            config.requestArgs["createRequest"]["RequestString"] = reqStr+"-"+site+"Backfill"
            config.requestArgs["createRequest"]["PrepID"] = None

            r = reqMgrClient.createRequest(config)
            print "Created:"
            print r
            reqMgrClient.changeSplitting(config)
            print "Changed splitting"
            
            #change assignment stuff
            config.requestArgs["assignRequest"]["SiteWhitelist"] = [site]        
            config.requestArgs["assignRequest"]["ProcessingString"] = "SITE_TEST_"+site
            #assign
            reqMgrClient.assignRequests(config)
            print "Assigned to",site
    
    else:
        #setup creation stuff
        config.requestArgs["createRequest"]["RequestString"] = reqStr+"-SiteBackfill"
        config.requestArgs["createRequest"]["PrepID"] = None

        r = reqMgrClient.createRequest(config)
        print "Created:"
        print r
        reqMgrClient.changeSplitting(config)
        print "Changed splitting"
        
        #change assignment stuff
        config.requestArgs["assignRequest"]["SiteWhitelist"] = siteList
        config.requestArgs["assignRequest"]["ProcessingString"] = "SITE_TEST"
        #assign
        reqMgrClient.assignRequests(config)
        print "Assigned to",siteList

if __name__ == '__main__':
    main()

