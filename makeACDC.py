#!/usr/bin/env python
"""
    Simple creating of acdc's
    Use: The workflow name and the initial task name.
    It will copy all the original workflow parameters unless specified
"""

import reqmgr
import json
from reqmgr import ReqMgrClient
import reqMgrClient as rqMgr
import sys
from optparse import OptionParser

class Config:
    def __init__(self, info):
        self.requestArgs = info
        self.requestNames = []
        self.cert = None
        self.key = None
        self.assignRequests = False
        self.changeSplitting = False
        self.assignRequest = False


def main():

    url = 'https://cmsweb.cern.ch'
    testbed_url = 'https://cmsweb-testbed.cern.ch'
    #url = 'https://alan-cloud1.cern.ch'

    #Create option parser
    usage = "usage: %prog [options] WORKFLOW TASK"
    parser = OptionParser(usage=usage)
    parser.add_option("-f","--file", dest="file", default=None,
                        help="Text file of a list of workflows")
    parser.add_option("-m","--memory", dest="memory", default=None,
                        help="Memory to override the original request memory")

    (options, args) = parser.parse_args()
    
    if len(args) != 2:
        parser.error("Provide the JSON file and the site list")
        sys.exit(1)
    
    #the input options
    workflow = args[0]
    task = args[1]
    #original wf info
    wf = rqMgr.Workflow(workflow)    
    
    #read request params and wrap
    configJson = {"createRequest":{}}
    config = Config(configJson)
    reqMgrClient = ReqMgrClient(url, config)

    #set up acdc stuff
    config.requestArgs["createRequest"]["RequestString"] = "ACDC_"+wf.info["RequestString"]
    config.requestArgs["createRequest"]["PrepID"] = wf.info["PrepID"]
    config.requestArgs["createRequest"]["RequestPriority"] = wf.info["RequestPriority"]
    config.requestArgs["createRequest"]["OriginalRequestName"] = wf.name
    config.requestArgs["createRequest"]["InitialTaskPath"] = "/%s/%s"%(wf.name, task)
    config.requestArgs["createRequest"]["ACDCServer"] = "https://cmsweb.cern.ch/couchdb"
    config.requestArgs["createRequest"]["ACDCDatabase"] = "acdcserver"
    config.requestArgs["createRequest"]["TimePerEvent"] = wf.info["TimePerEvent"]

    if options.memory:
        config.requestArgs["createRequest"]["Memory"] = options.memory
    else:
        config.requestArgs["createRequest"]["Memory"] = wf.info["Memory"]

    config.requestArgs["createRequest"]["SizePerEvent"] = wf.info["SizePerEvent"]
    config.requestArgs["createRequest"]["RequestType"] = "Resubmission"
    config.requestArgs["createRequest"]["Group"] = wf.info["Group"]

    r = reqMgrClient.createRequest(config)
    print "Created:"    
    print r

if __name__ == '__main__':
    main()

