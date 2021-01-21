#!/usr/bin/env python

import os
import sys
import urllib
import httplib
import unprocessedBlocks
import changeSplittingWorkflow

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from assignWorkflow import assignRequest
from reqMgrClient import retrieveSchema, submitWorkflow, Workflow

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
#reqmgrHostname = "vocms144"
#reqmgrPort = 8687

def approveRequest(url,workflow):
    params = {"requestName": workflow,
              "status": "assignment-approved"}

    encodedParams = urllib.urlencode(params)
    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #conn  =  httplib.HTTPConnection(url)
    conn.request("PUT",  "/reqmgr/reqMgr/request", encodedParams, headers)
    response = conn.getresponse()
    if response.status != 200:
        print 'could not approve request with following parameters:'
        for item in params.keys():
            print item + ": " + str(params[item])
        print 'Response from http call:'
        print 'Status:',response.status,'Reason:',response.reason
        print 'Explanation:'
        data = response.read()
        print data
        print "Exiting!"
        sys.exit(1)
    conn.close()
    return

def modifySchema(helper, user, group, oldworkflow):
    #for (key, value) in helper.data.request.schema.dictionary_whole_tree_().iteritems():
    for (key, value) in helper.data.request.schema.dictionary_().iteritems():
        #print key, value
        if key == 'ProcConfigCacheID':
            schema['ConfigCacheID'] = value
        elif key=='RequestSizeEvents':
            schema['RequestSizeEvents'] = value
            #schema['RequestNumEvents'] = int(value)
        elif key=='Requestor':
            schema['Requestor']=user
        elif key=='Group':
            schema['Group']=group
        elif key=='BlockWhitelist':
            schema['BlockWhitelist']=unprocessedBlocks.getListUnprocessedBlocks(url, oldworkflow)
        elif value != None:
                schema[key] = value
    return schema

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Usage:"
        print "  ./resubmit WORKFLOW_NAME USER GROUP"
        sys.exit(0)
    oldworkflow=sys.argv[1]
    user=sys.argv[2]
    group=sys.argv[3]
    url='cmsweb.cern.ch'    
    #print "Going to attempt to resubmit %s..." % sys.argv[1]
    wfInfo = Workflow(oldworkflow)
    helper = retrieveSchema(oldworkflow)
    schema = modifySchema(helper, user, group, oldworkflow)
    schema['OriginalRequestName'] = oldworkflow
    #print schema
    newWorkflow = submitWorkflow(url, schema)
    approveRequest(url,newWorkflow)
    print 'Cloned workflow:',newWorkflow
    
    team = wfInfo.info["team"]
    if 'teams' in wfInfo.info:
        site = wfInfo.info['Site Whitelist']
    activity = "reprocessing"
    era = wfInfo.info["AcquisitionEra"]
    procversion = wfInfo.info["ProcessingVersion"]
    procstring = wfInfo.info["ProcessingString"]
    lfn = wfInfo.info["MergedLFNBase"]
    maxmergeevents = 50000
    if 'Fall11_R1' in oldworkflow:
        maxmergeevents = 6000
    if 'DR61SLHCx' in oldworkflow:
        maxmergeevents = 5000
    changeSplittingWorkflow.changeSplittingWorkflow(url, newWorkflow, 1)
    #assignWorkflow.assignRequest(url , newWorkflow ,team ,site ,era, procversion, procstring, activity, lfn, maxmergeevents, 2300000, 4100000000, 0, [])
    assignRequest(url, newWorkflow, team, site, era, procversion, activity, lfn, procstring)
    sys.exit(0)
