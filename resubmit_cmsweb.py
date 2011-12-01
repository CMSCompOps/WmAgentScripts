#!/usr/bin/env python

import os
import sys
import urllib
import httplib
import WMCore.Wrappers.JsonWrapper as json_wrap

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "http://vocms144.cern.ch:5984/reqmgrdb"
reqmgrHostname = "vocms144"
reqmgrPort = 8687

def retrieveSchema(workflowName):
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    print "  retrieving original workflow...",
    helper.load(specURL)
    print "done."
    schema = {}
    for (key, value) in helper.data.request.schema.dictionary_().iteritems():
        if value != None:
            schema[key] = value
    schema["Requestor"] = "linacre"
    schema["Group"] = "DATAOPS"
    del schema["RequestName"]
    del schema["CouchDBName"]
    del schema["CouchURL"]

    assign = {}
    assign["unmergedLFNBase"] = helper.data.properties.unmergedLFNBase
    assign["mergedLFNBase"] = helper.data.properties.mergedLFNBase
    assign["processingVersion"] = helper.data.properties.processingVersion
    assign["dashboardActivity"] = helper.data.properties.dashboardActivity
    assign["acquisitionEra"] = helper.data.properties.acquisitionEra

    topLevelTask = helper.getTopLevelTask()[0]
    assign["SiteWhitelist"] = topLevelTask.siteWhitelist()

    mergeTask = None
    for mergeTask in topLevelTask.childTaskIterator():
        if mergeTask.taskType() == "Merge":
            if mergeTask.getPathName().find("DQM") == -1:
                break
        
    assign["MinMergeSize"] = mergeTask.jobSplittingParameters()["min_merge_size"]
    assign["MaxMergeSize"] = mergeTask.jobSplittingParameters()["max_merge_size"]
    assign["MaxMergeEvents"] = mergeTask.jobSplittingParameters().get("max_merge_events", 50000)
    
    return (schema, assign)

def submitWorkflow(schema):
    for schemaListItem in ["RunWhitelist", "RunBlacklist", "BlockWhitelist",
                           "BlockBlacklist"]:
        if schemaListItem in schema.keys():
            schema[schemaListItem] = str(schema[schemaListItem])
            
    encodedParams = urllib.urlencode(schema, True)
    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection("cmsweb.cern.ch", cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    print "  submitting new workflow..."

    conn.request("POST",  "/reqmgr/create/makeSchema", encodedParams, headers)
    response = conn.getresponse()
    print response.status, response.reason

    data = response.read()
    conn.close()
    return data.split("'")[1][42:]

def assignRequest(workflow, site, era, procversion, mLFN, uLFN, minSize, maxSize, maxEvents, dashboard):
    params = {"action": "Assign",
              "Teamprocessing": "checked",
              "SiteWhitelist": site[0],
              "MergedLFNBase": mLFN,
              "UnmergedLFNBase": uLFN,
              "MinMergeSize": minSize,
              "MaxMergeSize": maxSize, 
              "MaxMergeEvents": maxEvents,
              "AcquisitionEra": era,
              "ProcessingVersion": procversion,
              "dashboard": dashboard,
              "maxRSS": 3000000000,
              "maxVSize": 3100000000,
              "checkbox"+workflow: "checked"}

    encodedParams = urllib.urlencode(params)

    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection("cmsweb.cern.ch", cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

    conn.request("POST",  "/reqmgr/assign/handleAssignmentPage", encodedParams, headers)
    response = conn.getresponse()
    if response.status != 200:
        print 'could not assign request with following parameters:'
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
    print 'Assigned workflow:',workflow,'to site:',site,'with processing version',procversion
    return

def abortRequest(requestName):
    conn = httplib.HTTPConnection("vocms144.cern.ch:8687")
    conn.request("GET", "/reqmgr/reqMgr/request?requestName=%s" % requestName)
    resp = conn.getresponse()
    status = json_wrap.loads(resp.read())["WMCore.RequestManager.DataStructs.Request.Request"]["RequestStatus"]

    print "Status: %s" % status

    if status == "acquired":
        os.system("curl -X PUT -d \"requestName=%s&status=running\" \"http://vocms144:8687/reqmgr/reqMgr/request\"" % requestName)
        os.system("curl -X PUT -d \"requestName=%s&status=aborted\" \"http://vocms144:8687/reqmgr/reqMgr/request\"" % requestName)
    else:
        os.system("curl -X PUT -d \"requestName=%s&status=rejected\" \"http://vocms144:8687/reqmgr/reqMgr/request\"" % requestName)
        
    return    
            
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage:"
        print "  ./resubmit WORKFLOW_NAME"
        sys.exit(0)

    print "Going to attempt to resubmit %s..." % sys.argv[1]
    (schema, assign) = retrieveSchema(sys.argv[1])
    newName = submitWorkflow(schema)
#   assignRequest(newName, assign["SiteWhitelist"], assign["acquisitionEra"], assign["processingVersion"], assign["mergedLFNBase"],
#                  assign["unmergedLFNBase"], assign["MinMergeSize"], assign["MaxMergeSize"], assign["MaxMergeEvents"], assign["dashboardActivity"])

    abortRequest(sys.argv[1])
    sys.exit(0)
