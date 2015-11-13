#!/usr/bin/env python

import os
import sys
import urllib
import httplib
import re
from deprecated import Priorities
import json
import changePriorityWorkflow
import closeOutWorkflows


from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
#reqmgrHostname = "vocms144"
#reqmgrPort = 8687

def getPriorityWorkflow(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'RequestPriority' in request:
		return request['RequestPriority']
	else:	
		return 0

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



def retrieveSchema(workflowName, user, group ):
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    #print "  retrieving original workflow...",
    helper.load(specURL)
    #print "done."
    schema = {}
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
	elif key=='ProdJobSplitArgs':
 	    schema['ProdJobSplitArgs']={'events_per_job': 600000, 'events_per_lumi': 300, 'lheInputFiles': True}
	elif value != None:
            schema[key] = value
    return schema

def submitWorkflow(schema):
    for schemaListItem in ["RunWhitelist", "RunBlacklist", "BlockWhitelist",
                           "BlockBlacklist"]:
        if schemaListItem in schema.keys():
            schema[schemaListItem] = str(schema[schemaListItem])
    jsonEncodedParams = {}
    for paramKey in schema.keys():
		jsonEncodedParams[paramKey] = json.dumps(schema[paramKey])
    encodedParams = urllib.urlencode(jsonEncodedParams, False)

    #encodedParams = urllib.urlencode(schema, True)
    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection("cmsweb.cern.ch", cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("POST",  "/reqmgr/create/makeSchema", encodedParams, headers)
    response = conn.getresponse()
    print response.status, response.reason
    data = response.read()
    print data
    details=re.search("details\/(.*)\'",data)
    return details.group(1)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Usage:"
        print "  ./resubmit WORKFLOW_NAME USER GROUP"
        sys.exit(0)
    user=sys.argv[2]
    group=sys.argv[3]	
    #print "Going to attempt to resubmit %s..." % sys.argv[1]
    schema = retrieveSchema(sys.argv[1], user, group)
    #print schema
    newWorkflow=submitWorkflow(schema)
    approveRequest('cmsweb.cern.ch',newWorkflow)
    print 'Cloned workflow:',newWorkflow
    newPriority=getPriorityWorkflow('cmsweb.cern.ch',sys.argv[1])
    if  newPriority>2: 
	changePriorityWorkflow.changePriorityWorkflow('cmsweb.cern.ch', newWorkflow, newPriority)
    sys.exit(0)
