#!/usr/bin/env python

"""
This script clones a given workflow
*args must be: workflow_name user group
"""
import os
import sys
import urllib
import httplib
import re
import json
import changePriorityWorkflow
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
#reqmgrHostname = "vocms144"
#reqmgrPort = 8687

"""
Get the priority of a given workflow
This is intended to retrieve the priority of the workflow to be cloned
"""
def getPriorityWorkflow(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'RequestPriority' in request:
		return request['RequestPriority']
	else:	
		return 0
"""
Move the given workflow to Assignment-approve
"""
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

"""
Retrieves the specs for the workflow to be cloned
"""
def retrieveSchema(workflowName, user, group ):
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    #print "  retrieving original workflow...",
    helper.load(specURL)
    #print "done."
    schema = {}
    
    # Add AcquisitionEra, ProcessingString and increase ProcessingVersion by 1
    schema["ProcessingString"] = helper.getProcessingString()
    schema["ProcessingVersion"] = helper.getProcessingVersion()+1
    schema["AcquisitionEra"] = helper.getAcquisitionEra()
	
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
#		elif key=='SizePerEvent':
#			schema['SizePerEvent']=1
		elif key in ["RunWhitelist", "RunBlacklist", "BlockWhitelist", "BlockBlacklist"] and not value:
			schema[key]=[]
		elif not value:
			continue
		elif value != None:
			schema[key] = value
    if 'LumisPerJob' not in schema and schema['RequestType']=='MonteCarlo':
    	schema['LumisPerJob']=300
    if 'EventsPerJob' not in schema and schema['RequestType']=='MonteCarlo':
    	schema['EventsPerJob']=120000
	
    return schema
   
"""
This submits a workflow into the ReqMgr
"""
def submitWorkflow(schema):
    for schemaListItem in ["RunWhitelist", "RunBlacklist", "BlockWhitelist",
                           "BlockBlacklist"]:
        if schemaListItem in schema.keys():
        	continue
            #schema[schemaListItem] = str(schema[schemaListItem])
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
    print "Response status: %s, Response reason: %s" % (str(response.status), response.reason) 
    data = response.read()
    # This print out where the resource can be found (url)
    print data
    details=re.search("details\/(.*)\'",data)
    return details.group(1)

"""
__Main__
"""
if __name__ == "__main__":
    # Check the arguements, get info from them
    if len(sys.argv) != 4:
        print "Usage:"
        print "  ./resubmit WORKFLOW_NAME USER GROUP"
        sys.exit(0)
    user=sys.argv[2]
    group=sys.argv[3]	
    #print "Going to attempt to resubmit %s..." % sys.argv[1]
    
    # Get info about the workflow to be cloned
    schema = retrieveSchema(sys.argv[1], user, group)
    #print schema
    
    # Sumbit cloned workflow to ReqMgr and move it to Assignment-approve
    newWorkflow=submitWorkflow(schema)
    approveRequest('cmsweb.cern.ch',newWorkflow)
    print 'Cloned workflow: '+newWorkflow
    
    # Set the priority of the cloned workflow
    newPriority=getPriorityWorkflow('cmsweb.cern.ch',sys.argv[1])
    if  newPriority>2: 
	changePriorityWorkflow.changePriorityWorkflow('cmsweb.cern.ch', newWorkflow, newPriority)
    
    sys.exit(0)
