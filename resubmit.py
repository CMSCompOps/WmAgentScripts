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
import reqMgrClient
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"


"""
Creates the cloned specs for the original request
Updates parameters
"""
def retrieveSchema(workflowName, user, group ):
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    helper.load(specURL)
    schema = {}
    
    # Add AcquisitionEra, ProcessingString and increase ProcessingVersion by 1
    schema["ProcessingString"] = helper.getProcessingString()
    schema["ProcessingVersion"] = helper.getProcessingVersion()+1
    schema["AcquisitionEra"] = helper.getAcquisitionEra()
	
    for (key, value) in helper.data.request.schema.dictionary_().iteritems():
		if key == 'ProcConfigCacheID':
			schema['ConfigCacheID'] = value
		elif key=='RequestSizeEvents':
			schema['RequestSizeEvents'] = value
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
    	schema['LumisPerJob'] = 300
    if 'EventsPerJob' not in schema and schema['RequestType']=='MonteCarlo':
    	schema['EventsPerJob'] = 120000
	
    return schema

def submitWorkflow(schema):
	"""
	This submits a workflow into the ReqMgr
	"""
	jsonEncodedParams = {}
	for paramKey in schema.keys():
		jsonEncodedParams[paramKey] = json.dumps(schema[paramKey])
	encodedParams = urllib.urlencode(jsonEncodedParams, False)
	
	headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}
	conn  =  httplib.HTTPSConnection("cmsweb.cern.ch", cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	conn.request("POST",  "/reqmgr/create/makeSchema", encodedParams, headers)
	response = conn.getresponse()
	print "Response status: %s, Response reason: %s" % (str(response.status), response.reason)
	
	# This print out where the resource can be found (url)
	data = response.read()
	print data
	
	details = re.search("details\/(.*)\'",data)
	newWorkflow = details.group(1)
	return newWorkflow

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
    
    # Get info about the workflow to be cloned
    schema = retrieveSchema(sys.argv[1], user, group)
    
    # Sumbit cloned workflow to ReqMgr
    newWorkflow = submitWorkflow(schema)
    print 'Cloned workflow: '+newWorkflow
    
    # Move the request to Assignment-approved
    data = reqMgrClient.setWorkflowApproved('cmsweb.cern.ch', newWorkflow)
    print 'Approve request response:'
    print data
    
    # Set the priority of the cloned workflow
    newPriority = reqMgrClient.getWorkflowPriority('cmsweb.cern.ch',sys.argv[1])
    if  newPriority>2: 
	changePriorityWorkflow.changePriorityWorkflow('cmsweb.cern.ch', newWorkflow, newPriority)
    
    sys.exit(0)
