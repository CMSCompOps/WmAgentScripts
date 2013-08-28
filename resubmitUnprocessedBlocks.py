#!/usr/bin/env python

import os
import sys
import urllib
import httplib
import re
import Priorities
import json
import changePriorityWorkflow
import closeOutWorkflows
import unprocessedBlocks
import assignWorkflowsAuto
import changeSplittingWorkflow

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
#reqmgrHostname = "vocms144"
#reqmgrPort = 8687

def getSiteWhitelist(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'teams' in request:
		return request['Site Whitelist']
	else:	
		return []

def getDashboardActivity(url, workflow):
	return "reprocessing"

def getProcessingString(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'teams' in request:
		return request['ProcessingString']
	else:	
		return None

def getProcessingVersion(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'teams' in request:
		return request['ProcessingVersion']
	else:	
		return None

def getAcquisitonEra(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'teams' in request:
		return request['AcquisitionEra']
	else:	
		return None

def getMergedLFNBaseWorkflow(url,workflow):
 	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'teams' in request:
		return request['MergedLFNBase']
	else:	
		return None


def getTeamWorkflow(url,workflow):
 	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'teams' in request:
		return request['teams'][0]
	else:	
		return None

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
	elif key=='BlockWhitelist':
	    schema['BlockWhitelist']=unprocessedBlocks.getListUnprocessedBlocks(url, workflowName)
	elif value != None:
            schema[key] = value
    return schema

def submitWorkflow(url, schema):
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
    oldworkflow=sys.argv[1]
    user=sys.argv[2]
    group=sys.argv[3]
    url='cmsweb.cern.ch'	
    #print "Going to attempt to resubmit %s..." % sys.argv[1]
    schema = retrieveSchema(oldworkflow, user, group)
    #print schema
    newWorkflow=submitWorkflow(url, schema)
    approveRequest(url,newWorkflow)
    print 'Cloned workflow:',newWorkflow
    team=getTeamWorkflow(url,oldworkflow)
    site=getSiteWhitelist(url, oldworkflow)
    activity=getDashboardActivity(url, oldworkflow)
    era=getAcquisitonEra(url, oldworkflow)
    procversion=getProcessingVersion(url, oldworkflow)
    procstring=getProcessingString(url, oldworkflow)
    lfn=getMergedLFNBaseWorkflow(url, oldworkflow)
    maxmergeevents = 50000
    if 'Fall11_R1' in oldworkflow:
    	maxmergeevents = 6000
    if 'DR61SLHCx' in oldworkflow:
        maxmergeevents = 5000
    changeSplittingWorkflow.changeSplittingWorkflow(url, newWorkflow, 1)
    assignWorkflowsAuto.assignRequest(url , newWorkflow ,team ,site ,era, procversion, procstring, activity, lfn, maxmergeevents, 2300000, 4100000000, 0, [])
    sys.exit(0)
