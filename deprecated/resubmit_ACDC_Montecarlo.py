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
from deprecated import dbsTest
from deprecated import phedexSubscription
import pickle
import changeSplittingWorkflow
from deprecated import assignWorkflowsAuto
import resubmitUnprocessedBlocks

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.Database.CMSCouch import Database

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"


def getOriginalCustodial(url, requestname):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+requestname)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'SubscriptionInformation' in request:
		for dataset in request['SubscriptionInformation']:
		   return request['SubscriptionInformation'][dataset]['CustodialSites'][0]
	else:	
		return None



#Only works now for event splitting
def getSplitting(requestName):
	reqmgrUrl='https://cmsweb.cern.ch/reqmgr/reqMgr/'
	reqmgr = RequestManager(dict = {'endpoint' : reqmgrUrl})
	result = reqmgr.getRequest(requestName)
	workloadDB = Database(result['CouchWorkloadDBName'], result['CouchURL'])
	workloadPickle = workloadDB.getAttachment(requestName, 'spec')
	spec = pickle.loads(workloadPickle)
	workload = WMWorkloadHelper(spec)
	params = workload.getTopLevelTask()[0].jobSplittingParameters()
	algo = workload.getTopLevelTask()[0].jobSplittingAlgorithm()
	return params['events_per_job']
	


def getFinalRequestedNumEvents(url, workflow):
	outputDataSets=deprecated.phedexSubscription.outputdatasetsWorkflow(url, workflow)
	obtainedEvents=deprecated.dbsTest.getOutputEvents(url, workflow, outputDataSets[0])
	requestedEvents=deprecated.dbsTest.getInputEvents(url, workflow)
	return (requestedEvents-obtainedEvents)

def getMaxLumi(url, workflow):
	outputDataSets=deprecated.phedexSubscription.outputdatasetsWorkflow(url, workflow)
	dataset=outputDataSets[0]
	output=os.popen("./dbssql --input='find run, max(lumi) where dataset="+dataset+"'| awk '{print $2}' | grep '[0-9]\{1,\}'").read()
	try:
		return int(output)
	except ValueError:
        	return -1

def getPriorityWorkflow(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.read(r2.read())
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



def retrieveSchema(url, workflowName, user, group ):
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
	elif key=='RequestNumEvents':
  	    schema['RequestNumEvents']=getFinalRequestedNumEvents(url, workflowName)
        elif key=='FirstLumi':
	   schema['FirstLumi']=getMaxLumi(url, workflowName)*2
        elif key=='FirstEvent':
	   schema['FirstEvent']=deprecated.dbsTest.getInputEvents(url, workflowName)*2
	elif key=='RequestString':
	   schema['RequestString']='ACDC_'+value
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
    oldworkflow=sys.argv[1]
    user=sys.argv[2]
    group=sys.argv[3]	
    #print "Going to attempt to resubmit %s..." % sys.argv[1]
    url='cmsweb.cern.ch'
    schema = retrieveSchema(url, oldworkflow, user, group)
    newWorkflow=submitWorkflow(schema)
    print 'Cloned workflow:',newWorkflow
    approveRequest('cmsweb.cern.ch',newWorkflow)
    originalSplitting=getSplitting(oldworkflow)
    changeSplittingWorkflow.changeSplittingWorkflow(url, newWorkflow, int(originalSplitting/10))
    team=resubmitUnprocessedBlocks.getTeamWorkflow(url,oldworkflow)
    site=resubmitUnprocessedBlocks.getSiteWhitelist(url, oldworkflow)
    #activity=resubmitUnprocessedBlocks.getDashboardActivity(url, oldworkflow)
    activity='production'
    era=resubmitUnprocessedBlocks.getAcquisitonEra(url, oldworkflow)
    procversion=resubmitUnprocessedBlocks.getProcessingVersion(url, oldworkflow)
    procstring=resubmitUnprocessedBlocks.getProcessingString(url, oldworkflow)
    lfn=resubmitUnprocessedBlocks.getMergedLFNBaseWorkflow(url, oldworkflow)
    siteCust=getOriginalCustodial(url, oldworkflow)
    maxmergeevents = 50000
    if 'Fall11_R1' in oldworkflow:
    	maxmergeevents = 6000
    if 'DR61SLHCx' in oldworkflow:
        maxmergeevents = 5000
    deprecated.assignWorkflowsAuto.assignRequest(url , newWorkflow ,team ,site ,era, procversion, procstring, activity, lfn, maxmergeevents, 2300000, 4100000000, 0, siteCust)
    sys.exit(0)
