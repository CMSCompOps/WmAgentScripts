#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest, duplicateEventsGen
from xml.dom.minidom import getDOMImplementation


def getOverviewRequest():
	url='vocms204.cern.ch'
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/monitorSvc/requestmonitor')
	r2=conn.getresponse()
        requests = json.loads(r2.read())
	return requests

def getOverviewRequestsWMStats(url):
	conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                     key_file = os.getenv('X509_USER_PROXY'))
	conn.request("GET",
                 "/couchdb/wmstats/_design/WMStats/_view/requestByStatusAndType?stale=update_after")
	response = conn.getresponse()
	data = response.read()
	conn.close()
	myString=data.decode('utf-8')
	workflows=json.loads(myString)['rows']
	return workflows


def classifyCompletedRequests(url, requests):
	workflows={'ReDigi':[],'MonteCarloFromGEN':[],'MonteCarlo':[] , 'ReReco':[], 'LHEStepZero':[]}
	for request in requests:
	    name=request['id']
	    if len(request['key'])<3:
		print request
		continue
	    status=request['key'][1]
	    requestType=request['key'][2]
	    if status=='completed':
		if requestType=='MonteCarlo':
			datasets=phedexSubscription.outputdatasetsWorkflow(url, name)
			m=re.search('.*/GEN$',datasets[0])
			if m:
				workflows['LHEStepZero'].append(name)
			else:
				workflows[requestType].append(name)
		if requestType=='MonteCarloFromGEN' or requestType=='LHEStepZero'or requestType=='ReDigi' or requestType=='ReReco':
				workflows[requestType].append(name)
	return workflows

def classifyRunningRequests(url, requests):
    """
    Creates an index for running requests
    The key is the request string
    """
    workflows={}
    for request in requests:
        #name of the request
        name=request['id']
        if len(request['key'])<3:
            print request
            continue
        #status
        status=request['key'][1]
        #add to the index
        if status in ['running-closed', 'running-open', 'assigned','acquired','assignment-approved']:
            #if it has the same request string add to a list            
            reqString = getRequestString(name)
            if reqString not in workflows:
                workflows[reqString] = [name]
            else:
                workflows[reqString].append(name)
    return workflows


def classifyCompletedRequests(url, requests):
	workflows={'ReDigi':[],'MonteCarloFromGEN':[],'MonteCarlo':[] , 'ReReco':[], 'LHEStepZero':[]}
	for request in requests:
	    name=request['id']
	    if len(request['key'])<3:
		print request
		continue
	    status=request['key'][1]
	    requestType=request['key'][2]
	    if status=='completed':
		if requestType=='MonteCarlo':
			datasets=phedexSubscription.outputdatasetsWorkflow(url, name)
			m=re.search('.*/GEN$',datasets[0])
			if m:
				workflows['LHEStepZero'].append(name)
			else:
				workflows[requestType].append(name)
		if requestType=='MonteCarloFromGEN' or requestType=='LHEStepZero'or requestType=='ReDigi' or requestType=='ReReco':
				workflows[requestType].append(name)
	return workflows


def countNoDupWF(workflowsCompleted, workflowsRunning, wfType):
    wfs = workflowsCompleted[wfType]
    result = []
    #check for everyone if it has one runnign with the same strng name
    for wf in wfs:
        reqString = getRequestString(wf)    
        #check how many acdcs have
        #print wf
        if reqString in workflowsRunning:
            #print workflowsRunning[reqString]
            pass
        else:
            #print 'no acdcs running'
            result.append(wf)
    return result

import re
p = re.compile(r'[a-z_]+(?:ACDC_)*([a-zA-Z0-9_\-]+)')
p2 = re.compile(r'_\d{6}_[0-9_]+')

def getRequestString(request):
    """
    Extracts the reques string from the request name
    """
    m = p2.search(request)
    if not m:
        print request, 'NOT MATCH!!!'
        return request
    s = m.group(0)    
    s = request.replace(s,'')
    m = p.match(s)    
    if not m:
        print s
        return request
    return m.group(1)

def main():
    url='cmsweb.cern.ch'
    print "Gathering Requests"
    requests=getOverviewRequestsWMStats(url)
    print "Classifying Requests"
    workflowsCompleted=classifyCompletedRequests(url, requests)
    workflowsRunning =classifyRunningRequests(url, requests)
    print "Getting no duplicated requests"
    noDupWfs = countNoDupWF(workflowsCompleted, workflowsRunning,'ReReco')
    print "Workflows that are completed, but don't have ACDC's"
    print "---------------------------------------------------"
    print "ReReco's"
    print "---------------------------------------------------"
    for wf in noDupWfs:
        print wf
    noDupWfs = countNoDupWF(workflowsCompleted, workflowsRunning,'ReDigi')
    print "---------------------------------------------------"
    print "ReDigi's"
    print "---------------------------------------------------"
    for wf in noDupWfs:
        print wf
    noDupWfs = countNoDupWF(workflowsCompleted, workflowsRunning,'MonteCarloFromGEN')
    print "---------------------------------------------------"
    print "MonteCarloFromGEN"
    print "---------------------------------------------------"
    for wf in noDupWfs:
        print wf
    noDupWfs = countNoDupWF(workflowsCompleted, workflowsRunning,'MonteCarlo')
    print "---------------------------------------------------"
    print "MonteCarlo"
    print "---------------------------------------------------"
    for wf in noDupWfs:
        print wf
    noDupWfs = countNoDupWF(workflowsCompleted, workflowsRunning,'LHEStepZero')
    print "---------------------------------------------------"
    print "LHEStepZero"
    print "---------------------------------------------------"
    for wf in noDupWfs:
        print wf    
    sys.exit(0);

if __name__ == "__main__":
	main()
"""wfs = open('wfs')
    for wf in wfs.readlines():
        wf = wf.strip()
        print wf, getRequestString(wf)
"""
