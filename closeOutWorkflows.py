#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest
from xml.dom.minidom import getDOMImplementation

def TransferComplete(url, dataset, site):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset+'&node='+site+'_MSS')
	r2=conn.getresponse()
	result = json.read(r2.read())
	blocks=result['phedex']
	if 'block' not in blocks.keys():
		return False
	for block in blocks['block']:
		if block['replica'][0]['complete']!='y':
			return False
	return True
			

def CustodialMoveSubscriptionCreated(datasetName):
	url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions?dataset=' + datasetName
	result = json.read(urllib2.urlopen(url).read())
        datasets=result['phedex']
	if 'dataset' not in datasets.keys():
		return False
	else:
		if len(result['phedex']['dataset'])<1:
			return False
		for subscription in result['phedex']['dataset'][0]['subscription']:
			if subscription['custodial']=='y':
				return True
		return False
	

def getOverviewRequest():
	url='vocms204.cern.ch'
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/monitorSvc/requestmonitor')
	r2=conn.getresponse()
        requests = json.read(r2.read())
	return requests


def findCustodial(url, requestname):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+requestname)
	r2=conn.getresponse()
	request = json.read(r2.read())
	siteList=request['Site Whitelist']
	if len(filter(lambda s: s[1] == '1', siteList))>1:
		return "NoSite"
	for site in siteList:
		if 'T1' in site:
			return site
	res=re.search("T1_[A-Z]{2}_[A-Z]{3,4}", requestname)
	if res:
		return res.group()
	else:
		return "NoSite"

def classifyCompletedRequests(url, requests):
	workflows={'ReDigi':[],'MonteCarloFromGEN':{},'MonteCarlo':{} }	
	for request in requests:
	    name=request['request_name']
	    status='NoStatus'
	    if 'status' in request.keys():
			status=request['status']
            requestType='NoType'
	    if 'type' in request.keys():
			 requestType=request['type']
	    if status=='completed':
		if requestType=='MonteCarloFromGEN' or requestType=='MonteCarlo':
			site=findCustodial(url, name)
			if site not in workflows[requestType].keys():
				workflows[requestType][site]=[name]
			else:
				workflows[requestType][site].append(name)
		if requestType=='ReDigi':
			workflows[requestType].append(name)
	return workflows

def testEventCountWorkflow(url, workflow):
	#inputDataSet=dbsTest.getInputDataSet(url, workflow)
	inputEvents=0
	inputEvents=inputEvents+dbsTest.getInputEvents(url, workflow)
	datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
	CorrectCount=1
	for dataset in datasets:
		outputEvents=dbsTest.getEventCountDataSet(dataset)
		percentage=outputEvents/float(inputEvents)
		if float(percentage)<float(0.95):
			print "Workflow: " + workflow+" not closed-out cause outputdataset event count too low: "+dataset
			return 0
		if float(percentage)>float(1.5):
			print "Workflow: " + workflow+" not closed-out cause outputdataset event count too HIGH: "+dataset
			return 0
	return 1


def testOutputDataset(datasetName):
	 url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/Subscriptions?dataset=' + datasetName
         result = json.read(urllib2.urlopen(url).read())
	 datasets=result['phedex']['dataset']
	 if len(datasets)>0:
		dicts=datasets[0]
		subscriptions=dicts['subscription']
		for subscription in subscriptions:
			if subscription['level']=='DATASET' and subscription['custodial']=='y':
				return 1
	 else:
		print "This dataset wasn't subscribed: "+ datasetName
		return 0

def testWorkflow(url, workflow):
	datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
	subscribed=1
	for dataset in datasets:
		if not testOutputDataset(dataset):
			subscribed=0
			return 0
	return 1

def closeOutRedigiWorkflows(url, workflows):
	for workflow in workflows:
		if testWorkflow(url, workflow) and testEventCountWorkflow(url, workflow):
			print "This workflow is closed-out: " + workflow
			phedexSubscription.closeOutWorkflow(url, workflow)
		else:
			print "This workflow has not been closed-out: " + workflow

def closeOutMonterCarloRequests(url, workflows):
	for site in workflows.keys():
		datasetsUnsuscribed=[]
		if site!='NoSite':
			for workflow in workflows[site]:
				if testEventCountWorkflow(url, workflow):
					datasetWorkflow=phedexSubscription.outputdatasetsWorkflow(url, workflow)
					uncompletedTransfers=[]	
					for dataset in datasetWorkflow:
						if not phedexSubscription.TestCustodialSubscriptionRequested(url, dataset, site):
							print "This dataset hasn't been subscribed "+dataset
							datasetsUnsuscribed.append(dataset)
							uncompletedTransfers.append(dataset)
					datasetWorkflow=phedexSubscription.outputdatasetsWorkflow(url, workflow)
					for dataset in datasetWorkflow:
						if TransferComplete(url, dataset, site)==False:
							uncompletedTransfers.append(dataset)
					if len(uncompletedTransfers)==0:
						phedexSubscription.closeOutWorkflow(url, workflow)
					else:
						print "This workflow has not been closed-out: " + workflow
						for dataset in uncompletedTransfers:
							print dataset +" not completely transfered"
						
			if len(datasetsUnsuscribed)>0:
				phedexSubscription.makeCustodialMoveRequest('cmsweb-dev.cern.ch', site, datasetsUnsuscribed, "Custodial Move Subscription for MonteCarlo")

def main():
	url='cmsweb.cern.ch'
	print "Gathering Requests"
	requests=getOverviewRequest()
	print "Classifying Requests"
	workflowsCompleted=classifyCompletedRequests(url, requests)
	closeOutRedigiWorkflows(url, workflowsCompleted['ReDigi'])
	closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarlo'])
	closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarloFromGEN'])
	print "MC Workflows for which couldn't find Custodial Tier1 Site"
	if 'NoSite' in workflowsCompleted['MonteCarlo']:
		print workflowsCompleted['MonteCarlo']['NoSite']
	if 'NoSite' in workflowsCompleted['MonteCarloFromGEN']:
		print workflowsCompleted['MonteCarloFromGEN']['NoSite']
	sys.exit(0);

if __name__ == "__main__":
	main()

