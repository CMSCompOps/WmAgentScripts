#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest, duplicateEventsGen
from xml.dom.minidom import getDOMImplementation

def TransferComplete(url, dataset, site):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset+'&node='+site+'_MSS')
	r2=conn.getresponse()
	result = json.loads(r2.read())
	blocks=result['phedex']
	if 'block' not in blocks.keys():
		return False
	if len(result['phedex']['block'])==0:
		return False
	for block in blocks['block']:
		if block['replica'][0]['complete']!='y':
			return False
	return True

def TransferPercentage(url, dataset, site):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset+'&node='+site)
	r2=conn.getresponse()
	result = json.loads(r2.read())
	blocks=result['phedex']
	if 'block' not in blocks.keys():
		return 0
	if len(result['phedex']['block'])==0:
		return 0
	total=len(blocks['block'])
	completed=0
	for block in blocks['block']:
		if block['replica'][0]['complete']=='y':
			completed=completed+1
	return float(completed)/float(total)		

#Returns the site for which a custodial move subscription for a dataset was created, if none is found it return 0
def CustodialMoveSubscriptionCreated(datasetName):
	url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions?dataset=' + datasetName
	result = json.loads(urllib2.urlopen(url).read())
        datasets=result['phedex']
	if 'dataset' not in datasets.keys():
		return False
	else:
		if len(result['phedex']['dataset'])<1:
			return False
		for subscription in result['phedex']['dataset'][0]['subscription']:
			if subscription['custodial']=='y':
				return subscription['node']
		return False
	

def getOverviewRequest():
	url='vocms204.cern.ch'
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/monitorSvc/requestmonitor')
	r2=conn.getresponse()
        requests = json.loads(r2.read())
	return requests

#Returns a list with the custodial sites for a request
def findCustodial(url, requestname):
	custodialSites=[]
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+requestname)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	while 'exception' in request:
		conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
		r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
		r2=conn.getresponse()
		request = json.loads(r2.read())
	siteList=request['Site Whitelist']
	for site in siteList:
		if 'T1' in site:
			custodialSites.append(site)
	return "NoSite"

def classifyCompletedRequests(url, requests):
	workflows={'ReDigi':[],'MonteCarloFromGEN':[],'MonteCarlo':[] , 'ReReco':[], 'LHEStepZero':[]}
	for request in requests:
	    name=request['request_name']
	    status='NoStatus'
	    if 'status' in request.keys():
			status=request['status']
            requestType='NoType'
	    if 'type' in request.keys():
			 requestType=request['type']
	    if status=='completed':
		if requestType=='MonteCarloFromGEN' or requestType=='MonteCarlo' or requestType=='LHEStepZero'or requestType=='ReDigi' or requestType=='ReReco':
			workflows[requestType].append(name)
	return workflows

def testOutputDataset(datasetName):
	 url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/Subscriptions?dataset=' + datasetName
         result = json.loads(urllib2.urlopen(url).read())
	 datasets=result['phedex']['dataset']
	 if len(datasets)>0:
		dicts=datasets[0]
		subscriptions=dicts['subscription']
		for subscription in subscriptions:
			if subscription['level']=='DATASET' and subscription['custodial']=='y':
				return True
	 else:
		return False

def testWorkflow(url, workflow):
	datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
	subscribed=1
	for dataset in datasets:
		if not testOutputDataset(dataset):
			subscribed=0
			return 0
	return 1

def closeOutReRecoWorkflows(url, workflows):
	for workflow in workflows:
		if 'RelVal' in workflow:
			continue
    		if 'TEST' in workflow:
			continue
		datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
		closeOutWorkflow=True
		InputDataset=dbsTest.getInputDataSet(url, workflow)
		for dataset in datasets:
			duplicate=False
			closeOutDataset=True
			Percentage=PercentageCompletion(url, workflow, dataset)
			PhedexSubscription=testOutputDataset(dataset)
			closeOutDataset=False
			if Percentage==1 and PhedexSubscription and not duplicate:
				closeOutDataset=True
			else:
         			closeOutDataset=False
			closeOutWorkflow=closeOutWorkflow and closeOutDataset
			print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), 100, duplicate, closeOutDataset)
		if closeOutWorkflow:
			phedexSubscription.closeOutWorkflow(url, workflow)
	print '----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'

def closeOutRedigiWorkflows(url, workflows):
	for workflow in workflows:
		closeOutWorkflow=True
		InputDataset=dbsTest.getInputDataSet(url, workflow)
		datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
		for dataset in datasets:
			closeOutDataset=False
			Percentage=PercentageCompletion(url, workflow, dataset)
			PhedexSubscription=testOutputDataset(dataset)
			duplicate=True
			if PhedexSubscription!=False and Percentage>=float(0.95):
				duplicate=dbsTest.duplicateRunLumi(dataset)
			if Percentage>=float(0.95) and PhedexSubscription and not duplicate:
				closeOutDataset=True
			else:
		 		closeOutDataset=False
			closeOutWorkflow=closeOutWorkflow and closeOutDataset
			print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), 100, duplicate, closeOutDataset)
		if closeOutWorkflow:
			phedexSubscription.closeOutWorkflow(url, workflow)
	print '----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'



#the team on which a requet is running
def getRequestTeam(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	while 'exception' in request:
		conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
		r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
		r2=conn.getresponse()
		request = json.loads(r2.read())
	if 'teams' not in request:
		return 'NoTeam'		 
	teams=request['teams']
	if len(teams)<1:
		return 'NoTeam'
	else:
		return teams[0]

def closeOutMonterCarloRequests(url, workflows):
	for workflow in workflows:
		datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
		closeOutWorkflow=True
		if getRequestTeam(url, workflow)!='analysis':#If request is not in special queue
			for dataset in datasets:
				ClosePercentage=0.93
				if 'SMS' in dataset:
					ClosePercentage=1
				closeOutDataset=True
				Percentage=PercentageCompletion(url, workflow, dataset)
				PhedexSubscription=CustodialMoveSubscriptionCreated(dataset)
				TransPercen=0
				if PhedexSubscription!=False:
					site=PhedexSubscription
					TransPercen=TransferPercentage(url, dataset, site)
				duplicate=True
				if PhedexSubscription!=False and Percentage>=float(0.9):
					duplicate=dbsTest.duplicateLumi(dataset)
				if Percentage>=float(ClosePercentage) and PhedexSubscription!=False and not duplicate:
					closeOutDataset=True
				else:
		 			closeOutDataset=False
				closeOutWorkflow=closeOutWorkflow and closeOutDataset
				print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), str(int(TransPercen*100)), duplicate, closeOutDataset)
			if closeOutWorkflow:
				phedexSubscription.closeOutWorkflow(url, workflow)
		
	print'-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------' 				    


def closeOutStep0Requests(url, workflows):
	for workflow in workflows:
		datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
		closeOutWorkflow=True
		if getRequestTeam(url, workflow)!='analysis':#If request is not in special queue
			for dataset in datasets:
				closeOutDataset=False
				Percentage=PercentageCompletion(url, workflow, dataset)
				PhedexSubscription=CustodialMoveSubscriptionCreated(dataset)
				if PhedexSubscription!=False:
					site=PhedexSubscription
					TransPercen=TransferPercentage(url, dataset, site)
				duplicate=dbsTest.duplicateLumi(dataset)
				correctLumis=dbsTest.checkCorrectLumisEventGEN(dataset)
				if Percentage>=float(0.90) and PhedexSubscription!=False and not duplicate and correctLumis:
					closeOutDataset=True
				else:
		 			closeOutDataset=False
				closeOutWorkflow=closeOutWorkflow and closeOutDataset
				print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), str(correctLumis), duplicate, closeOutDataset)
			if closeOutWorkflow:
				phedexSubscription.closeOutWorkflow(url, workflow)
		
	print'-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'


# It assumes dataset is an output dataset from the workflow
def PercentageCompletion(url, workflow, dataset):
	inputEvents=0
	inputEvents=inputEvents+int(dbsTest.getInputEvents(url, workflow))
	outputEvents=dbsTest.getOutputEvents(url, workflow, dataset)
	if inputEvents==0:
		return 0
	percentage=outputEvents/float(inputEvents)
	return percentage

def main():
	url='cmsweb.cern.ch'
	print "Gathering Requests"
	requests=getOverviewRequest()
	print "Classifying Requests"
	workflowsCompleted=classifyCompletedRequests(url, requests)
	print '-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'
    	print '| Request                                                                          | OutputDataSet                                                                                        |%Compl|Subscr|Tran|Dupl|ClosOu|'
   	print '-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'
	closeOutReRecoWorkflows(url, workflowsCompleted['ReReco'])	
	closeOutRedigiWorkflows(url, workflowsCompleted['ReDigi'])
	closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarlo'])
	closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarloFromGEN'])
	closeOutStep0Requests(url, workflowsCompleted['LHEStepZero'])
	print "MC Workflows for which couldn't find Custodial Tier1 Site"
	if 'NoSite' in workflowsCompleted['MonteCarlo']:
		print workflowsCompleted['MonteCarlo']['NoSite']
	if 'NoSite' in workflowsCompleted['MonteCarloFromGEN']:
		print workflowsCompleted['MonteCarloFromGEN']['NoSite']
	sys.exit(0);

if __name__ == "__main__":
	main()

