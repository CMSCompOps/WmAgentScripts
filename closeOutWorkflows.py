#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest, duplicateEventsGen
from xml.dom.minidom import getDOMImplementation

def TransferComplete(url, dataset, site):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset+'&node='+site+'_MSS')
	r2=conn.getresponse()
	result = json.read(r2.read())
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
	r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset+'&node='+site+'_MSS')
	r2=conn.getresponse()
	result = json.read(r2.read())
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
	if len(filter(lambda s: s[1] == '1', siteList))>1 and (not 'T1_CH_CERN' in siteList):
		return "NoSite"
	for site in siteList:
		if 'CERN' in site:
			continue
		if 'T1' in site:
			return site
	res=re.search("T1_[A-Z]{2}_[A-Z]{3,4}", requestname)
	if res:
		return res.group()
	else:
		return "NoSite"

def classifyCompletedRequests(url, requests):
	workflows={'ReDigi':[],'MonteCarloFromGEN':{},'MonteCarlo':{} , 'ReReco':[], 'LHEStepZero':{}}
	for request in requests:
	    name=request['request_name']
	    status='NoStatus'
	    if 'status' in request.keys():
			status=request['status']
            requestType='NoType'
	    if 'type' in request.keys():
			 requestType=request['type']
	    if status=='completed':
		if requestType=='MonteCarloFromGEN' or requestType=='MonteCarlo' or requestType=='LHEStepZero':
			site=findCustodial(url, name)
			if requestType=='LHEStepZero':
				site='T1_US_FNAL'
			if site not in workflows[requestType].keys():
				workflows[requestType][site]=[name]
			else:
				workflows[requestType][site].append(name)
		if requestType=='ReDigi' or requestType=='ReReco':
			workflows[requestType].append(name)
	return workflows

def testEventCountWorkflow(url, workflow):
	print workflow
	#inputDataSet=dbsTest.getInputDataSet(url, workflow)
	inputEvents=0
	inputEvents=inputEvents+int(dbsTest.getInputEvents(url, workflow))
	datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
	CorrectCount=1
	for dataset in datasets:
		outputEvents=dbsTest.getEventCountDataSet(dataset)
		percentage=outputEvents/float(inputEvents)
		if float(percentage)<float(0.95):
			print "Workflow: " + workflow+" not closed-out cause outputdataset event count too low: "+dataset
			return 0
		if float(percentage)>float(1.1):
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
#		if 'franzoni_laserHCALskim' in workflow:
#			phedexSubscription.closeOutWorkflow(url, workflow)
#			phedexSubscription.announceWorkflow(url, workflow)
#			continue
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
		if getRequestTeam(url, workflow)=='analysis':#If the request ran in the special fast queue agent.
			CustodialT1Site=findCustodial(url, workflow)
			datasetsUnsuscribed=[]
			SubscriptionInputDataset=phedexSubscription.TestCustodialSubscriptionRequested(url, InputDataset, CustodialT1Site)
			for dataset in datasets:
				duplicate=duplicateEventsGen.duplicateLumi(dataset)
				PhedexSubscription=phedexSubscription.TestCustodialSubscriptionRequested(url, dataset, CustodialT1Site)
				if not PhedexSubscription:
					datasetsUnsuscribed.append(dataset)
				duplicate=duplicateEventsGen.duplicateLumi(dataset)
				if Percentage>float(0.95) and Percentage<=float(1) and PhedexSubscription and not duplicate:
					closeOutDataset=True
				else:
		 			closeOutDataset=False
				closeOutWorkflow=closeOutWorkflow and closeOutDataset
				print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), 100, duplicate, closeOutDataset)
			if closeOutWorkflow:
				datasetsUnsuscribed.append(InputDataset)
			if len(datasetsUnsuscribed)>0:
				phedexSubscription.makeCustodialMoveRequest(url, CustodialT1Site, datasetsUnsuscribed, "Custodial Move Subscription for MonteCarlo")
			if closeOutWorkflow:
				phedexSubscription.closeOutWorkflow(url, workflow)
			
		else:
			for dataset in datasets:
				duplicate=duplicateEventsGen.duplicateLumi(dataset)
				closeOutDataset=True
				Percentage=PercentageCompletion(url, workflow, dataset)
				PhedexSubscription=testOutputDataset(dataset)
				closeOutDataset=False
				if Percentage>float(0.95) and Percentage<=float(1) and PhedexSubscription and not duplicate:
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
	request = json.read(r2.read())
	while 'exception' in request:
		conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
		r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
		r2=conn.getresponse()
		request = json.read(r2.read())
	if 'teams' not in request:
		return 'NoTeam'		 
	teams=request['teams']
	if len(teams)<1:
		return 'NoTeam'
	else:
		return teams[0]

def closeOutMonterCarloRequests(url, workflows):
	datasetsUnsuscribedSpecialQueue=[]
	for site in workflows.keys():
		if site!='NoSite':
			datasetsUnsuscribed=[]
			for workflow in workflows[site]:
				datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
				closeOutWorkflow=True
				if getRequestTeam(url, workflow)!='analysis':#If request is in special queue
					#print workflow
					for dataset in datasets:
						closeOutDataset=True
						Percentage=PercentageCompletion(url, workflow, dataset)
						PhedexSubscription=phedexSubscription.TestCustodialSubscriptionRequested(url, dataset, site)
						if not PhedexSubscription:
								datasetsUnsuscribed.append(dataset)
						TransPercen=TransferPercentage(url, dataset, site)
						duplicate=duplicateEventsGen.duplicateLumi(dataset)
						if Percentage>=float(0.90) and PhedexSubscription and not duplicate and TransPercen==1:
							closeOutDataset=True
						else:
		 					closeOutDataset=False
						closeOutWorkflow=closeOutWorkflow and closeOutDataset
						print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), str(int(TransPercen*100)), duplicate, closeOutDataset)
				else:
					for dataset in datasets:
						closeOutDataset=True
						Percentage=PercentageCompletion(url, workflow, dataset)
						PhedexSubscriptionDone=phedexSubscription.TestSubscritpionSpecialRequest(url, dataset, 'T2_DE_DESY')
						if not PhedexSubscriptionDone:
							datasetsUnsuscribedSpecialQueue.append(dataset)
						PhedexSubscriptionAccepted=phedexSubscription.TestAcceptedSubscritpionSpecialRequest(url, dataset, 'T2_DE_DESY')
						TransPercen=1
						duplicate=duplicateEventsGen.duplicateLumi(dataset)
						if Percentage>=float(0.90) and PhedexSubscriptionAccepted and not duplicate and TransPercen==1:
							closeOutDataset=True
						else:
         						closeOutDataset=False
					closeOutWorkflow=closeOutWorkflow and closeOutDataset
					print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), str(int(TransPercen*100)), duplicate, closeOutDataset)
				if closeOutWorkflow:
					phedexSubscription.closeOutWorkflow(url, workflow)
			if len(datasetsUnsuscribed)>0:
				phedexSubscription.makeCustodialMoveRequest(url, site, datasetsUnsuscribed, "Custodial Move Subscription for MonteCarlo")
	phedexSubscription.makeCustodialReplicaRequest(url, 'T2_DE_DESY',datasetsUnsuscribedSpecialQueue, "Replica Subscription for Request in special production queue")
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
	#closeOutReRecoWorkflows(url, workflowsCompleted['ReReco'])	
	closeOutRedigiWorkflows(url, workflowsCompleted['ReDigi'])
	closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarlo'])
	closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarloFromGEN'])
	closeOutMonterCarloRequests(url, workflowsCompleted['LHEStepZero'])
	print "MC Workflows for which couldn't find Custodial Tier1 Site"
	if 'NoSite' in workflowsCompleted['MonteCarlo']:
		print workflowsCompleted['MonteCarlo']['NoSite']
	if 'NoSite' in workflowsCompleted['MonteCarloFromGEN']:
		print workflowsCompleted['MonteCarloFromGEN']['NoSite']
	sys.exit(0);

if __name__ == "__main__":
	main()

