#!/usr/bin/env python

import urllib2, urllib, httplib, sys, re, os, json, time, math, locale
from deprecated import dbsTest
import optparse, closeOutWorkflows
from xml.dom.minidom import getDOMImplementation


def maxEventsFileDataset(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if not 'InputDataset' in request.keys():
		return False
	inputDataSet=request['InputDataset']
	BlockWhitelist=request['BlockWhitelist']
	runWhitelist=request['RunWhitelist']
	querry="'find dataset, max(file.numevents) where dataset="+inputDataSet
	if len(BlockWhitelist)>0:
		querry=querry+' AND ('
		for block in BlockWhitelist:
			querry=querry+' block= '+block+' OR'
		querry=querry+' block= '+BlockWhitelist[0] +')'
	if len(runWhitelist)>0:
		querry=querry+' AND ('
		for run in runWhitelist:
			querry=querry+' run= '+str(run)+' OR'
		querry=querry+' run= '+str(runWhitelist[0]) +')'
	
	output=os.popen("./dbssql --input="+querry+"'| awk '{print $2}' | grep '[0-9]\{1,\}'").read()
	if "version" in output:
		output = 0
	if not output:
		output = 0
	return int(output)

def getEffectiveLumiSections(url, workflow, requestType):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if not 'InputDataset' in request.keys():
		return -1
	inputDataSet=request['InputDataset']
	BlockWhitelist=request['BlockWhitelist']
	runWhitelist=request['RunWhitelist']
	querry='find file,run,lumi where dataset ='+inputDataSet
	if requestType == "ReReco":
		querry='find run,lumi where dataset ='+inputDataSet
	else:
		querry="'find dataset,count(lumi) where dataset="+inputDataSet
	if len(BlockWhitelist)>0:
		querry=querry+' AND ('
		for block in BlockWhitelist:
			querry=querry+' block= '+block+' OR'
		querry=querry+' block= '+BlockWhitelist[0] +')'
	if len(runWhitelist)>0:
		querry=querry+' AND ('
		for run in runWhitelist:
			querry=querry+' run= '+str(run)+' OR'
		querry=querry+' run= '+str(runWhitelist[0]) +')'
	if requestType == "ReReco":
		output=os.popen("./dbssql --limit=1000000 --input='"+querry+"'"+ "|wc -l").read()	
	else:
		output=os.popen("./dbssql --limit=1000000 --input="+querry+"'| awk '{print $2}' | grep '[0-9]\{1,\}'").read()
	if not output:
		output = 0
	if requestType == "ReReco":
		output = int(output)-3
	return int(output)

#Gets the time per event of a requests if the parameter hasn't been set up by the requestor it returns 1

def getTimeEventRequest(url, requestName):
	conn=httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+requestName)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'TimePerEvent' in request.keys():
		return int(request['TimePerEvent'])
	elif 'time_per_event' in request.keys():
		return int(request['time_per_event'])
	else:
		return 0


def classifyRequests(url, requests, historic, noNameSites, requestType):
	for request in requests:
	    name=request['request_name']
	    status='NoStatus'
	    reqType='NoType'
	    if 'type' in request.keys():
		reqType=request['type']
	    if reqType!=requestType:
		continue
	    if 'status' in request.keys():
			status=request['status']
            if status in noNameSites.keys():
		namefound=0
		for Site in historic.keys():
 		    if name.find(Site)>=0:
			namefound=1
			for stat in historic[Site].keys():#stat is the status of the request in the list of requests
				if status==stat:
					EffectiveLumi=-999
					TimeEvent=getTimeEventRequest(url, name)
					priority=getPriorityWorkflow(url, name)
					numevents=-999
					checkLumi=False
					if float(numevents/EffectiveLumi)>400:
						checkLumi=True
					else:
						checkLumi=False
					maxEvents=-999
					historic[Site][stat].append((name,priority,numevents, TimeEvent,EffectiveLumi, checkLumi, maxEvents))
		if namefound==0:
			for stat in noNameSites.keys():
				if status==stat:
					EffectiveLumi=-999
					TimeEvent=getTimeEventRequest(url, name)
					priority=getPriorityWorkflow(url, name)
					numevents=-999
					checkLumi=False
					if float(numevents/EffectiveLumi)>400:
						checkLumi=True
					else:
						checkLumi=False
					maxEvents=-999
					noNameSites[stat].append((name,priority,numevents, TimeEvent,EffectiveLumi, checkLumi, maxEvents))
					
					
def orderfunction(workflowTuple):
	return workflowTuple[1]


def printRequests(completeList, numRequests, status, Site):
	print '-----------------------------------------------------------------------------------------------------------------------------------------------------'
	print '| %82s | Priority | Num Events | TimeEv  | Ef/Lumi|Num/Lum|CkLum|FileSize|' % (status + " Requests " + Site)
	print '-----------------------------------------------------------------------------------------------------------------------------------------------------'
	if numRequests==-1:
		numRequests=len(completeList)
	for workflow in completeList[:numRequests]:
		print '| %82s | %8d | %10d | %8d | %7d|%6d|%5s|%8d|' % (workflow[0], workflow[1], workflow[2], workflow[3], workflow[4], int(workflow[2]/workflow[4]),workflow[5], workflow[6])				
	print '-----------------------------------------------------------------------------------------------------------------------------------------------------'	


def printTopRequests(historic, noNameSites, numRequests):
	#print historic
	for Site in historic.keys():
		for stat in historic[Site].keys():
			completeList=historic[Site][stat]
			completeList.sort(key=orderfunction, reverse=True)
			if len(completeList)==0:
				continue
			printRequests(completeList,numRequests, stat, Site)
			
	#print "Other Workflows: "
	for stat in noNameSites.keys():
			completeList=noNameSites[stat]
			completeList.sort(key=orderfunction,reverse=True)
			if len(completeList)==0:
				continue
			printRequests(completeList,numRequests, stat, 'No Site')

	


def getPriorityWorkflow(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/'+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	if 'RequestPriority' in request.keys():
		return int(request['RequestPriority'])
	else:
		return 0

def main():
	url='cmsweb.cern.ch'	
	siteList=['CNAF', 'FNAL', 'IN2P3', 'PIC', 'KIT', 'ASGC', 'RAL']
	#siteList=['ASGC']
	parser = optparse.OptionParser()
	parser.add_option('-n', '--number', help='Number of Requests',dest='number')
	parser.add_option('-t', '--type', help='Type of Requests',dest='type')
	parser.add_option('-s', '--status', help='Status of Requests',dest='status')
	(options,args) = parser.parse_args()
	statusList=['assignment-approved','acquired', 'running', 'completed', 'closed-out']
	if not options.status:
		statusList=['assignment-approved','acquired', 'running', 'completed', 'closed-out']
	else:
		statusList = [options.status]
	historic=dict([(site, dict([(status, []) for status in statusList])) for site in siteList])
	noNameSites=dict([(status,[]) for status in statusList])
	numrequests=-1 # It will print all the requests
	if not options.number:
		numrequests=-1
	else:
		numrequests=options.number
	requestType='ReDigi'
	if not options.type:# The type of request default will be redigi
		requestType='ReDigi'
	else:
		requestType=options.type 
	print "Number of requests :"+str(numrequests)
	print "Type requested: "+requestType
       	print "Gathering request"
	requests=closeOutWorkflows.getOverviewRequest()
	print "Classifying requests"
	classifyRequests(url, requests, historic, noNameSites, requestType)
	print "Printing Requests"
	printTopRequests(historic, noNameSites, int(numrequests))
	sys.exit(0);

if __name__ == "__main__":
	main()
