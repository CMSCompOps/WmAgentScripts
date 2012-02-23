#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, json, time, math, dbsTest, locale
import optparse, closeOutWorkflows
from xml.dom.minidom import getDOMImplementation

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
					priority=getPriorityWorkflow(url, name)
					historic[Site][stat].append((name,priority))
		if namefound==0:
			for stat in noNameSites.keys():
				if status==stat:
					priority=getPriorityWorkflow(url, name)
					noNameSites[stat].append((name,priority))

def orderfunction(workflowTuple):
	return workflowTuple[1]


def printTopRequests(historic, noNameSites, numRequests):
	#print historic
	for Site in historic.keys():
		print "Site "+Site
		for stat in historic[Site].keys():
			completeList=historic[Site][stat]
			completeList.sort(key=orderfunction, reverse=True)
			print "Status: "+stat
			if numRequests==-1:
				for workflow in completeList[:len(completeList)]:
					print workflow[0]+ " Priority: " + str(workflow[1])
				print " "
			else:
				for workflow in completeList[:numRequests]:
					print workflow[0]+ " Priority: " + str(workflow[1])
				print " "
		print " "
	print "Other Workflows: "
	for stat in noNameSites.keys():
			completeList=noNameSites[stat]
			completeList.sort(key=orderfunction,reverse=True)
			print "Status: "+stat
			if numRequests==-1:
				for workflow in completeList[:len(completeList)]:
					print workflow[0]+ " Priority: " + str(workflow[1])
				print " "
			else:
				for workflow in completeList[:numRequests]:
					print workflow[0]+ " Priority: " + str(workflow[1])
				print " "
	print " "

	


def getPriorityWorkflow(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/'+workflow)
	r2=conn.getresponse()
	request = json.read(r2.read())
	if 'RequestPriority' in request.keys():
		return int(request['RequestPriority'])
	else:
		return 0

def main():
	url='cmsweb.cern.ch'	
	siteList=['CNAF', 'FNAL', 'IN2P3', 'PIC', 'KIT', 'ASGC', 'RAL']
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
