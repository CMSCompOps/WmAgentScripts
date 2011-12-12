#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, json, phedexSubscription
from xml.dom.minidom import getDOMImplementation

def getWorkflowType(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.read(r2.read())
	requestType=request['RequestType']
	return requestType


def getRunWhitelist(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.read(r2.read())
	runWhitelist=request['RunWhitelist']
	return runWhitelist

def getInputDataSet(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.read(r2.read())
	inputDataSets=request['InputDataset']
	if len(inputDataSets)<1:
		print "No InputDataSet for workflow " +workflow
	else:
		return inputDataSets

def getEventsRun(url, dataset, run):
	output=os.popen("./dbssql --input='find dataset,sum(block.numevents) where dataset="+dataset+" and run="+str(run)+"' "+"|awk '{print $2}' | grep '[0-9]\{1,\}'").read()
	try:
		int(output)
		return int(output)
	except ValueError:
       		return -1	


def getEventCountDataSet(dataset):
	output=os.popen("./dbssql --input='find dataset,sum(block.numevents) where dataset="+dataset+"'"+ "|awk '{print $2}' | grep '[0-9]\{1,\}'").read()
	try:
		int(output)
		return int(output)
	except ValueError:
       		return -1
def main():
	args=sys.argv[1:]
	if not len(args)==1:
		print "usage:dbsTest workflowname"
		sys.exit(0)
	workflow=args[0]
	url='cmsweb.cern.ch'
	InputDataset=getInputDataSet(url, workflow)
	outputDataSets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
	inputEvents=0
	if (len(getRunWhitelist(url, workflow))==0):
		inputEvents=getEventCountDataSet(InputDataset)
	else:
		runWhitelist=getRunWhitelist(url, workflow)
		for run in runWhitelist:
			inputEvents=inputEvents+getEventsRun(url, InputDataset, run)
	for dataset in outputDataSets:
		outputEvents=getEventCountDataSet(dataset)
		print dataset+" match: "+str(outputEvents/float(inputEvents)*100) +"%"
	sys.exit(0);

if __name__ == "__main__":
	main()

