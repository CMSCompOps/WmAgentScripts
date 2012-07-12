#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest, closeOutWorkflows, re
from xml.dom.minidom import getDOMImplementation


def getFilterEfficiency(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.read(r2.read())
	if 'FilterEfficiency' in request.keys():
		return request['FilterEfficiency']
	else:
		return 1

def getDatasetStatus(dataset):
	querry="./dbssql --input='find dataset.status where dataset="+dataset+"' | awk '{print $2}' | tail -n 2 | head -n 1"
	output=os.popen(querry).read()
	return output[2:-3]	

def duplicateLumi(dataset):
	querry="./dbssql --limit=10000000 --input='find file, lumi where dataset="+dataset+"'| grep store| awk '{print $2}' | sort | uniq -c | awk '{print $1}' | sort | uniq | awk '{if ($1>1) print $1}'"
	output=os.popen(querry).read()
	if output:
		return True
	else:
		return False
	


def classifyRequests(url, requests):
	print '-----------------------------------------------------------------------------------------------------------------------------------------------------------'
    	print '| Request                                                       |req Type |Status Req | Dataset             |Status Dataset | Percentage|FilterEfficiency| ' 
   	print '-----------------------------------------------------------------------------------------------------------------------------------------------------------'
	classifiedRequests={}
	for request in requests:
		if 'type' in request:
			name=request['request_name']
			if request['type']=='MonteCarloFromGEN' or request['type']=='MonteCarlo':
				datasetWorkflow=phedexSubscription.outputdatasetsWorkflow(url, name)
				problem=False
				percentage=0
				if len(datasetWorkflow)<1:
					continue
				dataset=datasetWorkflow[0]
				inputEvents=0.0001
				inputEvents=inputEvents+int(dbsTest.getInputEvents(url, name))
				outputEvents=dbsTest.getEventCountDataSet(dataset)
				percentage=outputEvents/float(inputEvents)
				duplicate=duplicateLumi(dataset)
				problem=False
				if duplicate:
					problem=True
				if problem:
					FilterEfficiency=getFilterEfficiency(url, name)
					datasetStatus=getDatasetStatus(dataset)
					print '| %20s | %8s| %8s | %20s | %10s| %10s | %10s| ' % (name, request['type'], request['status'], dataset,datasetStatus, str(percentage*100), FilterEfficiency)

	print '---------------------------------------------------------------------------------------------------------------------------'

def main():
	url='cmsweb.cern.ch'
	print "Gathering Requests"
	requests=closeOutWorkflows.getOverviewRequest()
	print "Classifying Requests"
	classifyRequests(url, requests)
	sys.exit(0);

if __name__ == "__main__":
	main()
