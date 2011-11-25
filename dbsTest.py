#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, json, phedexSubscription
from xml.dom.minidom import getDOMImplementation

def getInputDataSet(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/'+workflow)
	r2=conn.getresponse()
	request = json.read(r2.read())
	inputDataSets=request['InputDatasets']
	if len(inputDataSets)<1:
		print "No InputDataSet for workflow " +workflow
	else:
		return inputDataSets[0]

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
	inputEvents=getEventCountDataSet(InputDataset)
	for dataset in outputDataSets:
		outputEvents=getEventCountDataSet(dataset)
		print dataset+" match: "+str(outputEvents/float(inputEvents)*100) +"%"
	sys.exit(0);

if __name__ == "__main__":
	main()

