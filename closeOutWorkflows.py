#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest
from xml.dom.minidom import getDOMImplementation



def testEventCountWorkflow(url, workflow):
	inputDataSet=dbsTest.getInputDataSet(url, workflow)
	inputEvents=0
	inputEvents=inputEvents+dbsTest.getInputEvents(url, workflow)
	datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
	CorrectCount=1
	for dataset in datasets:
		outputEvents=dbsTest.getEventCountDataSet(dataset)
		percentage=outputEvents/float(inputEvents)
		if float(percentage)<float(0.99) or float(percentage)>float(1):
			print "Workflow: " + workflow+" not subscribed cause outputdataset event count too low: "+dataset
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


def main():
	args=sys.argv[1:]
	if not len(args)==1:
		print "usage file"
	filename=args[0]
	url='cmsweb.cern.ch'
	workflows=phedexSubscription.workflownamesfromFile(filename)
 	for workflow in workflows:
		if testWorkflow(url, workflow) and testEventCountWorkflow(url, workflow):
			print "This workflow is closed-out: " + workflow
			phedexSubscription.closeOutWorkflow(url, workflow)
		else:
			print "This workflow has not been closed-out: " + workflow
	sys.exit(0);

if __name__ == "__main__":
	main()

