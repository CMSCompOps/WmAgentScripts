#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest

def testEventCountWorkflow(url, workflow):
	inputEvents=0
	inputEvents=inputEvents+dbsTest.getInputEvents(url, workflow)
	datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
	for dataset in datasets:
		outputEvents=dbsTest.getEventCountDataSet(dataset)
		percentage=outputEvents/float(inputEvents)
		if float(percentage)>float(1):
			print "Workflow: " + workflow+" duplicate events in outputdataset: "+dataset +" percentage: "+str(outputEvents/float(inputEvents)*100) +"%"
	return 1


def main():
	args=sys.argv[1:]
	if not len(args)==1:
		print "usage file"
	filename=args[0]
	url='cmsweb.cern.ch'
	workflows=phedexSubscription.workflownamesfromFile(filename)
 	for workflow in workflows:
		testEventCountWorkflow(url, workflow)
	sys.exit(0);

if __name__ == "__main__":
	main()
