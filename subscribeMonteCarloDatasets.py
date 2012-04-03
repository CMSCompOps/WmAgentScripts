#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest, closeOutWorkflows
from xml.dom.minidom import getDOMImplementation

def classifyRunningRequests(url, requests):
	datasetsUnsuscribed={}
	for request in requests:
	    name=request['request_name']
	    status='NoStatus'
	    if 'status' in request.keys():
			status=request['status']
	    else:
		continue
            requestType='NoType'
	    if 'type' in request.keys():
			 requestType=request['type']
	    else:
		continue	  
	    if status=='running':
		if requestType=='MonteCarloFromGEN' or requestType=='MonteCarlo':
			site=closeOutWorkflows.findCustodial(url, name)
			if site=='NoSite':
				continue
			datasetWorkflow=phedexSubscription.outputdatasetsWorkflow(url, name)
			for dataset in datasetWorkflow:
						inputEvents=0
						inputEvents=inputEvents+int(dbsTest.getInputEvents(url, name))
						outputEvents=dbsTest.getEventCountDataSet(dataset)
						percentage=outputEvents/float(inputEvents)
						if float(percentage)>float(0.50):
							if not phedexSubscription.TestCustodialSubscriptionRequested(url, dataset, site):
								if site not in datasetsUnsuscribed.keys():
									datasetsUnsuscribed[site]=[dataset]
								else:
									datasetsUnsuscribed[site].append(dataset)
							
			
	return datasetsUnsuscribed


def makeSubscriptions(url, datasetsUnsuscribed):
	for site in datasetsUnsuscribed.keys():
		phedexSubscription.makeCustodialMoveRequest(url, site, datasetsUnsuscribed[site], "Custodial Move Subscription for MonteCarlo")



def main():
	url='cmsweb.cern.ch'
	print "Gathering Requests"
	requests=closeOutWorkflows.getOverviewRequest()
	print "Classifying Requests"
	datasetsUnsuscribed=classifyRunningRequests(url, requests)
	print "Making Suscription"
	makeSubscriptions(url, datasetsUnsuscribed)
	sys.exit(0);

if __name__ == "__main__":
	main()
