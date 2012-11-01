#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest, closeOutWorkflows
from xml.dom.minidom import getDOMImplementation

def classifyRunningRequests(url, requests):
	datasetsUnsuscribed={}
	datasetsUnsuscribedSpecialQueue=[]
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
            print name		 	  
	    team=closeOutWorkflows.getRequestTeam(url, name)	
	    if status=='running':
		if requestType=='MonteCarloFromGEN' or requestType=='MonteCarlo':
			site=closeOutWorkflows.findCustodial(url, name)
			if site=='NoSite':
				continue
			if closeOutWorkflows.getRequestTeam(url, name)=='analysis': # If the request is running in the special queue
				datasetWorkflow=phedexSubscription.outputdatasetsWorkflow(url, request)
				for dataset in datasetWorkflow:
					PhedexSubscriptionDone=phedexSubscription.TestSubscritpionSpecialRequest(url, dataset, 'T2_DE_DESY')
					if not PhedexSubscriptionDone:
						datasetsUnsuscribedSpecialQueue.append(dataset)
	
			else:
				datasetWorkflow=phedexSubscription.outputdatasetsWorkflow(url, name)
				for dataset in datasetWorkflow:
					if dataset == "/SMS-T2tt_Mgluino-225to1200_mLSP-0to1000_8TeV-Pythia6Z/Summer12-START52_V9_FSIM-v1/AODSIM": 
						print "Skipping",dataset
						continue
					percentage=closeOutWorkflows.PercentageCompletion(url, name, dataset)
					if float(percentage)>float(0):
						if not phedexSubscription.TestCustodialSubscriptionRequested(url, dataset, site):
							if site not in datasetsUnsuscribed.keys():
								datasetsUnsuscribed[site]=[dataset]
							else:
								datasetsUnsuscribed[site].append(dataset)
	    else:
		continue
	if len(datasetsUnsuscribedSpecialQueue)>0:				
		phedexSubscription.makeCustodialReplicaRequest(url, 'T2_DE_DESY',datasetsUnsuscribedSpecialQueue, "Replica Subscription for Request in special production queue")		
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
	print
	print datasetsUnsuscribed
	print "Making Suscription"
	makeSubscriptions(url, datasetsUnsuscribed)
	sys.exit(0);

if __name__ == "__main__":
	main()
