#!/usr/bin/env python

import urllib2, urllib, httplib, sys, re, os, json
from deprecated import phedexSubscription
from deprecated import dbsTest
from xml.dom.minidom import getDOMImplementation

#Tests whether a dataset was subscribed to phedex
def testOutputDataset(datasetName, requestType):
	 url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/Subscriptions?dataset=' + datasetName
         result = json.read(urllib2.urlopen(url).read())
	 datasets=result['phedex']['dataset']
	 if len(datasets)>0:
		dicts=datasets[0]
		subscriptions=dicts['subscription']
		for subscription in subscriptions:
			if subscription['level']=='DATASET' and subscription['custodial']=='y':
				if 'MonteCarlo' in requestType:
					if subscription['custodial']=='y':
						print "This dataset is subscribed : "+ datasetName
						print "Custodial: "+subscription['custodial']
						request=subscription['request']
						print "Request page: https://cmsweb.cern.ch/phedex/prod/Request::View?request="+str(request)
						return
				else:
					print "This dataset is subscribed : "+ datasetName
					print "Custodial: "+subscription['custodial']
					request=subscription['request']
					print "Request page: https://cmsweb.cern.ch/phedex/prod/Request::View?request="+str(request)
					return
			else:
				print "The Subscription exist but not custodial"
				request=subscription['request']
				print "Request page: https://cmsweb.cern.ch/phedex/prod/Request::View?request="+str(request)
				
	 else:
		print "This dataset wasn't subscribed: "+ datasetName

def main():
	args=sys.argv[1:]
	if not len(args)==1:
		print "usage:dbsTest workflowname"
		sys.exit(0)
	workflow=args[0]
	url='cmsweb.cern.ch'
	requestType=deprecated.dbsTest.getWorkflowType(url, workflow)
	datasets=deprecated.phedexSubscription.outputdatasetsWorkflow(url, workflow)
	for datasetName in datasets:
		testOutputDataset(datasetName, requestType)
	sys.exit(0);

if __name__ == "__main__":
	main()
	
