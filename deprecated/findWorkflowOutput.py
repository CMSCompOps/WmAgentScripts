#!/usr/bin/env python

import urllib2, urllib, httplib, sys, re, os, json, time, math, locale, re
from deprecated import dbsTest
import optparse
from deprecated import phedexSubscription
from xml.dom.minidom import getDOMImplementation

def classifyRequests(requests, dataset):
	for request in requests:
	    name=request['request_name']
	    outputDatasets=deprecated.phedexSubscription.outputdatasetsWorkflow('cmsweb.cern.ch',name)	
	    for out in outputDatasets:
		if dataset in out:
			print name
	return "None"






def main():
	
	url='vocms204.cern.ch'
	parser = optparse.OptionParser()
	parser.add_option('-d', '--dataset', help='Dataset',dest='dataset')
	(options,args) = parser.parse_args()
	if not options.dataset:
		print "Write a dataset"
		sys.exit(0);
	dataset=options.dataset
	print "Gathering request"
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/monitorSvc/requestmonitor')
	r2=conn.getresponse()
        requests = json.read(r2.read())
	print "Clasifying Request"
	print classifyRequests(requests, dataset)
	
	sys.exit(0);

if __name__ == "__main__":
	main()
