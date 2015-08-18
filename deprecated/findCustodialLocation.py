#!/usr/bin/env python

import urllib2, urllib, httplib, sys, re, os, json, time, math, locale
from deprecated import dbsTest
import optparse, closeOutWorkflows
from xml.dom.minidom import getDOMImplementation

def getInputDataset(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.read(r2.read())
	if 'InputDataset' in request.keys():
		return request['InputDataset']
	else:
		return "None"

def findCustodialLocation(url, dataset):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
	r2=conn.getresponse()
	result = json.read(r2.read())
	request=result['phedex']
	if 'block' not in request.keys():
		return "No Site"
	if len(request['block'])==0:
		return "No Site"
	for replica in request['block'][0]['replica']:
		if replica['custodial']=="y":
			return replica['node']
	return "No Custodial Site found"		
	
		
	


def main():
	url='cmsweb.cern.ch'	
	siteList=['CNAF', 'FNAL', 'IN2P3', 'PIC', 'KIT', 'ASGC', 'RAL']
	parser = optparse.OptionParser()
	parser.add_option('-d', '--dataset', help='Name of the dataset', dest='dataset')
	parser.add_option('-r', '--request', help='Name of the request', dest='request')
	(options,args) = parser.parse_args()
	if not options.dataset and not options.request:
		print "Must specify a request or a dataset"
		sys.exit(0);
	if options.dataset and options.request:
		print "Must specify request or dataset not BOTH"
		sys.exit(0);
	dataset=""
 	if options.dataset:
		dataset=options.dataset
	if options.request:
		dataset=getInputDataset(url, options.request)
		if dataset=="None":
			print "Request withouh input dataset"
	site=findCustodialLocation(url,dataset)	
	print site
	sys.exit(0);

if __name__ == "__main__":
	main()
