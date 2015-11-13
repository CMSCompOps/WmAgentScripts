#!/usr/bin/env python

import urllib2, urllib, httplib, sys, re, os, json
from deprecated import phedexSubscription
from xml.dom.minidom import getDOMImplementation

def findCustodialLocation(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
        r2=conn.getresponse()
        result = json.loads(r2.read())
        request=result['phedex']
        if 'block' not in request.keys():
                return "No Site"
        if len(request['block'])==0:
                return "No Site"
        for replica in request['block'][0]['replica']:
                if replica['custodial']=="y" and replica['node']!="T0_CH_CERN_MSS":
                        return replica['node']
        return "No Custodial Site found"

def getPrepID(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	prepID=request['PrepID']
	return prepID

def getInputDataSet(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	inputDataSets=request['InputDataset']
	if len(inputDataSets)<1:
		print "No InputDataSet for workflow " +workflow
	else:
		return inputDataSets

def main():
	args=sys.argv[1:]
	if not len(args)==1:
		print "usage:listReqTapeFamilies.py filename" 
                print "where the file should contain a list of workflows"
		sys.exit(0)

        sites = ['T1_DE_KIT', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_ES_PIC', 'T1_TW_ASGC', 'T1_UK_RAL', 'T1_US_FNAL']

	filename=args[0]
	url='cmsweb.cern.ch'
        workflows=deprecated.phedexSubscription.workflownamesfromFile(filename)
        for workflow in workflows:
           inputDataset = getInputDataSet(url, workflow)
	   custodialLocation = findCustodialLocation(url, inputDataset)
           print workflow+" "+custodialLocation+" "+inputDataset

	sys.exit(0);

if __name__ == "__main__":
	main()

