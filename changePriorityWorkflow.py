#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest, closeOutWorkflows
from xml.dom.minidom import getDOMImplementation



def changePriorityWorkflow(url, workflow, priority):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	params = {"requestName" : workflow,"priority" : priority}
    	headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
    	encodedParams = urllib.urlencode(params)
    	conn.request("PUT", "/reqmgr/reqMgr/request", encodedParams, headers)
    	response = conn.getresponse()	
    	print response.status, response.reason
    	data = response.read()
    	print data
    	conn.close()
def main():
	args=sys.argv[1:]
	if not len(args)==2:
		print "usage: workflowname priority"
		sys.exit(0)
	workflow=args[0]
	priority=args[1]
	url='cmsweb.cern.ch'
	changePriorityWorkflow(url, workflow, priority)
	sys.exit(0);

if __name__ == "__main__":
	main()
