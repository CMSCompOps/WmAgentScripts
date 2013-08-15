#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, json, phedexSubscription
from xml.dom.minidom import getDOMImplementation

def AnnounceWorkflow(url, workflowname):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	params = {"requestName" : workflowname, "cascade" : True}
	headers={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        encodedParams = urllib.urlencode(params)
        conn.request("POST", "/reqmgr/reqMgr/announce", encodedParams, headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()

def main():
	args=sys.argv[1:]
	if not len(args)==1:
		print "usage:AnnounceWF.py file name" 
                print "where the file should contain a list of workflows"
		sys.exit(0)

	filename=args[0]
	url='cmsweb.cern.ch'
        workflows=phedexSubscription.workflownamesfromFile(filename)
        for workflow in workflows:
           AnnounceWorkflow(url, workflow)
	   print workflow+" announced"
	sys.exit(0);

if __name__ == "__main__":
	main()

