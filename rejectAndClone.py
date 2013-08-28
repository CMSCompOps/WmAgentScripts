#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, json, phedexSubscription

def rejectWorkflow(url, workflowname):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    params = {"requestName" : workflowname,"status" : "rejected"}
    headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
    encodedParams = urllib.urlencode(params)
    conn.request("PUT", "/reqmgr/reqMgr/request", encodedParams, headers)
    response = conn.getresponse()	
    #print response.status, response.reason
    data = response.read()
    #print data
    conn.close()

def cloneWorkflow(url, workflowname):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	headers={"Content-Length": 0}
	params = {}
	encodedParams = urllib.urlencode(params)
	r1=conn.request("PUT", "/reqmgr/reqMgr/clone/"+workflowname, encodedParams, headers)
	r2=conn.getresponse()
	data = json.loads(r2.read())
	requestName=data.values()[0]['RequestName']
	return requestName

def main():
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:rejectAndClone file.txt"
        sys.exit(0)
    url='cmsweb.cern.ch'
    filename=args[0]
    workflows=phedexSubscription.workflownamesfromFile(filename)
    for workflow in workflows:
	rejectWorkflow(url, workflow)
	print cloneWorkflow(url, workflow)
    sys.exit(0);

if __name__ == "__main__":
    main()
