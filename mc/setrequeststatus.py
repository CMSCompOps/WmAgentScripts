#!/usr/bin/env python
import sys
try:
	import json
except:
	import simplejson as json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
reqmgrsocket='http://localhost:8687'

def getRequestByrequestName(workflow):
        url=reqmgrsocket + '/reqmgr/reqMgr/request?requestName=' + workflow
	try:
        	s = json.load(urllib.urlopen(url))
	except:
		print "Cannot get request"
		print "Did you tunnel correctly?"
		sys.exit(1)
	return s

def setStatus(workflowname,newstatus):
    print "Setting %s to %s" % (workflowname,newstatus)
    conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    headers={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    params = {"requestName" : workflowname,"status" : newstatus}
    encodedParams = urllib.urlencode(params)
    conn.request("PUT", "/reqmgr/reqMgr/request", encodedParams, headers)
    response = conn.getresponse()
    print response.status, response.reason
    data = response.read()
    conn.close()

def getStatus(workflow):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	s = json.loads(data)
	t = s['RequestStatus']
        return t

def main():
	print
	w = sys.argv[1]
	newst = sys.argv[2]

        print "Set %s from %s to %s" % (w,getStatus(w),newst)
	setStatus(w,newst)
	print "Final status is: %s"  % getStatus(w)

if __name__ == "__main__":
        main()
