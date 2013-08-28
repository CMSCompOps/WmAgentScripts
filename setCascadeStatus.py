#!/usr/bin/env python
#import json
import sys
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation

def setStatus(url, workflowname,newstatus):
    print "Setting %s to %s" % (workflowname,newstatus)
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    headers={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    params = {"requestName" : workflowname, "cascade" : True}
    encodedParams = urllib.urlencode(params)
    if newstatus == "closed-out":
        conn.request("POST", "/reqmgr/reqMgr/closeout", encodedParams, headers)
    elif newstatus == "announced":
        conn.request("POST", "/reqmgr/reqMgr/announce", encodedParams, headers)
    else:
        params = {"requestName" : workflowname,"status" : newstatus}
        encodedParams = urllib.urlencode(params)
        conn.request("PUT", "/reqmgr/reqMgr/request", encodedParams, headers)
    response = conn.getresponse()
    print response.status, response.reason
    data = response.read()
#    print data
    conn.close()

def getStatus(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
    r2=conn.getresponse()
    #data = r2.read()
    s = json.loads(r2.read())
    t = s['RequestStatus']
    return t

def main():
    args=sys.argv[1:]
    if not len(args)==2:
        print "usage: python ../RelVal/setrequeststatus.py <inputFile_containing_a_list_of_workflows> <new_status>"
        sys.exit(0)
    inputFile = args[0]
    newstatus = args[1]
    f = open(inputFile, 'r')
    url = 'cmsweb-testbed.cern.ch'

    for line in f:
        workflow = line.rstrip('\n')
#        print "Set %s from %s to %s" % (workflow,getStatus(url, workflow),newstatus)
        setStatus(url, workflow, newstatus)
        print "Final status is: %s"  % getStatus(url, workflow)
    f.close

if __name__ == "__main__":
    main()
