#!/usr/bin/env python
#import json
import sys
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import optparse

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
    parser = optparse.OptionParser()
    parser.add_option('--correct_env',action="store_true",dest='correct_env')
    (options,args) = parser.parse_args()

    command=""
    for arg in sys.argv:
        command=command+arg+" "

    if not options.correct_env:
        os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; python2.6 "+command + "--correct_env")
        sys.exit(0)
    
    #args=sys.argv[1:]
    if not len(args)==2:
        print "usage: python2.6 setrequeststatus.py <text_file_with_the_workflow_names> <newStatus>"
        sys.exit(0)
    inputFile = args[0]
    newstatus = args[1]
    f = open(inputFile, 'r')
    url = 'cmsweb.cern.ch'

    for line in f:
        workflow = line.rstrip('\n')
#        print "Set %s from %s to %s" % (workflow,getStatus(url, workflow),newstatus)
        setStatus(url, workflow, newstatus)
        print "Final status is: %s"  % getStatus(url, workflow)
    f.close

if __name__ == "__main__":
    main()
