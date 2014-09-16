#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation


url='cmsweb.cern.ch'

#TODO call reqMgrClient.py
def changePriorityWorkflow(url, workflow, priority):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	params = {workflow+":status":"",workflow+":priority" : str(priority)}
    	headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
    	encodedParams = urllib.urlencode(params)
    	conn.request("PUT", "/reqmgr/view/doAdmin", encodedParams, headers)
    	response = conn.getresponse()	
    	print response.status, response.reason
    	data = response.read()
    	print data
    	conn.close()

def main():
    args = sys.argv[1:]
    if len(args) == 2:
        #get workflow and priority
        workflow = args[0]
        priority = args[1]
        #and change it
        changePriorityWorkflow(url, workflow, priority)
        sys.exit(0);
    elif len(args) == 3 and args[0] == '-f':
        #read from files
        wfs = [l.strip() for l in open(args[1]).readlines() if l.strip()]
        #get priority
        priority = args[2]
        #repeat for everyone
        for wf in wfs:
            changePriorityWorkflow(url, wf, priority)
    else:
        print "usage: (workflowname | -f file) priority"
        sys.exit(0)


if __name__ == "__main__":
    main()
