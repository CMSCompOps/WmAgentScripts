#!/usr/bin/env python
"""
Abort a given list of workflows and then clone them.
This can be used when input workflows are in status: acquired
Please use rejectAndClone for workflows in status assigened or assignment-approved
The original existing requests are not touched. The cloned requests have a newly generated RequestName, 
new timestamp, RequestDate, however -everything- else is copied from the original request.
    input arg: Text file with list of workflows.

NOTE: ProcessingVersion will not be increased with this script! You can use this to abort running-open/closed request but you have to manually increse it before assign. 
"""

import urllib2,urllib, httplib, sys, re, os

try:
    import json
except:
    import simplejson as json


def abortWorkflow(url, workflowname):
    """
    This uses ReqMgr rest api to change the status of a request to aborted
    """
    print "Aborting workflow: " + workflowname
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    params = {"requestName" : workflowname,"status" : "aborted"}
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
    """
    This uses ReqMgr rest api to clone a request
    """
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    headers={"Content-Length": 0}
    params = {}
    encodedParams = urllib.urlencode(params)
    r1=conn.request("PUT", "/reqmgr/reqMgr/clone/"+workflowname, encodedParams, headers)
    r2=conn.getresponse()
    data = json.loads(r2.read())
    requestName=data.values()[0]['RequestName']
    return requestName

def workflownamesfromFile(filename):
    """
    This takes a given text file and extracts info from each row
    """
    workflows=[]
    f=open(filename,'r')
    for workflow in f:
        #This line is to remove the carrige return    
        workflow = workflow.rstrip('\n').replace(' ','')
        workflow = workflow.replace(' ','')
        if workflow != '': workflows.append(workflow)
    return workflows

def main():
    """
    Read the text file, for each workflow try:
    First abort it, then clone it.
    """
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:abortAndClone file.txt"
        sys.exit(0)
    url='cmsweb.cern.ch'
    filename=args[0]
    workflows=workflownamesfromFile(filename)
    for workflow in workflows:
        abortWorkflow(url, workflow)
        print "Aborted workflow: " + workflow
        print "Cloning workflow..."
        cloned = cloneWorkflow(url, workflow)
        print "Cloned workflow: " + cloned
    sys.exit(0);

if __name__ == "__main__":
    main()
