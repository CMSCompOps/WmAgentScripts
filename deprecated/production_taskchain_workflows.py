#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
from xml.dom.minidom import getDOMImplementation

'''
Created on Aug 29, 2013
This Script get the instantaneous overall amounts of Production and Task Chain workflows. 
The amounts are clisefied per status.
@author: lucacopa
'''

def getOveralls():
    # Set the conection to the server and get the response from WMStats
    url="cmsweb.cern.ch"
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), 
                                     key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET", 
                 "/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype?stale=update_after")
    response = conn.getresponse()
    data = response.read()
    conn.close() # Connection is closed
        
    # Decode the streamed data to unicode
    myString=data.decode('utf-8')
    # Create a json dictonary from streaming data for workflows
    workflows=json.loads(myString)['rows']
    # Go through all the workflows and count workflow type per each status
    wDict={}
    for workflow in workflows:
        workflowStatus=workflow['key'][1].encode('utf-8')
        workflowType=workflow['key'][2].encode('utf-8')
        if workflowStatus not in wDict:# add status only when find it
            wDict[workflowStatus]={}
            # Found the first
            wDict[workflowStatus][workflowType]=1 
        else:
            if workflowType in wDict[workflowStatus]:
                # Here you do the counting
                wDict[workflowStatus][workflowType]=wDict[workflowStatus][workflowType]+1
            else:
                # Found the first of this workflowType
                wDict[workflowStatus][workflowType]=1 

    # These are the interesting statuses for Monitoring
    wStatus=['new',
           'assignment-approved',
           'assigned',
           'acquired',
           'running-open',
           'running-closed',
           'aborted',
           'failed',
           'rejected',
           'completed',
           'closed-out']
    
#    Print the results in a table
    print "%20s %15s %15s" %( 'Workflow Status','Production', 'TaskChain' ) 
    print "%20s %15s %15s" %( '-'*20,'-'*15,'-'*15 )   
    
    # These are the overalls
    overallProd=0
    overallTaskchain=0
    
    for interestStatus in wStatus:
        if interestStatus in wDict:# Look for the interesting statuses for monitoring in wDict
            # Count all the workflows for each status
            totalWorkflows=sum(wDict[interestStatus].values())
            # If there is TaskChain workflows, calculate how many. If not assume 0
            if 'TaskChain' in wDict[interestStatus]:
                taskchainWorkflows=wDict[interestStatus]['TaskChain']
            else:
                taskchainWorkflows=0
            # Calculate all the production workflows (non-Taskchain)
            productionWorkflows=totalWorkflows-taskchainWorkflows
            # Acumulate the overalls
            overallProd=overallProd+productionWorkflows
            overallTaskchain=overallTaskchain+taskchainWorkflows
            print "%20s %15s %15s" %( interestStatus,productionWorkflows,taskchainWorkflows ) 
        else:# If the interesting status is not found, it assumes there are 0 workflows
            print "%20s %15s %15s" %( interestStatus,0,0 )   
    # Print the Overalls
    print "%20s %15s %15s" %( '','_'*10,'_'*10 ) 
    print "%20s %15s %15s" %( '',overallProd,overallTaskchain )       

def main():
    getOveralls()
    sys.exit(0);

if __name__ == "__main__":
    main()