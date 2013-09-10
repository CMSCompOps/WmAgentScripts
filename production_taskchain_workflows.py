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
#    Set the conection to the server and get the response from WMStats
    url="cmsweb.cern.ch"
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), 
                                     key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET", 
                 "/couchdb/wmstats/_design/WMStats/_view/requestByStatusAndType?stale=update_after")
    response = conn.getresponse()
    data = response.read()
    
#    Connection must be closed
    conn.close()
        
#    Extract the workflow name, status and type
    myString=data.decode("utf-8")
    myDict=json.loads(myString)
    workflows=myDict["rows"]
    
    i=0
    e=0
    wDict={}
    while i==0:
        if workflows:
            row=workflows.pop(0)
            wDict[e]=row["key"]
            e=e+1
        else:
            i=1

#    Look for the total amount of each workflow status

#    var is intended to look into the dictionary (wDict) an pop the type and status
#    of the first workflow (0), then var=var+1 step foward to the next workflow.
    var=0
    
    new=[]
    apr=[]
    asg=[]
    acq=[]
    runop=[]
    runcl=[]
    abt=[]
    fail=[]
    com=[]
    csd=[]
    rjc=[]
    while var<len(wDict):
        wtype=wDict[var].pop()
        wstatus=wDict[var].pop()
        if wstatus=="new":
            new.append(wstatus)
            if wtype=="TaskChain":
                new.append(wtype)
            var=var+1
        elif wstatus=="assignment-approved":
            apr.append(wstatus)
            if wtype=="TaskChain":
                apr.append(wtype)
            var=var+1
        elif wstatus=="assigned":
            asg.append(wstatus)
            if wtype=="TaskChain":
                asg.append(wtype)
            var=var+1
        elif wstatus=="acquired":
            acq.append(wstatus)
            if wtype=="TaskChain":
                acq.append(wtype)
            var=var+1
        elif wstatus=="running-open":
            runop.append(wstatus)
            if wtype=="TaskChain":
                runop.append(wtype)
            var=var+1
        elif wstatus=="running-closed":
            runcl.append(wstatus)
            if wtype=="TaskChain":
                runcl.append(wtype)
            var=var+1
        elif wstatus=="aborted":
            abt.append(wstatus)
            if wtype=="TaskChain":
                abt.append(wtype)
            var=var+1
        elif wstatus=="failed":
            fail.append(wstatus)
            if wtype=="TaskChain":
                fail.append(wtype)
            var=var+1
        elif wstatus=="completed":
            com.append(wstatus)
            if wtype=="TaskChain":
                com.append(wtype)
            var=var+1
        elif wstatus=="closed-out":
            csd.append(wstatus)
            if wtype=="TaskChain":
                csd.append(wtype)
            var=var+1
        elif wstatus=="rejected":
            rjc.append(wstatus)
            if wtype=="TaskChain":
                rjc.append(wtype)
            var=var+1
        else:
            var=var+1
            
    sumworkflows=[["Workflow Status","Production","TaskChain"]]
    
    sumworkflows.append(["new",
                   new.count("new"),new.count("TaskChain")])
    sumworkflows.append(["assignment-approved",
                   apr.count("assignment-approved"),apr.count("TaskChain")])
    sumworkflows.append(["assigned",
                   asg.count("assigned"),asg.count("TaskChain")])
    sumworkflows.append(["acquired",
                   acq.count("acquired"),acq.count("TaskChain")])
    sumworkflows.append(["running-open",
                   runop.count("running-open"),runop.count("TaskChain")])
    sumworkflows.append(["running-closed",
                   runcl.count("running-closed"),runcl.count("TaskChain")])
    sumworkflows.append(["aborted",
                   abt.count("aborted"),abt.count("TaskChain")])
    sumworkflows.append(["failed",
                   fail.count("failed"),fail.count("TaskChain")])
    sumworkflows.append(["completed",
                   com.count("completed"),com.count("TaskChain")])
    sumworkflows.append(["closed-out",
                   csd.count("closed-out"),csd.count("TaskChain")])
    sumworkflows.append(["rejected",
                   rjc.count("rejected"),rjc.count("TaskChain")])
    
#    Print the results in a table
    print("%20s %15s %15s" %( sumworkflows[0][0],
                               sumworkflows[0][1], sumworkflows[0][2] ) )
    print("%20s %15s %15s" %( "--------------------",
                               "---------------" , "---------------" ) )
    print("%20s %15s %15s" %( sumworkflows[1][0],
                               sumworkflows[1][1]-sumworkflows[1][2], 
                               sumworkflows[1][2] ) )
    print("%20s %15s %15s" %( sumworkflows[2][0],
                               sumworkflows[2][1]-sumworkflows[2][2], 
                               sumworkflows[2][2] ) )
    print("%20s %15s %15s" %( sumworkflows[3][0],
                               sumworkflows[3][1]-sumworkflows[3][2], 
                               sumworkflows[3][2] ) )
    print("%20s %15s %15s" %( sumworkflows[4][0],
                               sumworkflows[4][1]-sumworkflows[4][2], 
                               sumworkflows[4][2] ) )
    print("%20s %15s %15s" %( sumworkflows[5][0],
                               sumworkflows[5][1]-sumworkflows[5][2], 
                               sumworkflows[5][2] ) )
    print("%20s %15s %15s" %( sumworkflows[6][0],
                               sumworkflows[6][1]-sumworkflows[6][2], 
                               sumworkflows[6][2] ) )
    print("%20s %15s %15s" %( sumworkflows[7][0],
                               sumworkflows[7][1]-sumworkflows[7][2], 
                               sumworkflows[7][2] ) )
    print("%20s %15s %15s" %( sumworkflows[8][0],
                               sumworkflows[8][1]-sumworkflows[8][2], 
                               sumworkflows[8][2] ) )
    print("%20s %15s %15s" %( sumworkflows[9][0],
                               sumworkflows[9][1]-sumworkflows[9][2], 
                               sumworkflows[9][2] ) )
    print("%20s %15s %15s" %( sumworkflows[10][0],
                               sumworkflows[10][1]-sumworkflows[10][2], 
                               sumworkflows[10][2] ) )
    print("%20s %15s %15s" %( sumworkflows[11][0],
                               sumworkflows[11][1]-sumworkflows[11][2], 
                               sumworkflows[11][2] ) )
    print("%20s %15s %15s" %( "", "__________" , "__________" ) )
    
#    Workflows overalls
    ov1=sum([sumworkflows[1][1],sumworkflows[2][1],sumworkflows[3][1],
             sumworkflows[4][1],sumworkflows[5][1],
             sumworkflows[6][1],sumworkflows[7][1],sumworkflows[8][1],
             sumworkflows[9][1],sumworkflows[10][1],sumworkflows[11][1]])
    ov2=sum([sumworkflows[1][2],sumworkflows[2][2],sumworkflows[3][2],
             sumworkflows[4][2],sumworkflows[5][2],
             sumworkflows[6][2],sumworkflows[7][2],sumworkflows[8][2],
             sumworkflows[9][2],sumworkflows[10][2],sumworkflows[11][2]])
    print("%20s %15s %15s" %( "Overall", ov1 - ov2 , ov2 ) )

def main():
    getOveralls()
    sys.exit(0);

if __name__ == "__main__":
    main()