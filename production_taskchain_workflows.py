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
    conn.request("GET", "/couchdb/wmstats/_design/WMStats/_view/requestByStatusAndType?stale=update_after")
    response = conn.getresponse()
    data = response.read()
    
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
        else:
            var=var+1
    result=[["Workflow Status","Production","TaskChain"]]
    
    result.append(["new",new.count("new"),new.count("TaskChain")])
    result.append(["assignment-approved",apr.count("assignment-approved"),apr.count("TaskChain")])
    result.append(["assigned",asg.count("assigned"),asg.count("TaskChain")])
    result.append(["acquired",acq.count("acquired"),acq.count("TaskChain")])
    result.append(["running-open",runop.count("running-open"),runop.count("TaskChain")])
    result.append(["running-closed",runcl.count("running-closed"),runcl.count("TaskChain")])
    result.append(["aborted",abt.count("aborted"),abt.count("TaskChain")])
    result.append(["failed",fail.count("failed"),fail.count("TaskChain")])
    result.append(["completed",com.count("completed"),com.count("TaskChain")])
    result.append(["closed-out",csd.count("closed-out"),csd.count("TaskChain")])
    
#    Print the results
    print("%20s %15s %15s" %( result[0][0], result[0][1], result[0][2] ) )
    print("%20s %15s %15s" %( "--------------------", "---------------" , "---------------" ) )
    print("%20s %15s %15s" %( result[1][0], result[1][1]-result[1][2], result[1][2] ) )
    print("%20s %15s %15s" %( result[2][0], result[2][1]-result[2][2], result[2][2] ) )
    print("%20s %15s %15s" %( result[3][0], result[3][1]-result[3][2], result[3][2] ) )
    print("%20s %15s %15s" %( result[4][0], result[4][1]-result[4][2], result[4][2] ) )
    print("%20s %15s %15s" %( result[5][0], result[5][1]-result[5][2], result[5][2] ) )
    print("%20s %15s %15s" %( result[6][0], result[6][1]-result[6][2], result[6][2] ) )
    print("%20s %15s %15s" %( result[7][0], result[7][1]-result[7][2], result[7][2] ) )
    print("%20s %15s %15s" %( result[8][0], result[8][1]-result[8][2], result[8][2] ) )
    print("%20s %15s %15s" %( result[9][0], result[9][1]-result[9][2], result[9][2] ) )
    print("%20s %15s %15s" %( result[10][0], result[10][1]-result[10][2], result[10][2] ) )
    print("%20s %15s %15s" %( "", "__________" , "__________" ) )
    
#    Overalls 
    ov1=sum([result[1][1],result[2][1],result[3][1],result[4][1],result[5][1],
             result[6][1],result[7][1],result[8][1],result[9][1],result[10][1]])
    ov2=sum([result[1][2],result[2][2],result[3][2],result[4][2],result[5][2],
             result[6][2],result[7][2],result[8][2],result[9][2],result[10][2]])
    print("%20s %15s %15s" %( "Overall", ov1 - ov2 , ov2 ) )

#    Connection must be closed before finishing
    conn.close()

def main():
    getOveralls()
    sys.exit(0);

if __name__ == "__main__":
    main()