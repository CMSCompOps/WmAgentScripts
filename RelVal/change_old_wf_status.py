#!/usr/bin/env python
#import json
import datetime
import sys
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import optparse

import calendar

initialstatus='assignment-approved'
finalstatus='rejected'

def setStatus(url, workflowname,newstatus):
    #print "Setting %s to %s" % (workflowname,newstatus)
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
    print "    response of status change request: "+str(response.status)+", "+str(response.reason)
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
        os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536;  python2.6 "+command + "--correct_env")
        sys.exit(0)
    
    #args=sys.argv[1:]
    if not len(args)==0:
        print "usage: python2.6 abort_reject_or_announce.py"
        sys.exit(0)

    url = 'cmsweb.cern.ch'


    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))      
    r=conn.request('GET','/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatus?key="'+initialstatus+'"&stale=ok')
    r=conn.getresponse()
    data = r.read()
    s = json.loads(data)


    #print s['rows']
    #sys.exit(0)

    n_rvcmssw_wfs = 0
    for i in s['rows']:
        if 'RVCMSSW' in i['id']:
            n_rvcmssw_wfs=n_rvcmssw_wfs+1

    print n_rvcmssw_wfs

    count = 0

    for i in s['rows']:
        if 'RVCMSSW' in i['id']:
            
            print str(count)+" out of " + str(n_rvcmssw_wfs)
            count = count+1
            workflow=i['id']
            print "checking workflow " +workflow
            conn2  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            r2=conn.request('GET','/couchdb/reqmgr_workload_cache/_all_docs?keys=["'+workflow+'"]&include_docs=true')
            r2=conn.getresponse()
            data = r2.read()
            s = json.loads(data)

            print s['rows'][0]['doc']['RequestTransition'][len(s['rows'][0]['doc']['RequestTransition'])-1]
            if s['rows'][0]['doc']['RequestTransition'][len(s['rows'][0]['doc']['RequestTransition'])-1]['Status'] != initialstatus:
                continue
            #print "    in "+initialstatus+" for "+str((calendar.timegm(datetime.datetime.utcnow().utctimetuple()-s['rows'][0]['doc']['RequestTransition'][len(s['rows'][0]['doc']['RequestTransition'])-1]['UpdateTime'])/86400.)+" days"
            if (calendar.timegm(datetime.datetime.utcnow().utctimetuple())-s['rows'][0]['doc']['RequestTransition'][len(s['rows'][0]['doc']['RequestTransition'])-1]['UpdateTime'])/86400. > 14.:
                print "    moving workflow to "+finalstatus
                setStatus(url, workflow, finalstatus)

if __name__ == "__main__":
    main()
