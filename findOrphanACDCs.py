#!/usr/bin/env python
import json
import httplib, os
import reqMgrClient as reqMgrClient

"""
    Filters through the list of ACDC's that are in "completed" which ones
    have it's original workflow in a status beyond (archived, closed-out, rejected, etc)
"""

def getOverviewRequestsWMStats(url):
    """
    Retrieves workflows overview from WMStats
    by querying couch db JSON direcly
    """
    #TODO use the couch API from WMStatsClient instead of wmstats URL
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                     key_file = os.getenv('X509_USER_PROXY'))
    #conn.request("GET",
    #             "/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype?stale=update_after")
    
    conn.request("GET", '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatus?key="completed"')
    response = conn.getresponse()
    data = response.read()
    conn.close()
    myString=data.decode('utf-8')
    workflows=json.loads(myString)['rows']
    return workflows


def getAcdcs(url, requests):
    """
    Sorts completed requests using the type.
    returns a dic cointaining a list for each
    type of workflows.
    """
    acdcs = []
    for request in requests:
        name=request['id']
        #if a wrong or weird name
        if len(request['key'])<3:
            print request
            continue
        if 'ACDC' not in name:
            continue
        status=request['key']
        #only completed requests
        if status != 'completed':
            continue
        #requestType=request['key'][2]
        #only acdcs
        #if requestType != 'Resubmission':
        #    continue
        acdcs.append(name)            
    return acdcs

def filterOrphanAcdc(url, acdcs):

    orphans = []
    for wfname in acdcs:
        acdc = reqMgrClient.Workflow(wfname)
        origwf = None
        #original workflow
        if 'OriginalRequestName' in acdc.info:
            origwf = acdc.info['OriginalRequestName']
        elif 'OriginalRequestName' in acdc.cache:
            origwf = acdc.cache['OriginalRequestName']
        if origwf:
            origwf = reqMgrClient.Workflow(origwf)
            if origwf.status != 'completed':
                orphans.append((origwf.name, origwf.status, acdc.name))
    return orphans


def main():
    url='cmsweb.cern.ch'
    print "Gathering Requests"
    requests = getOverviewRequestsWMStats(url)
    print "Only ACDCs"
    acdcs = getAcdcs(url, requests)
    print len(acdcs)
    print "Filtering orphan acdcs"
    orphan = filterOrphanAcdc(url, acdcs)
    for o in orphan:
        print '\t'.join(o)
    
if __name__ == "__main__":
    main()

