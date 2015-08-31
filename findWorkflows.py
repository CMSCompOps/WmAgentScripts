#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, random
from xml.dom.minidom import getDOMImplementation
import reqMgrClient as reqMgrClient

"""
    Filters through the list of workflows in the overview
    with a given criteria
"""

def getOverviewRequestsWMStats(url):
    """
    Retrieves workflows overview from WMStats
    by querying couch db JSON direcly
    """
    #TODO use the couch API from WMStatsClient instead of wmstats URL
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                     key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET",
                 "/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype?stale=update_after")
    
    #conn.request("GET", '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatus?key="completed"')
    response = conn.getresponse()
    data = response.read()
    conn.close()
    myString=data.decode('utf-8')
    workflows=json.loads(myString)['rows']
    return workflows


def getReRecos(url, requests):
    """
    Sorts requests using the type.
    type of workflows.
    """
    rerecos = []
    for request in requests:
        name=request['id']
        #if a wrong or weird name
        if len(request['key'])<3:
            print request
            continue
        status=request['key'][1]
        #filter out rejected
        if status is None or 'aborted' in status or 'rejected' in status:
            continue

        requestType=request['key'][2]
        #only ReReco's
        if requestType != 'ReReco':
            continue
        rerecos.append(name)            
    return rerecos

def findIncludeParents(url, wfs):
    result = []
    for r in wfs:
        try:
            wf = reqMgrClient.Workflow(r, url=url)
            if 'IncludeParents' in wf.info:
                #print wf.name, wf.info['IncludeParents']
                if wf.info['IncludeParents'] == "True" or wf.info['IncludeParents'] is True:
                    print wf.name, wf.status
                    result.append(wf.name)
            else:
                #print "-",wf.name
                pass
        except AttributeError:
            #print "Error retrieving info for ", r
            pass
    return result

def main():
    url='cmsweb.cern.ch'
    #url='cmsweb-testbed.cern.ch'
    print "Gathering Requests"
    requests = getOverviewRequestsWMStats(url)
    print "Only ReRecos"
    rerecos = getReRecos(url, requests)
    print len(rerecos)
    print "Filtering ReRecos with IncludeParents=True"
    wfs = findIncludeParents(url, rerecos)
    #for r in wfs:
    #    print '\t'.join(r)
    
if __name__ == "__main__":
    main()

