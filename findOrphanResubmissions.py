#!/usr/bin/env python
import json
import http.client, os
import reqMgrClient as reqMgrClient

"""
    Filters through the list of ACDC's that are in "completed" which ones
    have it's original workflow in a status beyond (archived, closed-out, rejected, etc)
"""


def getOverviewRequestsWMStats(url, requestStatus):
    """
    Retrieves workflows overview from WMStats
    by querying couch db JSON direcly
    """
    # TODO use the couch API from WMStatsClient instead of wmstats URL
    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'),
                                       key_file=os.getenv('X509_USER_PROXY'))



    conn.request("GET", '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatus?key="{}"'.format(requestStatus))
    response = conn.getresponse()
    data = response.read()
    conn.close()
    myString = data.decode('utf-8')
    workflows = json.loads(myString)['rows']
    return workflows


def getResubmissions(url, requests):
    """
    Sorts completed requests using the type.
    returns a dic cointaining a list for each
    type of workflows.
    """
    resubmissions = []
    for request in requests:
        name = request['id']
        # if a wrong or weird name
        if len(request['key']) < 3:
            print(request)
            continue
        #if 'ACDC' not in name:
        #    continue
        status = request['key']
        # only completed requests
        #if status != 'completed':

        #    continue
        requestDetails = reqMgrClient.Workflow(name).info
        requestType = requestDetails['RequestType']

        if requestType != 'Resubmission':
            continue
        resubmissions.append(name)
    return resubmissions


def filterOrphanResubmissions(url, resubmissions):
    orphans = []
    for wfname in resubmissions:
        resubmission = reqMgrClient.Workflow(wfname)
        origwf = None
        # original workflow
        if 'OriginalRequestName' in resubmission.info:
            origwf = resubmission.info['OriginalRequestName']
        elif 'OriginalRequestName' in resubmission.cache:
            origwf = resubmission.cache['OriginalRequestName']
        if origwf:
            origwf = reqMgrClient.Workflow(origwf)
            if origwf.status != 'completed':
                orphans.append((origwf.name, origwf.status, resubmission.name))
    return orphans


def main():
    url = 'cmsweb.cern.ch'

    active_statuses = ['new', 'assignment-approved', 'assigned', 'staging', 'staged', 'acquired', 'running-open',
                       'running-closed', 'closed-out', 'failed']


    for status in active_statuses:
        print("Gathering Requests that are in status {}".format(status))
        requests = getOverviewRequestsWMStats(url, status)
        print("Filtering only resubmissions")
        resubmissions = getResubmissions(url, requests)
        print("Number of resubmissions in this status: {}".format(str(len(resubmissions))))
        print("Filtering orphan resubmissions")
        orphan = filterOrphanResubmissions(url, resubmissions)
        for o in orphan:
            print('\t'.join(o))
            print("")


if __name__ == "__main__":
    main()
