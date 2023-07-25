#!/usr/bin/env python
import sys
import os
import pwd
import json
import httplib
import optparse
import resubmit, reqMgrClient
import dbs3Client as dbs3
from optparse import OptionParser
from assignSession import *
from utils import *


"""
    Find the list of WFs that needs revive
    Clone and kill the original WFs
"""

def getRequestsFailed(url):
    """
    Retrieves workflows overview from WMStats
    by querying couch db JSON direcly
    """
    #TODO use the couch API from WMStatsClient instead of wmstats URL
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                     key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET", '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatus?key="failed"')
    response = conn.getresponse()
    data = response.read()
    conn.close()
    myString=data.decode('utf-8')
    workflows=json.loads(myString)['rows']

    print("{} failed WFs in ReqMgr.".format(len(workflows)))
    return workflows


def findReviveList(list_request_dicts):
    """
    Find the list of WFs that needs revive using the list of the request dictionaries
    """
    revived_names = []
    list_request_dicts = [d for d in list_request_dicts if d['key'] == "failed"]

    # Define the abnormal RegMgr and Unified status. If at least one child WF is NOT in abnormal status, do NOT revive the original WF.
    # abnormalRstatus = ["failed", "trouble"] 
    # abnormalUstatus = []

    # Remove the failed WFs that have already been revived
    for request_dict in list_request_dicts:
        request_name = request_dict['id']
        wf = workflowInfo(url, request_name)
        wf_family = wf.getFamilly()
        if (len(wf_family)):
            for wf_scion in wf_family:
                scion_name = wf_scion['RequestName']
                scion_Rstatus = wf_scion['RequestStatus'] # Request status
                # if scion_Rstatus not in abnormalRstatus:
                #     revived_names.append(request_name)
                #     continue

                all_info = session.query(Workflow).filter(Workflow.name == scion_name).all()
                if len(all_info):
                    revived_names.append(request_name)
                    print("{} has already been revived.".format(request_name))
                    continue
                    # scion_info = all_info[0]
                    # if scion_info.status not in abnormalUStatus:
                    #     revived_names.append(request_name)
                    #     continue

    list_request_dicts = [d for d in list_request_dicts if d['id'] not in revived_names]
    print("{} WFs need to be revived".format(len(list_request_dicts)))
    print("{} WFs has been revived or is being revived".format(len(revived_names)))
    return list_request_dicts


def resubmitFailed(list_revive_dicts):
    """
    Resubmit the list of WFs
    """
    uinfo = pwd.getpwuid(os.getuid())
    user = uinfo.pw_name
    group = 'DATAOPS'

    list_revive_dicts = [d for d in list_revive_dicts if d['key'] == "failed"]
    for revive_dict in list_revive_dicts:
        revive_name = revive_dict['id']
        clone = resubmit.cloneWorkflow(revive_name, user, group, verbose=False)


def rejectFailed(url, list_request_dicts):
    """
    Reject the list of WFs and invalidate the datasets
    """
    list_request_dicts = [d for d in list_request_dicts if d['key'] == "failed"]
    for request_dict in list_request_dicts:
        failed_name = request_dict['id']
        print("Rejcting {}...".format(failed_name))
        reqMgrClient.rejectWorkflow(url, failed_name)
        datasets = reqMgrClient.outputdatasetsWorkflow(url, failed_name)
        for ds in datasets:
            dbs3.setDatasetStatus(ds, 'INVALID', files=True)


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-r', '--reject', default=False, action = 'store_true', help='Reject all the failed WFs in RegMgr')
    (options, args) = parser.parse_args()

    url='cmsweb.cern.ch'
    failedWFs = getRequestsFailed(url)
    reviveWFs = findReviveList(failedWFs)

    resubmitFailed(reviveWFs)
    if options.reject:
        rejectFailed(failedWFs)
