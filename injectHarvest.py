"""
Pre-requisites:
 1. a valid proxy in your X509_USER_PROXY variable
 2. wmagent env: source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
"""
from __future__ import print_function

import httplib
import json
import os
import sys
from copy import copy

url = "cmsweb.cern.ch"
reqmgrCouchURL = "https://" + url + "/couchdb/reqmgr_workload_cache"

DEFAULT_DICT = {
    "AcquisitionEra": "UPDATEME",
    "CMSSWVersion": "UPDATEME",
    "Campaign": "UPDATEME",
    "ConfigCacheUrl": "https://cmsweb.cern.ch/couchdb",
    "DQMConfigCacheID": "UPDATEME",
    "DQMHarvestUnit": "UPDATEME",
    "DQMUploadUrl": "UPDATEME",
    "DbsUrl": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/",
    "GlobalTag": "UPDATEME",
    "InputDataset": "UPDATEME",
    "Memory": 2200,
    "PrepID": "UPDATEME",
    "ProcessingString": "UPDATEME",
    "ProcessingVersion": 1,
    "RequestPriority": 999999,
    "RequestString": "UPDATEME",
    "RequestType": "DQMHarvest",
    "ScramArch": "UPDATEME",
    "SizePerEvent": 1600,
    "TimePerEvent": 1}


def main():
    if len(sys.argv) != 2:
        print("Usage: python injectHarvest.py WORKFLOW_NAME")
        sys.exit(0)

    work = retrieveWorkload(sys.argv[1])
    newDict = buildRequest(work)
    if newDict:
        # pprint(newDict)
        print("Creating DQMHarvest workflow for: %s" % sys.argv[1])
        workflow = submitWorkflow(newDict)
        approveRequest(workflow)
    else:
        print("%s either has harvesting disabled or it has no DQM or DQMIO samples in the output" % sys.argv[1])
    sys.exit(0)


def retrieveWorkload(workflowName):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflowName
    conn.request("GET", urn, headers=headers)
    r2 = conn.getresponse()
    request = json.loads(r2.read())["result"][0][workflowName]
    return request


def buildRequest(req_cache):
    newSchema = {}
    if not req_cache.get('EnableHarvesting'):
        return newSchema

    dset = [d for d in req_cache['OutputDatasets'] if d.endswith(tuple(['/DQM', '/DQMIO']))]
    if dset:
        inputDataset = dset.pop()
    else:
        return newSchema

    newSchema = copy(DEFAULT_DICT)
    for k, v in DEFAULT_DICT.iteritems():
        if v != "UPDATEME":
            continue
        if k == 'RequestString':
            newSchema[k] = req_cache[k] + '_HARV'
        elif k == 'InputDataset':
            newSchema[k] = inputDataset
        else:
            if isinstance(req_cache[k], dict):
                # then simply pick the first value, makes no difference in the end
                newSchema[k] = req_cache[k].values()[0]
            else:
                newSchema[k] = req_cache[k]

    return newSchema


def submitWorkflow(schema):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    encodedParams = json.dumps(schema)
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    # print("Submitting new workflow...")
    conn.request("POST", "/reqmgr2/data/request", encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s" % (resp.status, resp.reason))
        print("Error message: %s" % resp.msg.getheader('X-Error-Detail'))
        sys.exit(1)
    data = json.loads(data)
    requestName = data['result'][0]['request']
    print("  Request '%s' successfully created." % requestName)
    return requestName


def approveRequest(workflow):
    # print("Approving request...")
    encodedParams = json.dumps({"RequestStatus": "assignment-approved"})
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    conn.request("PUT", "/reqmgr2/data/request/%s" % workflow, encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s" % (resp.status, resp.reason))
        if hasattr(resp.msg, "x-error-detail"):
            print("Error message: %s" % resp.msg["x-error-detail"])
            sys.exit(2)
    conn.close()
    # print("  Request successfully approved!")
    return


if __name__ == '__main__':
    main()
