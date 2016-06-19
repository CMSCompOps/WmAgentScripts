#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os
import json
import optparse
from xml.dom.minidom import getDOMImplementation


reqmgr_url = 'cmsweb.cern.ch'
dbs3_url = 'https://cmsweb.cern.ch'

def getOutputDset(workflow):
    """
    Fetch list of output datasets from ReqMgr
    Returns a list of strings
    """

    headers = {"Content-type": "application/json", "Accept": "application/json"}

    conn = httplib.HTTPSConnection(reqmgr_url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1 = conn.request("GET",'/reqmgr2/data/request?name='+workflow, headers=headers)
    r2 = conn.getresponse()

    if r2.status != 200:
        os.system('echo \"'+workflow+'\" | mail -s \"announcor.py error 1\" andrew.m.levin@vanderbilt.edu')
        sys.exit(1)

    datasets = json.loads(r2.read())

    datasets = datasets['result']

    datasets =datasets[0]

    datasets = datasets[workflow]

    datasets = datasets['OutputDatasets']

    if len(datasets)== 0:
        print "ERROR: No output datasets for: "+ workflow
    return datasets

def getDBSApi():
    """
    Instantiate the DBS3 Client API
    """
    if 'testbed' in dbs3_url:
        dbs3_url_reader = dbs3_url + '/dbs/int/global/DBSReader'
    else:
        dbs3_url_reader = dbs3_url + '/dbs/prod/global/DBSReader'

    from dbs.apis.dbsClient import DbsApi


    #this needs to come after /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh is sourced    
    dbsApi = DbsApi(url = dbs3_url_reader)
    return dbsApi

def getDsetSummary(dbsApi, dset):
    """
    Get a dataset summary containing num of events, num of files and total size
    Returns a list of dictionary
    """


    summary = dbsApi.listBlockSummaries(dataset = dset)
    return summary

def getNumEvents(dbsApi, dset):
    """
    Get back the dataset summary and returns the num of events
    """
    summary = getDsetSummary(dbsApi, dset)
    # it means the dataset was not produced
    if summary[0]['num_file'] == 0:
        return -1
    return summary[0]['num_event']


def collect_dsets_and_nevents(wf_list):

    dbsApi = getDBSApi()

    dset_list=[]

    for workflow in wf_list:
        outputDataSets = getOutputDset(workflow)
        for dataset in outputDataSets:
            outEvents = getNumEvents(dbsApi, dataset)
            dset_list.append((dataset, outEvents))

    return dset_list        

def main():
    parser = optparse.OptionParser()
    (options,args) = parser.parse_args()

    if not len(args)==1:
        print "usage: python2.6 collect_dsets_and_nevents.py <inputFile_containing_a_list_of_workflows>"
        sys.exit(0)

    wf_list = []    

    inputFile=args[0]
    f = open(inputFile, 'r')
    for line in f:
        wf=line.rstrip('\n')
        wf_list.append(wf)

    dsets_nevents=collect_dsets_and_nevents(wf_list)    
    for dset_nevents in dsets_nevents:
        print dset_nevents[0] + " " + str(dset_nevents[1])

if __name__ == "__main__":
    main()

