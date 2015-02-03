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
    conn = httplib.HTTPSConnection(reqmgr_url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1 = conn.request("GET",'/reqmgr/reqMgr/outputDatasetsByRequestName?requestName='+workflow)
    r2 = conn.getresponse()
    datasets = json.loads(r2.read())
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

def main():
    parser = optparse.OptionParser()
    parser.add_option('--correct_env',action="store_true",dest='correct_env')
    (options,args) = parser.parse_args()

    command=""
    for arg in sys.argv:
        command=command+arg+" "

    if not options.correct_env:
        # source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
        os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; source /tmp/relval/sw/comp.pre/slc6_amd64_gcc481/cms/dbs3-client/3.2.8a/etc/profile.d/init.sh; python2.6 "+command + "--correct_env")
        sys.exit(0)
        
    if not len(args)==1:
        print "usage: python2.6 getRelValDsetNames.py <inputFile_containing_a_list_of_workflows>"
        sys.exit(0)
    inputFile=args[0]
    f = open(inputFile, 'r')

    # Instantiate the dbs3 api
    dbsApi = getDBSApi()

    for line in f:
        workflow = line.rstrip('\n')
        outputDataSets = getOutputDset(workflow)
        for dataset in outputDataSets:
            outEvents = getNumEvents(dbsApi, dataset)
            #print "%-120s\t%d" % (dataset, dset_summary[0]['num_event'])
            print "%s\t%d" % (dataset, outEvents)

    f.close
    sys.exit(0);

if __name__ == "__main__":
    main()

