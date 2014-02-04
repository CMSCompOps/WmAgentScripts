#!/usr/bin/env python
"""

 DBS 3 Client
 Ports all the functionality from previously used
 dbsTest.py to use DBS3 directly.

"""


import urllib2,urllib, httplib, sys, re, os, json, phedexSubscription
from xml.dom.minidom import getDOMImplementation
from das_client import get_data
from dbs.apis.dbsClient import DbsApi

#das_host='https://das.cern.ch'
das_host='https://cmsweb.cern.ch'
#das_host='https://cmsweb-testbed.cern.ch'
#das_host='https://das-dbs3.cern.ch'
#das_host='https://dastest.cern.ch'
dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'


def duplicateRunLumi(dataset, verbose=False):
    """
    checks if output dataset has duplicate lumis
    for every run.
    """  
    dbsapi = DbsApi(url=dbs3_url)
    duplicated = False
    #check each run
    for run in getRunsDataset(dataset):
        #create a set
        lumisChecked={}
        # retrieve files for that run
        reply = dbsapi.listFiles(dataset=dataset, run_num=run)
        for f in reply:
            logical_file_name = f['logical_file_name']
            reply2 = dbsapi.listFileLumis(logical_file_name=logical_file_name)
            #retrieve lumis for each file
            lumis = reply2[0]['lumi_section_num']
            #check that each lumi is only in one file
            for lumi in lumis:
                if lumi in lumisChecked:
                    #if verbose print results, if not end quickly
                    if verbose:
                        print 'Lumi',lumi,'in run',run,'is in these files'
                        print logical_file_name
                        print lumisChecked[lumi]
                        duplicated = True
                    else:
                        return True
                else:
                    lumisChecked[lumi] = logical_file_name

    return duplicated

def duplicateLumi(dataset, verbose=False):
    """
    checks if output dataset has a duplicate lumi
    """
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    duplicated = False
    lumisChecked={}
    # retrieve files
    reply = dbsapi.listFiles(dataset=dataset)
    for f in reply:
        logical_file_name = f['logical_file_name']
        reply2 = dbsapi.listFileLumis(logical_file_name=logical_file_name)
        #retrieve lumis for each file
        lumis = reply2[0]['lumi_section_num']
        #check that each lumi is only in one file
        for lumi in lumis:
            if lumi in lumisChecked:
                #if verbose print results, if not end quickly
                if verbose:
                    print 'Lumi',lumi,'is in these files'
                    print logical_file_name
                    print lumisChecked[lumi]
                    duplicated = True
                else:
                    return True
            else:
                lumisChecked[lumi] = logical_file_name
    return duplicated

def getRunsDataset(dataset):
    """
    returns a list with number of each run.
    """
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve runs
    reply = dbsapi.listRuns(dataset=dataset)
    #a list with only the run numbers
    runs = [run['run_num'] for run in reply]
    return runs

def getNumberofFilesPerRun(das_url, dataset, run):
    """
    Count number of files
    """
     # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)

    # retrieve file list
    reply = dbsapi.listFiles(dataset=dataset)
    return len(reply)


def getEventCountDataSet(dataset):
    """
    Returns the number of events in a dataset using DBS3

    """
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    reply = dbsapi.listBlockSummaries(dataset=dataset)
    return reply[0]['num_event']



def hasAllBlocksClosed(dataset):
    """
    checks if a given dataset has all blocks closed
    """
    
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    reply = dbsapi.listBlocks(dataset=dataset, detail=True)
    for block in reply:
        print block['block_name']
        print block['open_for_writing']
        if block['open_for_writing']:
            return False
    return True


def getEventCountDataSetBlockList(dataset,blockList):
    """
    Counts and adds all the events for a given lists
    blocks inside a dataset
    """
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)    
    lumis=0
    reply = dbsapi.listBlockSummaries(block_name=blockList)       
    return reply[0]['num_event']

def getEventCountDataSetRunList(dataset,runList):
    """
    Counts and adds all the events for a given lists
    of runs inside a dataset
    """
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve file aggregation only by the runs
    reply = dbsapi.listFileSummaries(dataset=dataset,run_num=runList)
    #a list with only the run numbers
    return reply[0]['num_event']

def main():
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:dbs3Client workflow"
        sys.exit(0)
    workflow=args[0]
    url='cmsweb.cern.ch'
    outputDataSets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
    #runlist = [176801, 176807, 176702, 176796, 175896]
    ##inputEvents=getInputEvents(url, workflow)
    ##print " Runs", getEventCountDataSetRunList('/PhotonHad/Run2011B-v1/RAW',runlist)
    #print " Events:", getEventCountDataSet('/Neutrino_Pt-2to20_gun/Fall13-POSTLS162_V1-v4/GEN-SIM')
    for dataset in outputDataSets:
        print dataset
        print " Events:", getEventCountDataSet(dataset)
        #print " Duplicated Lumis:", duplicateRunLumi(dataset)
        #print " Duplicated Lumis:", duplicateLumi(dataset)
        #print " Runs", getRunsDataset(dataset))
        #print " Blocks", hasAllBlocksClosed(dataset)
        #print " blockEvents", getEventCountDataSetBlockList(dataset,blocklist)
        
    sys.exit(0);

if __name__ == "__main__":
    main()
    sys.exit(0);
