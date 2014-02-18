#!/usr/bin/env python
"""

 DBS 3 Client
 Ports all the functionality from previously used
 dbsTest.py to use DBS3 directly.

"""


import urllib2,urllib, httplib, sys, re, os, json
from xml.dom.minidom import getDOMImplementation
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
    returns true if at least one duplicate lumi was found
    That is if there is the same lumi in the same run and
    two different files
    This can be used on datasets that have separate
    runs.
    """  
    dbsapi = DbsApi(url=dbs3_url)
    duplicated = False
    #check each run
    runs = getRunsDataset(dataset)
    #if only one run in the list
    if len(runs) == 1:
        return duplicateLumi(dataset, verbose)
    #else manually
    for run in runs:
        #create a set
        lumisChecked={}
        # retrieve files for that run
        reply = dbsapi.listFiles(dataset=dataset)
        for f in reply:
            logical_file_name = f['logical_file_name']
            reply2 = dbsapi.listFileLumis(logical_file_name=logical_file_name, run_num=run)
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
    checks if output dataset has duplicate lumis
    returns true if at least one duplicate lumi was found
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
    runs = []
    #filter and clean
    for run in reply:
        if type(run['run_num']) is list:
            runs.append(run['run_num'][0])
        else:
            runs.append(run['run_num'])
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


def getLumiCountDataSet(dataset):
    """
    Get the number of unique lumis in a dataset
    """
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    reply = dbsapi.listFileSummaries(dataset=dataset)
    return reply[0]['num_lumi']

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
    #reply = dbsapi.listBlockSummaries(block_name=blockList)
    #transform from strin to list
    if type(blockList) in (str, unicode):
        blockList = eval(blockList)
    total = 0
    #get one by one block and add it so uri wont be too large
    for block in blockList:
        reply = dbsapi.listBlockSummaries(block_name=block)
        total += reply[0]['num_event']
    return total

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
    outputDataSets=reqMgrClient.outputdatasetsWorkflow(url, workflow)
    #runlist = [176801, 176807, 176702, 176796, 175896]
    ##inputEvents=getInputEvents(url, workflow)
    ##print " Runs", getEventCountDataSetRunList('/PhotonHad/Run2011B-v1/RAW',runlist)
    #print " Events:", getEventCountDataSet('/Neutrino_Pt-2to20_gun/Fall13-POSTLS162_V1-v4/GEN-SIM')
    for dataset in outputDataSets:
        print dataset
        print " Events:", getEventCountDataSet(dataset)
        print " Lumis:", getLumiCountDataSet(dataset)
        #print " Duplicated Lumis:", duplicateRunLumi(dataset)
        #print " Duplicated Lumis:", duplicateLumi(dataset)
        #print " Runs", getRunsDataset(dataset))
        #print " Blocks", hasAllBlocksClosed(dataset)
        #print " blockEvents", getEventCountDataSetBlockList(dataset,blocklist)
        
    sys.exit(0);

if __name__ == "__main__":
    main()
    sys.exit(0);
