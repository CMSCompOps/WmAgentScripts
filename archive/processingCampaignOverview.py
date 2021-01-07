#!/usr/bin/env python
# Generate campaign overview table for Monday Comp Ops meetings

import urllib2,urllib, httplib, sys, re, os, json
from xml.dom.minidom import getDOMImplementation
from dbs.apis.dbsClient import DbsApi

das_host='https://cmsweb.cern.ch'
dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

def getEventsDetails(acquisitionEra, dataTierName, searchStr):
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    reply = dbsapi.listDatasets(data_tier_name=dataTierName,dataset_access_type='VALID')

    sumValid = 0
    sumValidProd = 0

    for dataset in reply:
       if (acquisitionEra in dataset['dataset'] and (searchStr =='NA' or (searchStr != 'NA' and searchStr in dataset['dataset']))):
          events = getEventCountDataSet(dataset['dataset'])
          sumValid = sumValid + events
          sumValidProd = sumValidProd + events

    reply = dbsapi.listDatasets(data_tier_name=dataTierName,dataset_access_type='PRODUCTION')

    for dataset in reply:
       if (acquisitionEra in dataset['dataset'] and (searchStr =='NA' or (searchStr != 'NA' and searchStr in dataset['dataset']))):
          events = getEventCountDataSet(dataset['dataset'])
          sumValidProd = sumValidProd + events

    return [sumValid, sumValidProd]

def getEventCountDataSet(dataset):
    """
    Returns the number of events in a dataset using DBS3

    """
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    reply = dbsapi.listBlockSummaries(dataset=dataset)
    return reply[0]['num_event']


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

def main():
    args=sys.argv[1:]
    if not len(args)==0:
        print "usage:processingCampaignOverview.py"
        sys.exit(0)
    url='cmsweb.cern.ch'

    [events1,events2] = getEventsDetails('Spring14dr','AODSIM','NA')
    print 'Spring14dr : VALID = ',events1,' VALID+PROD = ',events2
    [events1,events2] = getEventsDetails('Summer12','AODSIM','LowPU2010_DR42')
    print 'LowPU2010DR42 : VALID = ',events1,' VALID+PROD = ',events2
    [events1,events2] = getEventsDetails('Summer11dr53X','AODSIM','NA')
    print 'Summer11dr53X : VALID = ',events1,' VALID+PROD = ',events2
    [events1,events2] = getEventsDetails('Summer11LegDR','AODSIM','NA')
    print 'Summer11LegDR : VALID = ',events1,' VALID+PROD = ',events2
    [events1,events2] = getEventsDetails('HiWinter13','GEN-SIM-RECO','HiWinter13-STARTHI53')
    print 'HiWinter13DR53X : VALID = ',events1,' VALID+PROD = ',events2
    [events1,events2] = getEventsDetails('HiWinter13','GEN-SIM-RECO','HiWinter13-pa_STARTHI53')
    print 'pAWinter13DR53X : VALID = ',events1,' VALID+PROD = ',events2
    [events1,events2] = getEventsDetails('Fall11','GEN-RAW','START42_V14B')
    print 'Fall11R1 : VALID = ',events1,' VALID+PROD = ',events2
    [events1,events2] = getEventsDetails('Fall11','AODSIM','START42_V14B')
    print 'Fall11R2 : VALID = ',events1,' VALID+PROD = ',events2
    [events1,events2] = getEventsDetails('Summer12_DR53X','AODSIM','NA')
    print 'Summer12DR53X : VALID = ',events1,' VALID+PROD = ',events2
    [events1,events2] = getEventsDetails('Fall13dr','AODSIM','NA')
    print 'Fall13dr : VALID = ',events1,' VALID+PROD = ',events2

    sys.exit(0);

if __name__ == "__main__":
    main()
    sys.exit(0);
