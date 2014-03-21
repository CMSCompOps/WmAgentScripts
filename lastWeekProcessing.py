#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, json
from xml.dom.minidom import getDOMImplementation
from dbs.apis.dbsClient import DbsApi

#das_host='https://das.cern.ch'
das_host='https://cmsweb.cern.ch'
#das_host='https://cmsweb-testbed.cern.ch'
#das_host='https://das-dbs3.cern.ch'
#das_host='https://dastest.cern.ch'
dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

def getEventsDetails(acquisitionEra, dataTierName, searchStr, date):
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    reply = dbsapi.listBlocks(data_tier_name=dataTierName,min_cdate=1394818770,max_cdate=1395407514)

    sum = 0

    for block in reply:
       if (acquisitionEra in block['block_name'] and (searchStr =='NA' or (searchStr != 'NA' and searchStr in block['block_name']))):
          events = getEventCountBlock(block['block_name'])
#          print ' - block - ',block['block_name'],events
          sum = sum + events

    return sum

def getEventCountBlock(block):
    """
    Returns the number of events in a dataset using DBS3

    """
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    reply = dbsapi.listBlockSummaries(block_name=block)
    return reply[0]['num_event']

def main():
    args=sys.argv[1:]
    if not len(args)==0:
        print "usage:processingCampaignOverview.py"
        sys.exit(0)
    url='cmsweb.cern.ch'

    date = 1392681600

    events1 = getEventsDetails('HiWinter13','GEN-SIM-RECO','NA',date)
    print 'HiWinter13DR53X : ',events1
    events1 = getEventsDetails('Summer11LegDR','AODSIM','NA',date)
    print 'Summer11LegDR : ',events1
    events1 = getEventsDetails('HiWinter13','GEN-SIM-RECO','pa',date)
    print 'pAHiWinter13DR53X : ',events1
    events1 = getEventsDetails('Summer12','AODSIM','LowPU2010_DR42',date)
    print 'LowPU2010DR42 : ',events1
    events1 = getEventsDetails('Fall11','GEN-RAW','START42_V14B',date)
    print 'Fall11R1 : ',events1
    events1 = getEventsDetails('Fall11','AODSIM','START42_V14B',date)
    print 'Fall11R2 : ',events1
    events1 = getEventsDetails('Summer12_DR53X','AODSIM','NA',date)
    print 'Summer12DR53X : ',events1
    events1 = getEventsDetails('Fall13dr','AODSIM','NA', date)
    print 'Fall13dr : ',events1
    events1 = getEventsDetails('Summer11dr53X','AODSIM','NA', date)
    print 'Summer11dr53X : ',events1
        
    sys.exit(0);

if __name__ == "__main__":
    main()
    sys.exit(0);
