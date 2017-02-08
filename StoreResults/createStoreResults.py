#!/usr/bin/env python

"""
    Executes the migration and creation of a storeResults query
    Usage:
    python createStoreResults.py TICKET DATASET DBS_URL CMSSW_RELEASE GROUP_NAME
    TICKET: The ticket # in GGUS, could be any number, this is used only for tracking.
    DATASET: The input dataset, it has to be located at the same Tier-2 which is used to finally
        hold the group data in /store/results/
    DBS_URL: For example: "phys01" for https://cmsweb.cern.ch/dbs/prod/phys01/DBSReader.
    CMSSW_RELEASE: which should be used for merging step. In general, use always the version used for
        the dataset production, if the version is outdated you need to check which is the closest version
        available.
    GROUP_NAME: The physics group (HIN, HIG, SUS, etc.) requesting the migration, this will determine
        the subdirectory below /store/results/ and sets the appropriate Phedex accounting group tag. 
"""

from RequestQuery import RequestQuery
from MigrationToGlobal import MigrationToGlobal 
import sys

def main():
    #retrieve requests
    #rq = SavannahRequestQuery({'SavannahUser':'jbadillo','SavannahPasswd':'pmr2g&vsmc2g',
    #    'ComponentDir':'./storeResults/Tickets'})
    #report = rq.getRequests(resolution_id='1',task_status='0', status_id='1')#ticketList=['51775','51776']) #

    #Single request
    try:
        ticket, input_dataset, dbs_url, cmssw_release, group_name = sys.argv[1:6]
    except:
        print "Usage:"
        print "python runStoreResults.py TICKET DATASET DBS_URL CMSSW_RELEASE GROUP_NAME"
        return

    rq = RequestQuery({'ComponentDir':'./storeResults/Tickets'})
    report = rq.createRequestJSON(ticket, input_dataset, dbs_url, cmssw_release, group_name)
    # Print out report
    rq.printReport(report)

    #migrate to dbs3
    m = MigrationToGlobal() 
    m.Migrates([report])

if __name__ == '__main__':
    main()
