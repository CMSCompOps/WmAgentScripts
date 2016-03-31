#!/usr/bin/env python
"""
 Assign a task chain workflow
"""

import httplib
import sys
import re
import os
import json
import optparse
from dbs.apis.dbsClient import DbsApi
from random import choice
from pprint import pprint
import reqMgrClient as reqMgr

dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

T1S = [
    "T1_DE_KIT",
    "T1_ES_PIC",
    "T1_FR_CCIN2P3",
    "T1_IT_CNAF",
    "T1_RU_JINR",
    "T1_UK_RAL",
    "T1_US_FNAL",
]

GOOD_SITES = T1S + [
    "T2_CH_CERN",
    "T2_DE_DESY",
    "T2_ES_CIEMAT",
    "T2_FR_CCIN2P3",
    "T2_IT_Bari",
    "T2_IT_Legnaro",
    "T2_IT_Pisa",
    "T2_IT_Rome",
    "T2_UK_London_Brunel",
    "T2_UK_London_IC",
    "T2_US_Caltech",
    "T2_US_Florida",
    "T2_US_MIT",
    "T2_US_Nebraska",
    "T2_US_Purdue",
    "T2_US_UCSD",
    "T2_US_Wisconsin",
]

ALL_SITES = GOOD_SITES + [
    "T2_AT_Vienna",
    "T2_BE_IIHE",
    "T2_BE_UCL",
    "T2_BR_SPRACE",
    "T2_BR_UERJ",
    "T2_CH_CSCS",
    "T2_CN_Beijing",
    "T2_DE_RWTH",
    "T2_EE_Estonia",
    "T2_ES_IFCA",
    "T2_FI_HIP",
    "T2_FR_IPHC",
    "T2_FR_GRIF_IRFU",
    "T2_FR_GRIF_LLR",
    "T2_HU_Budapest",
    "T2_KR_KNU",
    "T2_PT_NCG_Lisbon",
    "T2_RU_JINR",
    "T2_RU_IHEP",
    "T2_RU_PNPI",
    "T2_UA_KIPT",
    "T2_RU_SINP",
    "T2_UK_SGrid_RALPP",
    "T2_US_Vanderbilt",

]


def getRandomDiskSite(site=T1S):
    """
        Gets a random disk site and append _Disk
    """
    s = choice(site)
    if s.startswith("T1"):
        s += "_Disk"
    return s


def assignRequest(url, workflow, team, site, era, procstr, procver, activity, lfn, replica, verbose, trust_site=False):
    """
    Sends assignment request
    """
    params = {"action": "Assign",
              "Team" + team: "checked",
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 50000,
              #"MaxRSS": 3772000,
              "MaxRSS": 2294967,
              "MaxVSize": 20294967,
              # "AcquisitionEra": era,
              # "ProcessingString": procstr,
              "ProcessingVersion": procver,
              "Dashboard": activity,
              # when we want to use xrootd to readin input files
              #              "useSiteListAsLocation" : True,
              #              "CustodialSites": ['T1_US_FNAL'],
              #              "CustodialSubType" : "Move",
              #              "NonCustodialSites": getRandomDiskSite(),
              #             "NonCustodialSubType" : "Replica",
              #              "AutoApproveSubscriptionSites": ['T2_CH_CERN'],
              #              "SubscriptionPriority": "Normal",
              #              "BlockCloseMaxWaitTime" : 3600,
              "BlockCloseMaxWaitTime": 64800,
              "BlockCloseMaxFiles": 500,
              "BlockCloseMaxEvents": 20000000,
              "BlockCloseMaxSize": 5000000000000,
              "SoftTimeout": 159600,
              "GracePeriod": 1000,
              "checkbox" + workflow: "checked"}
    # add xrootd (trustSiteList)
    if trust_site:
        params['TrustSitelists'] = True
        
    # if era is None, leave it out of the json
    if era is not None:
        params["AcquisitionEra"] = era
    if procstr is not None:
        params["ProcessingString"] = procstr

    # if era is None, leave it out of the json
    if era is not None:
        params["AcquisitionEra"] = era
    if procstr is not None:
        params["ProcessingString"] = procstr

    # if replica we add NonCustodial sites
    if replica:
        params["NonCustodialSites"] = getRandomDiskSite(),
        params["NonCustodialSubType"] = "Replica"
        params['AutoApproveSubscriptionSites'] = [params["NonCustodialSites"]]

    if verbose:
        pprint(params)

    # TODO try reqMgr standard
    params['execute'] = True
    res = reqMgr.assignWorkflow(url, workflow, team, params)
    if res:
        print 'Assigned workflow:', workflow, 'to site:', site, 'and team', team
    else:
        print 'could not assign the workflow',workflow
    #TODO check conditions of success
    if verbose:
        print res

def getRequestDict(url, workflow):
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv(
        'X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    r1 = conn.request("GET", '/reqmgr/reqMgr/request?requestName=' + workflow)
    r2 = conn.getresponse()
    request = json.loads(r2.read())
    return request


def main():
    url = 'cmsweb.cern.ch'
    # Example: python assignWorkflow.py -w amaltaro_RVZTT_120404_163607_6269
    # -t testbed-relval -s T1_US_FNAL -e CMSSW_6_0_0_pre1_FS_TEST_WMA -p v1 -a
    # relval -l /store/backfill/1
    parser = optparse.OptionParser()
    parser.add_option(
        '-w', '--workflow', help='Workflow Name', dest='workflow')
    parser.add_option('-t', '--team', help='Type of Requests', dest='team')
    parser.add_option('-s', '--site', help='Site', dest='site')
    parser.add_option('-p', '--procversion',
                      help='Processing Version', dest='procversion')
    parser.add_option('-a', '--activity',
                      help='Dashboard Activity', dest='activity')
    parser.add_option('-l', '--lfn', help='Merged LFN base', dest='lfn')
    parser.add_option('--special',
                      help='Use it for special workflows. You also have to change the code according to the type of WF', dest='special')
    parser.add_option('-r', '--replica', action='store_true', dest='replica', default=False,
                      help='Adds a _Disk Non-Custodial Replica parameter')
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Prints all query information.")
    parser.add_option('-x', '--xrootd', help='Assign with trustSiteLocation=True (allows xrootd capabilities)',
                      action='store_true', default=False, dest='xrootd')
    parser.add_option("--acqera", dest="acqera",
                      help="Overrides Acquisition Era with a single string")
    parser.add_option("--procstr", dest="procstring",
                      help="Overrides Processing String with a single string")

    parser.add_option('--test', action="store_true",
                      help='Nothing is injected, only print infomation about workflow and AcqEra', dest='test')
    parser.add_option('--pu', action="store_true",
                      help='Use it to inject PileUp workflows only', dest='pu')
    (options, args) = parser.parse_args()

    if not options.workflow:
        if args:
            workflows = args
        elif options.file:
            workflows = [l.strip() for l in open(options.file) if l.strip()]
        else:
            parser.error("Input a workflow name or a file to read them")
            sys.exit(0)
    else:
        workflows = [options.workflow]
        
    team = 'production'
    site = GOOD_SITES
    procversion = 1
    activity = 'production'
    lfn = '/store/mc'
    acqera = {}
    procstring = {}
    specialStr = ''
    replica = False
    
    for workflow in workflows:
        # Getting the original dictionary
        schema = getRequestDict(url, workflow)
    
        # Setting the AcqEra and ProcStr values per Task
        for key, value in schema.items():
            if type(value) is dict and key.startswith("Task"):
                try:
                    procstring[value['TaskName']] = value[
                        'ProcessingString'].replace("-", "_")
                    acqera[value['TaskName']] = value['AcquisitionEra']
                except KeyError:
                    print "This request has no AcquisitionEra or ProcessingString defined into the Tasks, aborting..."
                    sys.exit(1)
    
        # Adding the special string - in case it was provided in the command line
        if options.special:
            #specialStr = '_03Jan2013'
            specialStr = '_' + str(options.special)
            for key, value in procstring.items():
                procstring[key] = value + specialStr
    
        # Handling the parameters given in the command line
        if options.team:
            team = options.team
        if options.site:
            site = options.site
            if site == "all":
                site = ALL_SITES
            elif site == "t1":
                site = T1S
            #parse sites separated by commas
            elif "," in site:
                site = site.split(",")  
        if options.procversion:
            procversion = int(options.procversion)
        if options.activity:
            activity = options.activity
        if options.lfn:
            lfn = options.lfn
        if options.replica:
            replica = True
        # Override if there are new values in he
        if options.acqera:
            acqera = options.acqera
        if options.procstring:
            procstring = options.procstring
    
        # check output dataset existence, and abort if they already do!
        datasets = schema["OutputDatasets"]
        i = 0
        if 'ACDC' not in options.workflow:
            exist = False
            maxv = 1
            for key, value in schema.items():
                if type(value) is dict and key.startswith("Task"):
                    dbsapi = DbsApi(url=dbs3_url)
    
                    # list all datasets with same name but different version
                    # numbers
                    datasets = dbsapi.listDatasets(acquisition_era_name=value['AcquisitionEra'],
                                                   primary_ds_name=value['PrimaryDataset'], detail=True, dataset_access_type='*')
                    processedName = value['AcquisitionEra'] + \
                        '-' + value['ProcessingString'] + "-v\\d+"
                    # see if any of the dataset names is a match
                    for ds in datasets:
                        if re.match(processedName, ds['processed_ds_name']):
                            print "Existing dset:", ds['dataset'], "(%s)" % ds['dataset_access_type']
                            maxv = max(maxv, ds['processing_version'])
                            exist = True
                        else:
                            pass
                    i += 1
            # suggest max version
            if exist and procversion <= maxv:
                print "Some output datasets exist, its advised to assign with v ==", maxv + 1
                sys.exit(0)
    
        # If the --test argument was provided, then just print the information
        # gathered so far and abort the assignment
        if options.test:
            print "%s \tAcqEra: %s \tProcStr: %s \tProcVer: %s" % (workflow, acqera, procstring, procversion)
            # print workflow, '\tAcqEra:', acqera, '\tProcStr:', procstring,
            # '\tProcVer:', procversion
            print "LFN: %s \tTeam: %s \tSite: %s" % (lfn, team, site)
            # print '\tTeam:',team,  '\tSite:', site
            sys.exit(0)
    
        # Really assigning the workflow now
        # TODO use values when assigning merge jobs
        print workflow, '\tAcqEra:', acqera, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tTeam:', team, '\tSite:', site
        assignRequest(url, workflow, team, site, acqera,
                      procstring, procversion, activity, lfn, replica, options.verbose, options.xrootd)
    sys.exit(0)

if __name__ == "__main__":
    main()
