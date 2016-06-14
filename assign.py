#!/usr/bin/env python

# Combined assignWorkflow.py with assignProdTaskChain.py
# Author: Allie Reinsvold Hall
# May 2016

"""
    Quick request assignment, useful if you want to avoid assigning by
    Web interface and reqmgr.py is too unflexible.

    
"""

import httplib
import re
import os
import sys
import json
import optparse
from dbs.apis.dbsClient import DbsApi
import reqMgrClient as reqMgr
from pprint import pprint
from random import choice
from utils import workflowInfo, siteInfo, global_SI


dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

T1_SITES = [
            "T1_DE_KIT",
            "T1_ES_PIC",
            "T1_FR_CCIN2P3",
            "T1_IT_CNAF",
            "T1_RU_JINR",
            "T1_UK_RAL",
            "T1_US_FNAL"
            ]

T2_SITES = [
            "T2_CH_CERN",
            "T2_DE_DESY",
            "T2_DE_RWTH",
            "T2_ES_CIEMAT",
            "T2_FR_CCIN2P3",
            "T2_FR_IPHC",
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
            "T2_US_Wisconsin"
            ]

ALL_SITES = T1_SITES + T2_SITES


def getRandomDiskSite(site=T1_SITES):
    """
        Gets a random disk site and append _Disk
        """
    s = choice(site)
    if s.startswith("T1"):
        s += "_Disk"
    return s

def assignRequest(url, workflow, team, sites, era, procversion, activity, lfn, procstring, trust_site=False, replica=False, verbose=False, taskchain=False):
    """
    Sends assignment request
    """
    params = {"action": "Assign",
              "Team" + team: "checked",
              "SiteWhitelist": sites,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 50000,
              "MaxVSize": 4294967296,
                  # for task chains    "MaxVSize": 20294967,
              "SoftTimeout" : 159600,
              "Dashboard": activity,
              "ProcessingVersion": procversion,
              "checkbox" + workflow: "checked",
              "execute":True
              }
              
    if taskchain:
        params["GracePeriod"] = 1000
        params["BlockCloseMaxWaitTime"] = 64800
        params["BlockCloseMaxFiles"] = 500
        params["BlockCloseMaxEvents"] = 20000000
        params["BlockCloseMaxSize"] = 5000000000000
        params["MaxVSize"] = 20294967
    else:
        params["CustodialSites"] = []

    # add xrootd (trustSiteList)
    if trust_site:
        params['TrustSitelists'] = True
        params['TrustPUSitelists'] = True
    
    params["AcquisitionEra"] = era
    params["ProcessingString"] = procstring
    
    # if replica we add NonCustodial sites
    if replica:
        params["NonCustodialSites"] = getRandomDiskSite(),
        params["NonCustodialSubType"] = "Replica"
        if taskchain:
            params['AutoApproveSubscriptionSites'] = [params["NonCustodialSites"]]

    if verbose:
        pprint(params)

    res = reqMgr.assignWorkflow(url, workflow, team, params)
    if res:
        print 'Assigned workflow:', workflow, 'to site:', sites, 'with processing version', procversion
    else:
        print 'Could not assign workflow:', workflow, 'to site:', sites, 'with processing version', procversion
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
    url_tb = 'cmsweb-testbed.cern.ch'
    
    # Example: python assign.py -w amaltaro_RVZTT_120404_163607_6269
    # -t testbed-relval -s T1_US_FNAL -e CMSSW_6_0_0_pre1_FS_TEST_WMA -p v1 -a
    # relval -l /store/backfill/1
    usage = "usage: %prog [options] [WORKFLOW]"
    
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-t', '--team', help='Type of Requests', dest='team')
    parser.add_option('-s', '--sites', help=' "t1" for Tier-1\'s and "t2" for Tier-2\'s', dest='sites')
    parser.add_option('--special',  help='Use it for special workflows. You also have to change the code according to the type of WF', dest='special')
    parser.add_option('-r', '--replica', action='store_true', dest='replica', default=False, help='Adds a _Disk Non-Custodial Replica parameter')
    parser.add_option('-p', '--procversion', help='Processing Version, if empty it will leave the processing version that comes by default in the request', dest='procversion')
    parser.add_option('-a', '--activity', help='Dashboard Activity (reprocessing, production or test), if empty will set reprocessing as default', dest='activity')
    parser.add_option('-x', '--xrootd', help='Assign with trustSiteLocation=True (allows xrootd capabilities)',
                                        action='store_true', default=False, dest='xrootd')
    parser.add_option('-l', '--lfn', help='Merged LFN base', dest='lfn')
    parser.add_option('-v', '--verbose', help='Verbose', action='store_true', default=False, dest='verbose')
    parser.add_option('--testbed', help='Assign in testbed', action='store_true', default=False, dest='testbed')
    parser.add_option('--test', action="store_true",help='Nothing is injected, only print infomation about workflow and Era', dest='test')
    parser.add_option('-f', '--file', help='Text file with a list of wokflows. If this option is used, the same settings will be applied to all workflows', dest='file')
    parser.add_option('-w', '--workflow', help='Workflow Name', dest='workflow')
    parser.add_option('-e', '--era', help='Acquistion era', dest='era')
    parser.add_option("--procstr", dest="procstring", help="Overrides Processing String with a single string")

    (options, args) = parser.parse_args()
    
    if options.testbed:
        url = url_tb

    # parse input workflows and files. If both -w and -f options are used, then only the -w inputs are considered.
    if not options.workflow:
        if args:
            wfs = args
        elif options.file:
            wfs = [l.strip() for l in open(options.file) if l.strip()]
        else:
            parser.error("Input a workflow name or a file to read them")
            sys.exit(0)
    else:
        wfs = [options.workflow]

    #Default values
    era = {}
    procversion = 1
    procstring = {}
    replica = False
    sites = ALL_SITES
    specialStr = ''
    taskchain = False
    team = 'production'
    trust_site = False

    SI = global_SI
    # Handling the parameters given in the command line
    # parse site list
    if options.sites:
        if options.sites == "t1":
            sites = SI.sites_T1s
        elif options.sites == "t2":
            sites = SI.sites_T2s
        else:
#            sites = SI.sites_T1s + SI.sites_T2s
            sites = [site for site in options.sites.split(',')]
    else: 
        sites = SI.sites_T1s + SI.sites_T2s
    if options.team:
        team = options.team

    if options.xrootd:
        trust_site = True

    if options.replica:
        replica = True

    for wf in wfs:
        # Getting the original dictionary
        schema = getRequestDict(url, wf)
        wf = reqMgr.Workflow(wf, url=url)

        # WF must be in assignment-approved in order to be assigned
        if (schema["RequestStatus"] != "assignment-approved"):
            print("The workflow '" + wf.name + "' you are trying to assign is not in assignment-approved")
            sys.exit(1)

        #Check to see if the workflow is a task chain or an ACDC of a taskchain
        taskchain = (schema["RequestType"] == "TaskChain") or ((schema["RequestType"] == "Resubmission") and "task" in schema["InitialTaskPath"].split("/")[1])

        #Dealing with era and proc string
        if taskchain:
            # Setting the Era and ProcStr values per Task
            for key, value in schema.items():
                if type(value) is dict and key.startswith("Task"):
                    try:
                        if 'ProcessingString' in value:
                            procstring[value['TaskName']] = value['ProcessingString']
                        else:
                            procstring[value['TaskName']] = schema['ProcessingString']
                        if 'AcquisitionEra' in value:
                            era[value['TaskName']] = value['AcquisitionEra']
                        else:
                            procstring[value['TaskName']] = schema['AcquisitionEra']
                    except KeyError:
                        print("This taskchain request has no AcquisitionEra or ProcessingString defined into the Tasks, aborting...")
                        sys.exit(1)
        # Adding the special string - in case it was provided in the command line
        if options.special:
            specialStr = '_' + str(options.special)
            for key, value in procstring.items():
                procstring[key] = value + specialStr
        # Override if a value is given using the procstring command
        if options.procstring:
            procstring = options.procstring
        elif not taskchain:
            procstring = wf.info['ProcessingString']
        if options.era:
            era = options.era
        elif not taskchain:
            era = wf.info['AcquisitionEra']
        #Set era and procstring to none for merge ACDCs inside a task chain
        if schema["RequestType"] == "Resubmission" and wf.info["PrepID"].startswith("task") and "Merge" in schema["InitialTaskPath"].split("/")[-2]:
            era = None
            procstring = None

        # Must use --lfn option, otherwise workflow won't be assigned
        if options.lfn:
            lfn = options.lfn
        elif "MergedLFNBase" in wf.info:
            lfn = wf.info['MergedLFNBase']
        else:
            print "Can't assign the workflow! Please include workflow lfn using --lfn option."
            sys.exit(0)
        # activity production by default for taskchains, reprocessing for default by workflows
        if options.activity:
            activity = options.activity
        elif taskchain:
            activity = 'production'
        else:
            activity = 'reprocessing'

        # given or default processing version
        if options.procversion:
            procversion = int(options.procversion)
        else:
            procversion = wf.info["ProcessingVersion"]

        # Check for output dataset existence, and abort if output datasets already exist!
        # Don't perform this check for ACDC's
        datasets = schema["OutputDatasets"]
        i = 0
        if not (schema["RequestType"] == "Resubmission" ):
            exist = False
            maxv = 1
            for key, value in schema.items():
                if type(value) is dict and key.startswith("Task"):
                    dbsapi = DbsApi(url=dbs3_url)
                    
                    # list all datasets with same name but different version
                    # numbers
                    datasets = dbsapi.listDatasets(acquisition_era_name=value['AcquisitionEra'], primary_ds_name=value['PrimaryDataset'], detail=True, dataset_access_type='*')
                    processedName = value['AcquisitionEra'] + '-' + value['ProcessingString'] + "-v\\d+"
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
            print "%s \tEra: %s \tProcStr: %s \tProcVer: %s" % (wf.name, era, procstring, procversion)
            print "LFN: %s \tTeam: %s \tSite: %s" % (lfn, team, sites)
            print "Taskchain? " + str(taskchain)
            print "Activity:" + activity
            sys.exit(0)
        
        # Really assigning the workflow now
        print wf.name, '\tEra:', era, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tTeam:', team, '\tSite:', sites
        assignRequest(url, wf.name, team, sites, era, procversion, activity, lfn, procstring, trust_site, options.replica, options.verbose, taskchain)
    
    sys.exit(0)

if __name__ == "__main__":
    main()
