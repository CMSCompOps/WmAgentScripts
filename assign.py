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
from utils import workflowInfo, siteInfo


dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'


def getRandomDiskSite(site=None):
    """
        Gets a random disk site and append _Disk
        """
    if site == None:
        site = getRandomDiskSite.T1s
    s = choice(site)
    if s.startswith("T1"):
        s += "_Disk"
    return s

def assignRequest(url, workflow, team, sites, era, procversion, activity, lfn, procstring, trust_site=False, replica=False, verbose=False, taskchain=False, trust_secondary_site=False, memory=None):
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

    if trust_secondary_site:
        params['TrustPUSitelists'] = True

    params["AcquisitionEra"] = era
    params["ProcessingString"] = procstring
    
    # if replica we add NonCustodial sites
    if replica:
        params["NonCustodialSites"] = getRandomDiskSite(),
        params["NonCustodialSubType"] = "Replica"
        if taskchain:
            params['AutoApproveSubscriptionSites'] = [params["NonCustodialSites"]]
    if memory:
        params["Memory"] = memory
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
    parser.add_option('-x', '--xrootd', help='Assign with TrustSitelists=True (allows xrootd capabilities)',
                      action='store_true', default=False, dest='xrootd')
    parser.add_option('--secondary_xrootd', help='Assign with TrustPUSitelists=True (allows xrootd capabilities)',
                      action='store_true', default=False, dest='secondary_xrootd')
    parser.add_option('-l', '--lfn', help='Merged LFN base', dest='lfn')
    parser.add_option('-v', '--verbose', help='Verbose', action='store_true', default=False, dest='verbose')
    parser.add_option('--testbed', help='Assign in testbed', action='store_true', default=False, dest='testbed')
    parser.add_option('--test', action="store_true",help='Nothing is injected, only print infomation about workflow and Era', dest='test')
    parser.add_option('-f', '--file', help='Text file with a list of wokflows. If this option is used, the same settings will be applied to all workflows', dest='file')
    parser.add_option('-w', '--workflow', help='Workflow Name', dest='workflow')
    parser.add_option('-m', '--memory', help='Set the Memory parameter to the workflow', dest='memory', default=None)
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
    memory = None
    replica = False
    sites = []
    specialStr = ''
    taskchain = False
    team = 'production'

    SI = siteInfo()
    getRandomDiskSite.T1 = SI.sites_T1s
    # Handling the parameters given in the command line
    # parse site list
    if options.sites:
        if options.sites.lower() == "t1":
            sites = SI.sites_T1s
        elif options.sites.lower() == "t2":
            sites = SI.sites_T2s
        elif options.sites.lower() in ["all","t1+t2","t2+t1"] :
            sites = SI.sites_T2s+SI.sites_T1s
        elif options.sites.lower() == "mcore":
            sites = SI.sites_mcore_ready
        elif hasattr(SI,options.sites):
            sites = getattr(SI,options.sites)
        #elif options.sites.lower() == 'acdc':
        #    sites = []
        else: 
            sites = [site for site in options.sites.split(',')]
    else: 
        sites = SI.sites_T1s + SI.sites_T2s

    if options.team:
        team = options.team

    if options.replica:
        replica = True

    for wfn in wfs:
        # Getting the original dictionary
        wfi = workflowInfo( url, wfn )
        schema = wfi.request
        if 'OriginalRequestName' in schema:
            print "Original workflow is:",schema['OriginalRequestName']
            original_wf = workflowInfo(url, schema['OriginalRequestName'])            
        else:
            original_wf = None

        if options.sites.lower() == 'original' and original_wf:
            sites = original_wf.request['SiteWhitelist']
            print "Using",sorted(sites),"from the original request",original_wf.request['RequestName']

        #print json.dumps( schema, indent=2 )
        wf_name = wfn
        wf_info = schema

        # WF must be in assignment-approved in order to be assigned
        if (schema["RequestStatus"] != "assignment-approved"):
            print("The workflow '" + wf_name + "' you are trying to assign is not in assignment-approved")
            sys.exit(1)

        #Check to see if the workflow is a task chain or an ACDC of a taskchain
        taskchain = (schema["RequestType"] == "TaskChain") or (original_wf and original_wf.request["RequestType"] == "Resubmission")

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
            procstring = wf_info['ProcessingString']
        if options.era:
            era = options.era
        elif not taskchain:
            era = wf_info['AcquisitionEra']
        #Set era and procstring to none for merge ACDCs inside a task chain
        if schema["RequestType"] == "Resubmission" and wf_info["PrepID"].startswith("task") and "Merge" in schema["InitialTaskPath"].split("/")[-1]:
            era = None
            procstring = None

        # Must use --lfn option, otherwise workflow won't be assigned
        if options.lfn:
            lfn = options.lfn
        elif "MergedLFNBase" in wf_info:
            lfn = wf_info['MergedLFNBase']
        elif original_wf and "MergedLFNBase" in original_wf.request:
            lfn = original_wf.request['MergedLFNBase']
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

        if options.memory:
            memory = options.memory

        # given or default processing version
        if options.procversion:
            procversion = int(options.procversion)
        else:
            procversion = wf_info["ProcessingVersion"]

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
        else:
            ## this is a resubmission !
            print "The taks in resubmission is:",schema['InitialTaskPath']
            ## pick up the sites from acdc
            if options.sites.lower() == 'acdc':
                where_to_run, _,_ =  original_wf.getRecoveryInfo()
                task = schema['InitialTaskPath']
                sites = [SI.SE_to_CE(site) for site in where_to_run[task]]
                print "Found",sorted(sites),"as sites where to run the ACDC at, from the acdc doc of ",original_wf.request['RequestName']

            ## re-assure yourself that the lfn,procversion is set correctly
            lfn = original_wf.request['MergedLFNBase']
            procversion = original_wf.request["ProcessingVersion"]

        ## check that the sites are all compatible and up
        check_mem = schema['Memory']
        ncores = wfi.getMulticore()
        memory_allowed = SI.sitesByMemory( float(check_mem), maxCore=ncores)
        not_ready = sorted(set(sites) & set(SI.sites_not_ready))
        not_existing = sorted(set(sites) - set(SI.all_sites))
        not_matching = sorted((set(sites) - set(memory_allowed) - set(not_ready) - set(not_existing)))
        previously_used = []
        if schema['SiteWhitelist']: previously_used = schema['SiteWhitelist']
        if original_wf: previously_used = original_wf.request['SiteWhitelist']
        if previously_used: not_matching = sorted(set(not_matching) & set(previously_used))
        
        sites = sorted( set(sites) - set(not_matching) - set(not_existing))
        
        print sorted(memory_allowed),"to allow",check_mem,ncores
        if not_ready:
            print not_ready,"is/are not ready"
            sys.exit(0)
        if not_matching:
            print "The memory requirement",check_mem,"is too much for",not_matching
            sys.exit(0)


    # If the --test argument was provided, then just print the information
    # gathered so far and abort the assignment
        print wf_name
        print "Era:",era
        print "ProcStr:",procstring
        print "ProcVer:",procversion
        print "LFN:",lfn
        print "Team:",team
        print "Site:",sites
        print "Taskchain? ", str(taskchain)
        print "Activity:", activity

        if options.test:
            continue
        
        # Really assigning the workflow now
        #print wf_name, '\tEra:', era, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tTeam:', team, '\tSite:', sites
        assignRequest(url, wf_name, team, sites, era, procversion, activity, lfn, procstring, 
                      trust_site = options.xrootd, 
                      replica = options.replica, 
                      verbose = options.verbose, 
                      taskchain = taskchain, 
                      trust_secondary_site = options.secondary_xrootd,
                      memory=memory
                      )
    
    sys.exit(0)

if __name__ == "__main__":
    main()
