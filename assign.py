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
import copy
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

#def assignRequest(url, workflow, team, sites, era, procversion, activity, lfn, procstring, trust_site=False, replica=False, verbose=False, taskchain=False, trust_secondary_site=False, memory=None, multicore=None):
def assignRequest(url, **args):
    """
    Sends assignment request
    """
    workflow = args.get('workflow')
    team = args.get('team')
    sites = args.get('sites')
    era = args.get('era')
    procversion = args.get('procversion')
    activity = args.get('activity')
    lfn = args.get('lfn')
    procstring = args.get('procstring')
    trust_site = args.get('trust_site',False)
    replica = args.get('replica',False)
    verbose = args.get('verbose',False)
    taskchain = args.get('taskchain',False)
    trust_secondary_site = args.get('trust_secondary_site',False)
    memory = args.get('memory',None)
    multicore = args.get('multicore',None)
    #params = copy.deepcopy(reqMgr.assignWorkflow.defaults)
    params = {
              "SiteWhitelist": sites,
              "MergedLFNBase": lfn,
              "Dashboard": activity,
              "ProcessingVersion": procversion,
              "execute": True
              }
    
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
        params["Memory"] = memory if type(memory)==dict else int(memory)
                    
    if multicore:
        params["Multicore"] = multicore if type(multicore)==dict else int(multicore)

    if verbose:
        #pprint(params)
        params['execute'] = False
        #return False

    if args.get('maxmergeevents',None):
        params['MaxMergeEvents'] = args.get('maxmergeevents')
    if args.get('lumisperjob',None):
        params['LumisPerJob'] = args.get('lumisperjob')

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
    parser.add_option('-t', '--team', help='Type of Requests', dest='team', default='production')
    parser.add_option('-s', '--sites', help=' "t1" for Tier-1\'s and "t2" for Tier-2\'s', dest='sites')
    parser.add_option('--special',  help='Use it for special workflows. You also have to change the code according to the type of WF', dest='special')
    parser.add_option('-r', '--replica', action='store_true', dest='replica', default=False, help='Adds a _Disk Non-Custodial Replica parameter')
    parser.add_option('-p', '--procversion', help='Processing Version, if empty it will leave the processing version that comes by default in the request', dest='procversion')
    parser.add_option('-a', '--activity', help='Dashboard Activity (reprocessing, production or test), if empty will set reprocessing as default', dest='activity')
    parser.add_option( '--xrootd', help='Assign with TrustSitelists=True (allows xrootd capabilities)',
                      action='store_true', dest='xrootd')
    parser.add_option('--no_xrootd', help='Assign with TrustSitelists=False',
                      action='store_false', dest='xrootd')
    parser.add_option('--secondary_xrootd', help='Assign with TrustPUSitelists=True (allows xrootd capabilities)',
                      action='store_true', dest='secondary_xrootd')
    parser.add_option('--no_secondary_xrootd', help='Assign with TrustPUSitelists=False',
                      action='store_false', dest='secondary_xrootd')
    parser.add_option('-l', '--lfn', help='Merged LFN base', dest='lfn')
    parser.add_option('-v', '--verbose', help='Verbose', action='store_true', default=False, dest='verbose')
    parser.add_option('--testbed', help='Assign in testbed', action='store_true', default=False, dest='testbed')
    parser.add_option('--test', action="store_true",help='Nothing is injected, only print infomation about workflow and Era', dest='test')
    parser.add_option('-f', '--file', help='Text file with a list of wokflows. If this option is used, the same settings will be applied to all workflows', dest='file')
    parser.add_option('-w', '--workflow', help='Workflow Name, or coma sperated list', dest='workflow')
    parser.add_option('-m', '--memory', help='Set the Memory parameter to the workflow', dest='memory', default=None)
    parser.add_option('--lumisperjob',help='Set the number of lumis per job', default=None, type=int)
    parser.add_option('--maxmergeevents',help='Set the number of event to merge at max', default=None, type=int)
    parser.add_option('-c', '--multicore', help='Set the multicore parameter to the workfllow', dest='multicore', default=None)
    parser.add_option('-e', '--era', help='Acquistion era', dest='era')
    parser.add_option("--procstr", dest="procstring", help="Overrides Processing String with a single string")
    parser.add_option('--checksite', default=False,action='store_true')
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
        wfs = options.workflow.split(',')

    #Default values
    era = {}
    procversion = 1
    procstring = {}
    memory = None
    multicore = None
    replica = False
    sites = []
    specialStr = ''
    taskchain = False
    xrootd= False
    secondary_xrootd= False

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

    if options.replica:
        replica = True

    for wfn in wfs:
        # Getting the original dictionary
        wfi = workflowInfo( url, wfn )
        schema = wfi.request
        if 'OriginalRequestName' in schema:
            print "Original workflow is:",schema['OriginalRequestName']
            original_wf = workflowInfo(url, schema['OriginalRequestName'])            
            ancestor_wf = workflowInfo(url, schema['OriginalRequestName'])
            ## go back as up as possible
            while ancestor_wf.request['RequestType'] == 'Resubmission':
                if 'OriginalRequestName' not in ancestor_wf.request:
                    ancestor_wf = None
                    break
                ancestor_wf = workflowInfo(url, ancestor_wf.request['OriginalRequestName'])
        else:
            original_wf = None
            ancestor_wf = None

        is_resubmission = (schema['RequestType'] == 'Resubmission')

        if options.sites.lower() == 'original' and original_wf:
            sites = original_wf.request['SiteWhitelist']
            print "Using",sorted(sites),"from the original request",original_wf.request['RequestName']

        #print json.dumps( schema, indent=2 )
        wf_name = wfn
        wf_info = schema

        # WF must be in assignment-approved in order to be assigned
        if (schema["RequestStatus"] != "assignment-approved") and not options.test:
            print("The workflow '" + wf_name + "' you are trying to assign is not in assignment-approved")
            sys.exit(1)

        #Check to see if the workflow is a task chain or an ACDC of a taskchain
        taskchain = (schema["RequestType"] == "TaskChain") or (ancestor_wf and ancestor_wf.request["RequestType"] == "TaskChain")

        # Adding the special string - in case it was provided in the command line
        if options.special:
            specialStr = '_' + str(options.special)
            for key, value in procstring.items():
                procstring[key] = value + specialStr

        # Override if a value is given using the procstring command
        if options.procstring:
            procstring = options.procstring
        elif is_resubmission:
            procstring = ancestor_wf.processingString()
        else:
            procstring = wfi.processingString()

        if options.era:
            era = options.era
        elif is_resubmission:
            era = ancestor_wf.acquisitionEra()
        else:
            era = wfi.acquisitionEra()
        #Dealing with era and proc string
        if (not era or not procstring) or (taskchain and (type(era)!=dict or type(procstring)!=dict)):
            print "We do not have a valid AcquisitionEra and ProcessingString"
            sys.exit(1)

        # Must use --lfn option, otherwise workflow won't be assigned
        if options.lfn:
            lfn = options.lfn
        elif "MergedLFNBase" in wf_info:
            lfn = wf_info['MergedLFNBase']
        elif ancestor_wf and "MergedLFNBase" in ancestor_wf.request:
            lfn = ancestor_wf.request['MergedLFNBase']
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

        if options.multicore:
            multicore = options.multicore

        # given or default processing version
        if options.procversion:
            procversion = int(options.procversion)
        else:
            if is_resubmission:
                procversion = ancestor_wf.request['ProcessingVersion']
            else:
                procversion = wf_info["ProcessingVersion"]

        # reading xrootd and secondary_xrootd values
        if options.xrootd is not None:
            xrootd = options.xrootd
        elif original_wf:
            xrootd= original_wf.request["TrustSitelists"]

        if options.secondary_xrootd is not None:
            secondary_xrootd = options.secondary_xrootd
        elif original_wf:
            secondary_xrootd= original_wf.request["TrustPUSitelists"]

        # Check for output dataset existence, and abort if output datasets already exist!
        # Don't perform this check for ACDC's
        datasets = schema["OutputDatasets"]
        i = 0
        if not is_resubmission:
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
                sites = list(set([SI.SE_to_CE(site) for site in where_to_run[task]]) & set(SI.all_sites))
                print "Found",sorted(sites),"as sites where to run the ACDC at, from the acdc doc of ",original_wf.request['RequestName']

        if options.checksite:
            ## check that the sites are all compatible and up
            check_mem = schema['Memory'] if not memory else memory
            ncores = wfi.getMulticore() if not multicore else multicore
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


        ## need to play with memory setting
        if taskchain:
            if memory:
                ## transform into a dictionnary
                increase = set_to = None
                tasks,set_to = memory.split(':') if ':' in memory else ("",memory)
                tasks = tasks.split(',') if tasks else []
                if set_to.startswith('+'):
                    increase = int(set_to[1:])
                else:
                    set_to = int(set_to)
                it = 1
                memory_dict = {}
                while True:
                    t = 'Task%d'%it
                    it += 1
                    if t in schema:
                        tname = schema[t]['TaskName']
                        if tasks and not tname in tasks:
                            print tname,"not concerned"
                            memory_dict[tname] = schema[t]['Memory']
                            continue
                        if set_to:
                            memory_dict[tname] = set_to
                        else:
                            memory_dict[tname] =schema[t]['Memory'] + increase
                    else:
                        break
                memory = memory_dict
                print memory_dict
            ## need to play with multicore setting
            if multicore:
                tasks,set_to = multicore.split(':') if ':' in multicore else ("",multicore)
                tasks = tasks.split(',') if tasks else []
                set_to = int(set_to)
                multicore_dict = {}
                timeperevent_dict = {}
                it=1
                while True:
                    t = 'Task%d'%it
                    it += 1
                    if t in schema:
                        tname = schema[t]['TaskName']
                        mcore = schema[t]['Multicore']
                        if tasks and not tname in tasks:
                            print tname,"not concerned"
                            multicore_dict[tname] = schema[t]['Multicore']
                            timeperevent_dict[tname] = schema[t]['TimePerEvent']
                            continue
                        mem = memory[tname]
                        factor = (set_to / float(mcore))
                        fraction_constant = 0.4
                        mem_per_core_c = int((1-fraction_constant) * mem / float(mcore))
                        print "mem per core", mem_per_core_c
                        print "base mem", mem
                        ## need to adjut the memory at the same time
                        ## will crash of --mem was not set in argument :FINE
                        memory[tname] = mem + (set_to-mcore)*mem_per_core_c
                        print "final mem",memory[tname]
                        timeperevent_dict[tname] = schema[t]['TimePerEvent']/factor
                        print "setting mcore",set_to
                        multicore_dict[tname] = set_to
                    else:
                        break
                multicore = multicore_dict
                print multicore
                print timeperevent_dict,"cannot be used yet."
    # If the --test argument was provided, then just print the information
    # gathered so far and abort the assignment
        print wf_name
        print "Era:",era
        print "ProcStr:",procstring
        print "ProcVer:",procversion
        print "LFN:",lfn
        print "Team:",options.team
        print "Site:",sites
        print "Taskchain? ", str(taskchain)
        print "Activity:", activity
        print "ACDC:", str(is_resubmission)
        print "Xrootd:", str(xrootd)
        print "Secondary_xrootd:", str(secondary_xrootd)
        #if options.test:            continue
        
        # Really assigning the workflow now
        #print wf_name, '\tEra:', era, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tTeam:', team, '\tSite:', sites
        assignRequest(url, 
                      workflow = wf_name,
                      team = options.team,
                      sites = sites,
                      era = era, 
                      procversion = procversion,
                      activity = activity,
                      lfn = lfn,
                      procstring = procstring, 
                      trust_site = xrootd, 
                      replica = options.replica, 
                      verbose = options.test, 
                      taskchain = taskchain, 
                      trust_secondary_site = secondary_xrootd,
                      memory=memory,
                      multicore=multicore,
                      lumisperjob = options.lumisperjob,
                      maxmergeevents = options.maxmergeevents
                      )
    
    sys.exit(0)

if __name__ == "__main__":
    main()
