#!/usr/bin/env python

# Combined makeACDC.py, assign.py into an auto ACDC script
# Author: Luca Lavezzo
# July 2022

"""
    The script is meant to automatically submit ACDCs and assign them
    for multiple tasks/workflows. The logic by which the options
    of the ACDC are made are contained in a script that calls this
    class to actually submit/assign them.
"""

import http.client
import os, sys, re
import logging
import json
import copy
import optparse
from optparse import OptionParser
from pprint import pprint
from random import choice

from dbs.apis.dbsClient import DbsApi
import reqMgrClient
import reqMgrClient as reqMgr
from utils import workflowInfo, siteInfo
from collections import defaultdict 
from Unified.recoveror import singleRecovery

logging.basicConfig(level=logging.WARNING)


class autoACDC()

    def __init__(self):

        self.dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        self.url = 'cmsweb.cern.ch'
        self.url_tb = 'cmsweb-testbed.cern.ch'

    def getRandomDiskSite(self, site=None):
        """
            Gets a random disk site and append _Disk
            """
        if site == None:
            site = getRandomDiskSite.T1s
        s = choice(site)
        if s.startswith("T1"):
            s += "_Disk"
        return s

    def assignRequest(self, url, **args):
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
            params["NonCustodialSites"] = self.getRandomDiskSite(),
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
            print('Assigned workflow:', workflow, 'to site:', sites, 'with processing version', procversion)
        else:
            print('Could not assign workflow:', workflow, 'to site:', sites, 'with processing version', procversion)
        if verbose:
            print(res)

    def getRequestDict(self, url, workflow):
        conn = http.client.HTTPSConnection(url, cert_file=os.getenv(
            'X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
        r1 = conn.request("GET", '/reqmgr/reqMgr/request?requestName=' + workflow)
        r2 = conn.getresponse()
        request = json.loads(r2.read())
        return request

    def getACDCServerInfo(self, url):
        conn = http.client.HTTPSConnection(url, cert_file=os.getenv(
            'X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
        a = """https://cmsweb.cern.ch/couchdb/acdcserver/_design/ACDC/_view/byCollectionName?key=%22pdmvserv_task_HIG-RunIISummer20UL17NanoAODv9-03145__v1_T_220407_021816_1328%22&include_docs=true&reduce=false"""

    def makeACDCRequest(self, **args):
        url = args.get('url')
        wfi = args.get('wfi')
        task = args.get('task')
        initial = wfi
        actions = []
        memory = args.get('memory',None)
        if memory:
            #increment = initial.request['Memory'] - memory
            #actions.append( 'mem-%d'% increment )
            actions.append( 'mem-%s'% memory )
        mcore = args.get('mcore',None)
        if mcore:
            actions.append( 'core-%s'% mcore)
        xrootd = args.get('xrootd',None)
        if xrootd:
            actions.append( 'xrootd-%s'% xrootd)
            
        acdc = singleRecovery(url, task, initial.request, actions, do=True)
        if acdc:
            return acdc
        else:
            print("Issue while creating the acdc for",task)
            return None

    def assign(self, **args):
        
        team = args.get('team')
        sites = args.get('sites') # options: t1, t2, acdc
        checksite = args.get('checksite') # bool
        exclude_sites = args.get('exclude_sites') # site name, or comma separated list
        special = args.get('special') # Use it for special workflows. You also have to change the code according to the type of WF
        replica = args.get('replica') # Adds a _Disk Non-Custodial Replica parameter
        procversion = args.get('procversion') # Processing Version, if empty it will leave the processing version that comes by default in the request
        activity = args.get('activity') # Dashboard Activity (reprocessing, production or test), if empty will set reprocessing as default
        xrootd = args.get('xrootd', False) # Assign with TrustSitelists=True (allows xrootd capabilities)
        secondary_xrootd = args.get('secondary_xrootd', False)
        lfn = args.get('lfn') # Merged LFN base
        verbose = args.get('verbose', False) 
        testbed = args.get('testbed', False) # Assign in testbed
        test = args.get('test') # Nothing is injected, only print infomation about workflow and Era
        file = args.get('file') # Text file with a list of wokflows. If this option is used, the same settings will be applied to all workflows
        workflow = args.get('workflow') # Workflow Name, or coma sperated list
        memory = args.get('memory') # Set the Memory parameter to the workflow
        lumisperjob = args.get('lumisperjob') # Set the number of lumis per job
        maxmergeevents = args.get('maxmergeevents') # Set the number of event to merge at max
        multicore = args.get('multicore', False) # Set the multicore parameter to the workfllow
        era = args.get('era') # Acquistion era
        procstring = args.get('procstring') # Overrides Processing String with a single string
        
        if testbed:
            self.url = self.url_tb

        # parse input workflows and files. If both -w and -f options are used, then only the -w inputs are considered.
        if not workflow:
            if args:
                wfs = args
            elif file:
                wfs = [l.strip() for l in open(file) if l.strip()]
            else:
                parser.error("Input a workflow name or a file to read them")
                sys.exit(0)
        else:
            wfs = workflow.split(',')

        #Default values
        era = {}
        procversion = 1
        procstring = {}
        memory = None
        multicore = False
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
        if sites.lower() == "t1":
            sites = SI.sites_T1s
        elif sites.lower() == "t2":
            sites = SI.sites_T2s
        elif sites.lower() in ["all","t1+t2","t2+t1"] :
            sites = SI.sites_T2s+SI.sites_T1s
        elif sites.lower() == "mcore":
            sites = SI.sites_mcore_ready
        elif hasattr(SI,sites):
            sites = getattr(SI,sites)
        #elif sites.lower() == 'acdc':
        #    sites = []
        else: 
            sites = [site for site in sites.split(',')]
        

        if replica:
            replica = True

        for wfn in wfs:
            # Getting the original dictionary
            wfi = workflowInfo( url, wfn )
            schema = wfi.request

            if 'OriginalRequestName' in schema:
                print("Original workflow is:",schema['OriginalRequestName'])
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

            if sites.lower() == 'original' and original_wf:
                sites = original_wf.request['SiteWhitelist']
                print("Using",sorted(sites),"from the original request",original_wf.request['RequestName'])

            #print json.dumps( schema, indent=2 )
            wf_name = wfn
            wf_info = schema

            # WF must be in assignment-approved in order to be assigned
            if (schema["RequestStatus"] != "assignment-approved") and not test:
                print(("The workflow '" + wf_name + "' you are trying to assign is not in assignment-approved"))
                sys.exit(1)

            #Check to see if the workflow is a task chain or an ACDC of a taskchain
            taskchain = (schema["RequestType"] == "TaskChain") or (ancestor_wf and ancestor_wf.request["RequestType"] == "TaskChain")

            # Adding the special string - in case it was provided in the command line
            if special:
                specialStr = '_' + str(special)
                for key, value in list(procstring.items()):
                    procstring[key] = value + specialStr

            # Override if a value is given using the procstring command
            if procstring:
                procstring = procstring
            elif is_resubmission:
                procstring = ancestor_wf.processingString()
            else:
                procstring = wfi.processingString()

            if era:
                era = era
            elif is_resubmission:
                era = ancestor_wf.acquisitionEra()
            else:
                era = wfi.acquisitionEra()
            #Dealing with era and proc string
            if (not era or not procstring) or (taskchain and (type(era)!=dict or type(procstring)!=dict)):
                print("We do not have a valid AcquisitionEra and ProcessingString")
                sys.exit(1)

            # Must use --lfn option, otherwise workflow won't be assigned
            if lfn:
                lfn = lfn
            elif "MergedLFNBase" in wf_info:
                lfn = wf_info['MergedLFNBase']
            elif ancestor_wf and "MergedLFNBase" in ancestor_wf.request:
                lfn = ancestor_wf.request['MergedLFNBase']
            else:
                print("Can't assign the workflow! Please include workflow lfn using --lfn option.")
                sys.exit(0)
            # activity production by default for taskchains, reprocessing for default by workflows
            if activity:
                activity = activity
            elif taskchain:
                activity = 'production'
            else:
                activity = 'reprocessing'

            if memory:
                memory = memory

            if multicore:
                multicore = multicore

            # given or default processing version
            if procversion:
                procversion = int(procversion)
            else:
                if is_resubmission:
                    procversion = ancestor_wf.request['ProcessingVersion']
                else:
                    procversion = wf_info["ProcessingVersion"]

            # reading xrootd and secondary_xrootd values
            if xrootd is not None:
                xrootd = xrootd
            elif original_wf:
                xrootd= original_wf.request["TrustSitelists"]

            if secondary_xrootd is not None:
                secondary_xrootd = secondary_xrootd
            elif original_wf:
                secondary_xrootd= original_wf.request["TrustPUSitelists"]

            # Check for output dataset existence, and abort if output datasets already exist!
            # Don't perform this check for ACDC's
            datasets = schema["OutputDatasets"]
            i = 0
            if not is_resubmission:
                exist = False
                maxv = 1
                for key, value in list(schema.items()):
                    if type(value) is dict and key.startswith("Task"):
                        dbsapi = DbsApi(url=dbs3_url)
                        
                        # list all datasets with same name but different version
                        # numbers
                        datasets = dbsapi.listDatasets(acquisition_era_name=value['AcquisitionEra'], primary_ds_name=value['PrimaryDataset'], detail=True, dataset_access_type='*')
                        processedName = value['AcquisitionEra'] + '-' + value['ProcessingString'] + "-v\\d+"
                        # see if any of the dataset names is a match
                        for ds in datasets:
                            if re.match(processedName, ds['processed_ds_name']):
                                print("Existing dset:", ds['dataset'], "(%s)" % ds['dataset_access_type'])
                                maxv = max(maxv, ds['processing_version'])
                                exist = True
                            else:
                                 pass
                        i += 1
                # suggest max version
                if exist and procversion <= maxv:
                    print("Some output datasets exist, its advised to assign with v ==", maxv + 1)
                    sys.exit(0)
            else:
                ## this is a resubmission !
                print("The taks in resubmission is:",schema['InitialTaskPath'])
                ## pick up the sites from acdc
                if sites.lower() == 'acdc':
                    where_to_run, missing_to_run, missing_to_run_at =  original_wf.getRecoveryInfo()
                    task = schema['InitialTaskPath']
                    sites = list(set([SI.SE_to_CE(site) for site in where_to_run[task]]) & set(SI.all_sites))
                    print("Found",sorted(sites),"as sites where to run the ACDC at, from the acdc doc of ",original_wf.request['RequestName'])

            if checksite:
                ## check that the sites are all compatible and up
                check_mem = schema['Memory'] if not memory else memory
                # ncores = wfi.getMulticore() if not multicore else multicore
                # memory_allowed = SI.sitesByMemory( float(check_mem), maxCore=ncores)
                not_ready = sorted(set(sites) & set(SI.sites_not_ready))
                not_existing = sorted(set(sites) - set(SI.all_sites))
                #not_matching = sorted((set(sites) - set(memory_allowed) - set(not_ready) - set(not_existing)))
                not_matching = sorted((set(sites) - set(not_ready) - set(not_existing)))
                previously_used = []
                if schema['SiteWhitelist']: previously_used = schema['SiteWhitelist']
                if original_wf: previously_used = original_wf.request['SiteWhitelist']
                #if previously_used: not_matching = sorted(set(not_matching) & set(previously_used))

                #sites = sorted( set(sites) - set(not_matching) - set(not_existing))
                sites = sorted( set(sites) - set(not_ready) - set(not_existing))
        
                # print(sorted(memory_allowed),"to allow",check_mem,ncores)
                # if not_ready:
                #     print(not_ready,"is/are not ready")
                #     sys.exit(0)
                #if not_matching:
                #    print("The memory requirement",check_mem,"is too much for",not_matching)
                #    sys.exit(0)

                if sites.lower() == 'acdc':
                    if len(sites) == 0:
                        print("None of the necessary sites are ready:", sites)
                        continue
                    elif len(not_ready) > 0: 
                        print("Some of the necessary sites are ready:", sites)
                        xrootd = True
                    else:
                        print("All necessary sites are available")

            # provide a list of site names to exclude
            if exclude_sites is not None:
                excludeSites = exclude_sites.split(',')
                sites = sorted(set(sites) - set(excludeSites))

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
                                print(tname,"not concerned")
                                memory_dict[tname] = schema[t]['Memory']
                                continue
                            if set_to:
                                memory_dict[tname] = set_to
                            else:
                                memory_dict[tname] =schema[t]['Memory'] + increase
                        else:
                            break
                    memory = memory_dict
                    print(memory_dict)
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
                                print(tname,"not concerned")
                                multicore_dict[tname] = schema[t]['Multicore']
                                timeperevent_dict[tname] = schema[t]['TimePerEvent']
                                continue
                            if memory:
                                mem = memory[tname]
                                print(mem, memory)
                                factor = (set_to / float(mcore))
                                fraction_constant = 0.4
                                mem_per_core_c = int((1-fraction_constant) * mem / float(mcore))
                                print("mem per core", mem_per_core_c)
                                print("base mem", mem)
                                
                                memory[tname] = mem + (set_to-mcore)*mem_per_core_c
                                print("final mem",memory[tname])
                                timeperevent_dict[tname] = schema[t]['TimePerEvent']/factor
                            print("setting mcore",set_to)
                            multicore_dict[tname] = set_to
                        else:
                            break
                    multicore = multicore_dict
                    print(multicore)
                    print(timeperevent_dict,"cannot be used yet.")
        # If the --test argument was provided, then just print the information
        # gathered so far and abort the assignment
            print(wf_name)
            print("Era:",era)
            print("ProcStr:",procstring)
            print("ProcVer:",procversion)
            print("LFN:",lfn)
            print("Team:",team)
            print("Site:",sites)
            print("Taskchain? ", str(taskchain))
            print("Activity:", activity)
            print("ACDC:", str(is_resubmission))
            print("Xrootd:", str(xrootd))
            print("Secondary_xrootd:", str(secondary_xrootd))
            #if test:            continue
            
            # Really assigning the workflow now
            #print wf_name, '\tEra:', era, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tTeam:', team, '\tSite:', sites
            self.assignRequest(url, 
                          workflow = wf_name,
                          team = team,
                          sites = sites,
                          era = era, 
                          procversion = procversion,
                          activity = activity,
                          lfn = lfn,
                          procstring = procstring, 
                          trust_site = xrootd, 
                          replica = replica, 
                          verbose = test, 
                          taskchain = taskchain, 
                          trust_secondary_site = secondary_xrootd,
                          memory=memory,
                          multicore=multicore,
                          lumisperjob = lumisperjob,
                          maxmergeevents = maxmergeevents
                          )
        
        sys.exit(0)


    def makeACDC(self, **args):

        file = args.get('file') # Text file with a list of workflows
        workflow = args.get('workflow') # Comma separated list of wf to handle
        task = args.get('task') # Comma separated task to be recovered
        path = args.get('path') # Comma separated list of paths to recover
        recover_all = args.get('recover_all', False) # Make acdc for all tasks to be recovered
        memory = args.get('memory') # Memory to override the original request memory
        mcore = args.get('mcore') # Multicore to override the original request multicore
        xrootd = args.get('xrootd') # Enable xrootd
        out_file = args.get("out_file", 'acdc_wf_list.txt') # Output file, to be filled with workflows for which ACDC was submitted.
        testbed = args.get('testbed', False)
   
        self.url = testbed_url if testbed else self.prod_url

        outACDClist = out_file
        if os.path.isfile(outACDClist):
            sys.exit("Make a new name for output file.")

        if recover_all : task = 'all'

        if not task:
            parser.error("Provide the -t Task Name or --all")
            sys.exit(1)

        if not ((workflow) or (path) or (file)):
            parser.error("Provide the -w Workflow Name or the -p path or the -f workflow filelist")
            sys.exit(1)
        
        wfs = None
        wf_and_task = defaultdict(set)
        if file:
            wfs = [l.strip() for l in open(file) if l.strip()]
        elif workflow:
            wfs = workflow.split(',')
        elif path:
            ## self contained
            paths = path.split(',')
            for p in paths:
                _,wf,t = p.split('/',2)
                wf_and_task[wf].add('/%s/%s'%(wf,t))
        else:
            parser.error("Either provide a -f filelist or a -w workflow or -p path")
            sys.exit(1)

        if not wf_and_task:
            if task == 'all':
                for wfname in wfs: 
                    wf_and_task[wfname] = None
            else:
                for wfname in wfs: 
                    wf_and_task[wfname].update( [('/%s/%s'%(wfname,task)).replace('//','/') for task in task.split(',')] )

        if not wf_and_task:
            parser.error("Provide the -w Workflow Name and the -t Task Name or --all")
            sys.exit(1)        

        
        for wfname,tasks in wf_and_task.items():
            wfi = workflowInfo(url, wfname)
            if tasks == None:
                where,how_much,how_much_where = wfi.getRecoveryInfo()
                tasks = sorted(how_much.keys())
            else:
                tasks = sorted(tasks)

            created = {}
            print("Workflow:",wfname)
            print("Tasks:",tasks)

            # FIXME: eventually, we want to be able to target each task
            # with different options
            if len(tasks) != 1:
                print("WARNING: Multiple tasks were found in this workflow \
                        be sure that you want to submit identical ACDCs for all tasks.")

            # create an ACDC workflow
            for task in tasks:
                r = makeACDCRequest(url=url, wfi=wfi, task=task,
                             memory = memory,
                             mcore = mcore,
                             xrootd = xrootd) 
                if not r: 
                    print("Error in creating ACDC for",task,"on",wfname)
                    break
                created[task] = r

            if len(created)!=len(tasks):
                print("Error in creating all required ACDCs")
                sys.exit(1)

            print("Created:")
            for task in created:
                print(created[task],"for",task)
                with open(outACDClist, 'a') as f: f.write(str(created[task])+"\n")