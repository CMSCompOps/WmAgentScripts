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
from typing import Optional, List, Tuple
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

    def __init__(self, taskName: str, testbed: Optional[bool] = False):

        self.testbed = testbed
        self.taskName = taskName
        self.exclude_sites = None
        self.xrootd = False
        self.secondary_xrootd = False
        self.memory = None
        self.multicore = None

        self.dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        self.url_real = 'cmsweb.cern.ch'
        self.url_tb = 'cmsweb-testbed.cern.ch'
        self.url = self.url_tb if self.testbed else self.url_real
        self.wfInfo = workflowInfo(self.url, self.taskName)
        self.sites  = self.setSites()

        self.acdcWf = None
        self.schema = None
        
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

    def makeACDCRequest(self, **args):
        
        actions = []
        if self.memory:
            actions.append( 'mem-%s'% self.memory )
        if self.mcore:
            actions.append( 'core-%s'% self.mcore)
        if self.xrootd:
            actions.append( 'xrootd-%s'% self.xrootd)
            
        acdc = singleRecovery(self.url, self.taskName, self.wfInfo.request, actions, do=True)
        if acdc:
            self.acdcWf = acdc
        else:
            raise Exception("Issue while creating ACDC for "+task)

    def checkSites(self, sites):

        SI = siteInfo()

        not_ready = sorted(set(sites) & set(SI.sites_not_ready))
        not_existing = sorted(set(sites) - set(SI.all_sites))
        not_matching = sorted((set(sites) - set(not_ready) - set(not_existing)))

        sites = sorted(set(sites) - set(not_ready) - set(not_existing))

        # if any (but not all) of the sites are down
        # enable xrootd and run anyways
        if sites.lower() == 'acdc':
            if len(sites) == 0:
                raise Exception("None of the necessary sites are ready")
            elif len(not_ready) > 0: 
                print("Some of the necessary sites are not ready:",  set(not_ready))
                self.xrootd = True
             else:
                print("All necessary sites are available")

        return sites

    def getACDCsites(self):

        SI = siteInfo()
        original_wf = workflowInfo(self.url, self.schema['OriginalRequestName']) 
            
        where_to_run, missing_to_run, missing_to_run_at =  original_wf.getRecoveryInfo()
        task = schema['InitialTaskPath']
        sites = list(set([SI.SE_to_CE(site) for site in where_to_run[task]]) & set(SI.all_sites))

        return sites


    def setSites(self):

        sites = self.getACDCsites()

        # check if all desired sites are up and running
        sites = self.checkSites(sites)

        # provide a list of site names to exclude
        if self.exclude_sites is not None:
            sites = sorted(set(sites) - set(self.excludeSites))

        return sites


    def assign(self, **args):
        
        team = args.get('team')
        replica = args.get('replica', False) # Adds a _Disk Non-Custodial Replica parameter
        activity = args.get('activity') # Dashboard Activity (reprocessing, production or test), if empty will set reprocessing as default
        lfn = args.get('lfn') # Merged LFN base
        lumisperjob = args.get('lumisperjob') # Set the number of lumis per job
        maxmergeevents = args.get('maxmergeevents') # Set the number of event to merge at max

        # WF must be in Resubmission in order to be assigned
        if not (self.schema['RequestType'] == 'Resubmission') and not self.testbed: 
            raise Exception("RequestType is not 'Resubmission'")

        # WF must be in assignment-approved in order to be assigned
        if (self.schema["RequestStatus"] != "assignment-approved") and not self.testbed:
            raise Exception("RequestType is not 'assignment-approved'")

        if 'OriginalRequestName' in self.schema:
            print("Original workflow is:",self.schema['OriginalRequestName'])
            original_wf = workflowInfo(url, self.schema['OriginalRequestName'])            
            ancestor_wf = workflowInfo(url, self.schema['OriginalRequestName'])
            ## go back as up as possible
            while ancestor_wf.request['RequestType'] == 'Resubmission':
                if 'OriginalRequestName' not in ancestor_wf.request:
                    ancestor_wf = None
                    break
                ancestor_wf = workflowInfo(url, ancestor_wf.request['OriginalRequestName'])
        else:
            raise Exception("'OriginalRequestName' not in schema.")

        # check to see if the workflow is a task chain or an ACDC of a taskchain
        taskchain = (self.schema["RequestType"] == "TaskChain") or (ancestor_wf and ancestor_wf.request["RequestType"] == "TaskChain")
        
        # determine era, procstring, procversion from ancestor workflow
        era = ancestor_wf.acquisitionEra()
        procstring = ancestor_wf.processingString()
        if (not era or not procstring) or (taskchain and (type(era)!=dict or type(procstring)!=dict)):
            raise Exception("We do not have a valid AcquisitionEra and ProcessingString")
        procversion = ancestor_wf.request['ProcessingVersion']
        
        # Must use --lfn option, otherwise workflow won't be assigned
        if lfn:
            lfn = lfn
        elif "MergedLFNBase" in self.schema:
            lfn = self.schema['MergedLFNBase']
        elif ancestor_wf and "MergedLFNBase" in ancestor_wf.request:
            lfn = ancestor_wf.request['MergedLFNBase']
        else:
            raise Exception("Can't assign the workflow! Please include workflow lfn using --lfn option.")
        
        # activity production by default for taskchains, reprocessing for default by workflows
        if activity:
            activity = activity
        elif taskchain:
            activity = 'production'
        else:
            activity = 'reprocessing'

        # FIXME
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
                memory_dict = memory_dict
                print(memory_dict)

            # FIXME
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
        print(self.acdcWf)
        print("Era:",era)
        print("ProcStr:",procstring)
        print("ProcVer:",procversion)
        print("LFN:",lfn)
        print("Team:",team)
        print("Site(s):", self.sites)
        print("Taskchain: ", str(taskchain))
        print("Activity:", activity)
        print("XRootD:", str(self.xrootd))
        print("Secondary XRootD:", str(self.secondary_xrootd))
        if test:
            continue
        
        # Really assigning the workflow now
        self.assignRequest(url, 
                      workflow = self.acdcWf,
                      trust_site = self.xrootd, 
                      verbose = self.testbed,
                      sites = self.sites,
                      trust_secondary_site = self.secondary_xrootd,

                      memory=memory,
                      multicore=multicore,
                      team = team,
                      era = era, 
                      procversion = procversion,
                      activity = activity,
                      lfn = lfn,
                      procstring = procstring, 
                      replica = replica, 
                      taskchain = taskchain, 
                      lumisperjob = lumisperjob,
                      maxmergeevents = maxmergeevents
                      )