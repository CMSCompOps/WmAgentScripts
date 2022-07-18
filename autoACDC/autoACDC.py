#!/usr/bin/env python

# Script: autoACDC.py
# Author: Luca Lavezzo
# Date: July 2022

"""
    The script is meant to automatically submit ACDCs and assign them
    for multiple tasks/workflows. The logic by which the options
    of the ACDC are made are contained in a script that calls this
    class to actually submit/assign them. This script is based on
    makeACDC.py and assign.py.
"""

import http.client
import os, sys, re
import logging
from typing import Optional, List, Tuple
from random import choice

from dbs.apis.dbsClient import DbsApi
import reqMgrClient
import reqMgrClient as reqMgr
from utils import workflowInfo, siteInfo
from collections import defaultdict 
from Unified.recoveror import singleRecovery

logging.basicConfig(level=logging.WARNING)


class autoACDC()

    def __init__(self, taskName, **args):

        self.taskName = taskName

        self.options = {
            "testbed": args.get('testbed', False),
            "testbed_assign": args.get('testbed_assign', False),
            "exclude_sites": args.get('exclude_sites', None),
            "xrootd": args.get('xrootd', False),
            "secondary_xrootd": args.get('secondary_xrootd', False),
            "memory": args.get('memory', None),
            "multicore": args.get('multicore', None),
            "team": args.get('team'),
            "replica": args.get('replica', False), # Adds a _Disk Non-Custodial Replica parameter
            "activity": args.get('activity'), # Dashboard Activity (reprocessing, production or test), if empty will set reprocessing as default
            "lfn": args.get('lfn'), # Merged LFN base
            "lumisperjob": args.get('lumisperjob', None), # Set the number of lumis per job
            "maxmergeevents": args.get('maxmergeevents', None) # Set the number of event to merge at max        
        }
        
        self.dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        self.url_real = 'cmsweb.cern.ch'
        self.url_tb = 'cmsweb-testbed.cern.ch'
        self.url = self.url_tb if self.options['testbed'] else self.url_real
        self.wfInfo = workflowInfo(self.url, self.taskName)

        self.acdcWf = None
        self.acdcInfo = None
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

    def getAssignParameters(self):

        self.acdcInfo = workflowInfo(self.url, self.acdcWf)
        self.schema = self.acdcInfo.request

        # WF must be in Resubmission in order to be assigned
        if not (self.schema['RequestType'] == 'Resubmission') and not self.options['testbed_assign']: 
            raise Exception("RequestType is not 'Resubmission'")

        # WF must be in assignment-approved in order to be assigned
        if (self.schema["RequestStatus"] != "assignment-approved") and not self.options['testbed_assign']:
            raise Exception("RequestType is not 'assignment-approved'")

        if 'OriginalRequestName' in self.schema:
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
        taskchain = (schema["RequestType"] == "TaskChain") or (ancestor_wf and ancestor_wf.request["RequestType"] == "TaskChain")

        # these are automatically determined from ancestor workflow
        era = ancestor_wf.acquisitionEra()
        procstring = ancestor_wf.processingString()
        if (not era or not procstring) or (taskchain and (type(era)!=dict or type(procstring)!=dict)):
            raise Exception("We do not have a valid AcquisitionEra and ProcessingString")
        procversion = ancestor_wf.request['ProcessingVersion']
        
        # some default options in case the arguments aren't passed

        # Must use --lfn option, otherwise workflow won't be assigned
        if self.options['lnf']:
            lfn = self.options['lnf']
        elif "MergedLFNBase" in self.schema:
            lfn = self.schema['MergedLFNBase']
        elif ancestor_wf and "MergedLFNBase" in ancestor_wf.request:
            lfn = ancestor_wf.request['MergedLFNBase']
        else:
            raise Exception("Can't assign the workflow! Please include workflow lfn using --lfn option.")
        
        # activity production by default for taskchains, reprocessing for default by workflows
        if self.options['activity']:
            activity = self.options['activity']
        elif taskchain:
            activity = 'production'
        else:
            activity = 'reprocessing'    

        params = {
            "SiteWhitelist": self.getSites(),
            "MergedLFNBase": lfn,
            "Dashboard": activity,
            "ProcessingVersion": procversion,
            "execute": True,
            "AcquisitionEra": era,
            "ProcessingString": procstring,
            "TrustSitelists": self.options['xrootd'],
            "TrustPUSitelists": self.options['secondary_xrootd']
        }

        if self.options['replica']:
            params["NonCustodialSites"] = self.getRandomDiskSite(),
            params["NonCustodialSubType"] = "Replica"
            if taskchain:
                params['AutoApproveSubscriptionSites'] = [params["NonCustodialSites"]]

        if self.options['testbed_assign']:
            params['execute'] = False    

        if self.options['maxmergeevents'] is not None:
            params['MaxMergeEvents'] = self.options['maxmergeevents']

        if self.options['lumisperjob'] is not None:
            params['LumisPerJob'] = self.options['lumisperjob']

        # FIXME: would we ever have a taskchain?
        # FIXME: clean this up
        ## need to play with memory setting
        if taskchain:
            if self.memory:

                if memory.startswith('+'):
                    increase = int(memory[1:])
                    memory = schema[t]['Memory'] + increase
                else:
                    memory = int(memory)

                memory = memory_dict

            # FIXME: clean this up
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

        if memory:
            params["Memory"] = memory if type(memory)==dict else int(memory)
                        
        if multicore:
            params["Multicore"] = multicore if type(multicore)==dict else int(multicore)

        return params


    def assign(self, url, **args):

        params = getAssignParameters(self)

        # testing
        if self.options['testbed_assign']:
            logging.info(self.acdcWf)
            logging.info(params)
            sys.exit("Running with testbed_assign on, quitting.")

        res = reqMgr.assignWorkflow(self.url, self.acdcWf, self.options['team'], params)
        if not res:
            raise Exception("Could not assing workflow.")

    def checkSites(self, sites):

        SI = siteInfo()

        not_ready = sorted(set(sites) & set(SI.sites_not_ready))
        not_existing = sorted(set(sites) - set(SI.all_sites))
        not_matching = sorted((set(sites) - set(not_ready) - set(not_existing)))

        sites = sorted(set(sites) - set(not_ready) - set(not_existing))

        # if any (but not all) of the sites are down
        # enable xrootd and run anyways
        if len(sites) == 0:
            raise Exception("None of the necessary sites are ready")
        elif len(not_ready) > 0: 
            logging.info("Some of the necessary sites are not ready:",  set(not_ready))
            self.options['xrootd'] = True
         else:
            logging.info("All necessary sites are available")

        return sites

    def getACDCsites(self):

        SI = siteInfo()
        original_wf = workflowInfo(self.url, self.schema['OriginalRequestName']) 
            
        where_to_run, missing_to_run, missing_to_run_at =  original_wf.getRecoveryInfo()
        task = self.schema['InitialTaskPath']
        sites = list(set([SI.SE_to_CE(site) for site in where_to_run[task]]) & set(SI.all_sites))

        return sites

    def getSites(self):

        sites = self.getACDCsites()

        # check if all desired sites are up and running
        sites = self.checkSites(sites)

        # provide a list of site names to exclude
        if self.exclude_sites is not None:
            sites = sorted(set(sites) - set(self.excludeSites))

        return sites

    def getACDCParameters(self):

        actions = []
        if self.memory:
            actions.append( 'mem-%s'% self.memory )
        if self.multicore:
            actions.append( 'core-%s'% self.multicore)
        if self.xrootd:
            actions.append( 'xrootd-%s'% self.xrootd)

        return actions

    def makeACDC(self):
        
        actions = self.getACDCParameters()

        # testing
        if self.options['testbed']:
            logging.info(self.taskName)
            logging.info(actions)
            sys.exit("Running with testbed on, quitting.")
            
        acdc = singleRecovery(self.url, self.taskName, self.wfInfo.request, actions, do=True)
        if acdc:
            self.acdcWf = acdc
        else:
            raise Exception("Issue while creating ACDC for "+task)

    def go(self):
        self.makeACDC()
        self.assign()