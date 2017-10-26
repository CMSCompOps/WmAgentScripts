#!/usr/bin/python

import re
import sys
import json
import socket
import urllib
import classad
import hashlib
import htcondor
from collections import defaultdict

def makeHighPrioAds(config):
    to_be_raised = config.get('highprio',[])
    if to_be_raised:
        anAd = classad.ClassAd()
        anAd["Name"] = str("Raising to highprio group")
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["RaisedTaskNames"] = map(str,to_be_raised)
        wfs_escaped = anAd.lookup('RaisedTaskNames').__repr__()
        del anAd["RaisedTaskNames"]
        exp = '(HasBeenRaisedHighPrio isnt true)  && member(target.WMAgent_RequestName, %s)' % wfs_escaped
        anAd["Requirements"] = classad.ExprTree(str(exp))        
        anAd["set_AccountingGroup"] = "highprio.cmsdataops"
        anAd["set_HasBeenRaisedHighPrio"] = True
        anAd["set_HasBeenRouted"] = False
        print anAd

def makeHoldAds(config):
    """
    Create a set of rules to hold a task from matching
    """
    for task,where in config.get('hold',{}).items():
        # task is the task name
        # where is either an empty list=all sites, or a list of sites (not implemented)
        anAd = classad.ClassAd()
        anAd["Name"] = str("Holding task %s from %s"%(task, where))
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        exp = '(HasBeenSetHeld isnt true)  && (target.WMAgent_SubTaskName =?= %s)' % classad.quote(str(task))
        anAd["Requirements"] = classad.ExprTree(str(exp))
        ## we use the site whitelist to prevent matching
        anAd["copy_DESIRED_Sites"] = "Held_DESIRED_Sites"
        anAd["set_DESIRED_Sites"] = "T2_NW_NOWHERE"
        anAd["set_HasBeenRouted"] = False
        anAd["set_HasBeenSetHeld"] = True
        print anAd

def makeReleaseAds(config):
    """
    Create a set of rules to release a task to match
    """
    for task,where in config.get('release',{}).items():
        anAd = classad.ClassAd()
        anAd["Name"] = str("Releasing task %s"%(task))
        anAd["GridResource"] = "condor localhost localhost"
        exp = '(HasBeenSetHeld is true) && (target.WMAgent_SubTaskName =?= %s)' % classad.quote(str(task))
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd["copy_Held_DESIRED_Sites"] = "DESIRED_Sites"
        anAd["set_HasBeenRouted"] = False
        anAd["set_HasBeenSetHeld"] = False
        print anAd

def makeReadAds(config):
    for needs, tasks in config.get('read',{}).items():
        anAd = classad.ClassAd()
        set_read = int(float(needs))
        anAd["Name"] = str("Set read requirement to %s"% set_read)
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["JobRouterTasknames"] = map(str, tasks)
        task_names_escaped = anAd.lookup('JobRouterTasknames').__repr__()
        del anAd["JobRouterTasknames"]

        #exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (HasBeenReadTuned isnt true)' % task_names_escaped)
        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (EstimatedInputRateKBs =!= %d)' %( task_names_escaped,int(set_read))) ## just set to a different value
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd["set_HasBeenRouted"] = False
        anAd["set_HasBeenReadTuned"] = True
        anAd["set_EstimatedInputRateKBs"] = int(set_read)
        print anAd

def makeOverflowAds(config):
    # Mapping from source to a list of destinations.
    reversed_mapping = config['reversed_mapping']

    overflow_tasks = {}
    for workflow, tasks in config.get('modifications',{}).items():
        for taskname,specs in tasks.items():
            anAd = classad.ClassAd()
            anAd["GridResource"] = "condor localhost localhost"
            anAd["TargetUniverse"] = 5
            exp = '(HasBeenReplaced isnt true)  && (target.WMAgent_SubTaskName =?= %s)' % classad.quote(str(taskname))
            anAd["Requirements"] = classad.ExprTree(str(exp))
            add_whitelist = specs.get("AddWhitelist")
            if "ReplaceSiteWhitelist" in specs:
                anAd["Name"] = str("Site Replacement for %s"% taskname)
                anAd["eval_set_DESIRED_Sites"] = str(",".join(specs['ReplaceSiteWhitelist']))
                anAd['set_Rank'] = classad.ExprTree("stringlistmember(GLIDEIN_CMSSite, ExtDESIRED_Sites)")
                anAd["set_HasBeenReplaced"] = True
                anAd["set_HasBeenRouted"] = False
                print anAd
            elif add_whitelist:
                add_whitelist.sort()
                add_whitelist_key = ",".join(add_whitelist)
                tasks = overflow_tasks.setdefault(add_whitelist_key, [])
                tasks.append(taskname)

    # Create a source->dests mapping from the provided reverse_mapping.
    source_to_dests = {}
    for dest, sources in reversed_mapping.items():
        for source in sources:
            dests = source_to_dests.setdefault(source, set())
            dests.add(dest)
    tmp_source_to_dests = source_to_dests

    # For each unique set of site whitelists, create a new rule.  Each task
    # should appear on just one of these ads, meaning it should only get routed
    # once.
    for whitelist_sites, tasks in overflow_tasks.items():
        whitelist_sites_set = set(whitelist_sites.split(","))

        # Create an updated source_to_dests, where the dests are filtered
        # on the whitelist.
        source_to_dests = {}
        for source, dests in tmp_source_to_dests.items():
            new_dests = [str(i) for i in dests if i in whitelist_sites_set]
            if new_dests:
                source_to_dests[str(source)] = new_dests

        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = "Master overflow rule for %s" % str(whitelist_sites)

        # ClassAds trick to create a properly-formatted ClassAd list.
        anAd["OverflowTasknames"] = map(str, tasks)
        overflow_names_escaped = anAd.lookup('OverflowTasknames').__repr__()
        del anAd['OverflowTaskNames']

        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (HasBeenRouted_Overflow isnt true)' % overflow_names_escaped)
        anAd["Requirements"] = classad.ExprTree(str(exp))
        # siteMapping will apply the source->dest rules, given the current set of sources in ExtDESIRED_Sites.
        anAd["eval_set_DESIRED_Sites"] = classad.ExprTree('ifThenElse(siteMapping("", []) isnt error, siteMapping(ExtDESIRED_Sites, %s), ExtDESIRED_Sites)' % str(classad.ClassAd(source_to_dests)))

        # Where possible, prefer to run at a site where the input can be read locally.
        anAd['set_Rank'] = classad.ExprTree("stringlistmember(GLIDEIN_CMSSite, ExtDESIRED_Sites)")
        anAd['set_HasBeenRouted'] = False
        anAd['set_HasBeenRouted_Overflow'] = True
        print anAd


def makeResizeAds(config):
    policies = {}
    for workflow, info in config.get('resizing', {}).items():
        minCores = info.get("minCores", 3)
        maxCores = info.get("maxCores", 8)
        memoryPerThread = info.get("memoryPerThread")
        workflows = policies.setdefault((minCores, maxCores, memoryPerThread), set())
        workflows.add(workflow)
    for policy, workflows in policies.items():
        minCores, maxCores, memoryPerThread = policy
        anAd = classad.ClassAd()
        anAd['GridResource'] = 'condor localhost localhost'
        anAd['TargetUniverse'] = 5

        # Same trick as above to convert the set to a ClassAd list.
        anAd["OverflowTasknames"] = map(str, workflows)
        tasks_escaped = anAd.lookup('OverflowTasknames').__repr__()
        del anAd['OverflowTaskNames']

        anAd['Name'] = 'Resize Jobs (%d-%d cores, %d MB/thread)' % (minCores, maxCores, memoryPerThread)
        anAd['Requirements'] = classad.ExprTree('(target.WMCore_ResizeJob is False) && member(target.WMAgent_SubTaskName, %s)' % tasks_escaped)
        anAd['set_WMCore_ResizeJob'] = True
        anAd['set_MinCores'] = minCores
        anAd['set_MaxCores'] = maxCores
        anAd['set_HasBeenRouted'] = False
        anAd['set_ExtraMemory'] = memoryPerThread
        #anAd['set_RequestMemory'] = classad.ExprTree('OriginalMemory + %d * ( WMCore_ResizeJob ? ( RequestCpus - OriginalCpus ) : 0 )' % memoryPerThread)
        print anAd


def makeSortAds():
    anAd = classad.ClassAd()
    anAd["GridResource"] = "condor localhost localhost"
    anAd["TargetUniverse"] = 5
    anAd["Name"] = "Sort Ads"
    anAd["Requirements"] = classad.ExprTree("(sortStringSet(\"\") isnt error) && (target.HasBeenRouted is false) && (target.HasBeenSorted isnt true)")
    anAd["copy_DESIRED_Sites"] = "Prev_DESIRED_Sites"
    anAd["eval_set_DESIRED_Sites"] = classad.ExprTree("debug(sortStringSet(Prev_DESIRED_Sites))")
    anAd["set_HasBeenSorted"] = True
    anAd['set_HasBeenRouted'] = False
    #print anAd


def makePrioCorrectionsAds():
    """
    Optimize the PostJobPrio* entries for HTCondor matchmaking.

    This will sort jobs within the schedd along the following criteria (higher is better):
    1) Workflow ID (lower is better).
    2) Step in workflow (later is better)
    3) # of sites in whitelist (lower is better).
    4) Estimated job runtime (lower is better).
    5) Estimated job disk requirements (lower is better).
    """
    anAd = classad.ClassAd()
    anAd["GridResource"] = "condor localhost localhost"
    anAd["TargetUniverse"] = 5
    anAd["Name"] = "Prio Corrections"
    anAd["Requirements"] = classad.ExprTree("(target.HasPrioCorrection isnt true)")
    anAd["set_HasPrioCorrection"] = True
    anAd["set_HasBeenRouted"] = False
    # -1 * Number of sites in workflow.
    anAd["copy_PostJobPrio1"] = "WMAgent_PostJobPrio1"
    # -1 * Workflow ID (newer workflows have higher numbers)
    anAd["copy_PostJobPrio2"] = "WMAgent_PostJobPrio2"
    anAd["eval_set_JR_PostJobPrio1"] = classad.ExprTree("WMAgent_PostJobPrio2*100*1000 + size(WMAgent_SubTaskName)*100 + WMAgent_PostJobPrio1")
    anAd["eval_set_JR_PostJobPrio2"] = classad.ExprTree("-MaxWallTimeMins - RequestDisk/1000000")
    anAd["set_PostJobPrio1"] = classad.Attribute("JR_PostJobPrio1")
    anAd["set_PostJobPrio2"] = classad.Attribute("JR_PostJobPrio2")
    print anAd

def makePerformanceCorrectionsAds(configs):
    m_config = configs.get('memory',{})
    for memory in m_config:
        wfs = m_config[memory]
        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = str("Set memory requirement to %s"% memory)
        anAd["MemoryTasknames"] = map(str, wfs)
        memory_names_escaped = anAd.lookup('MemoryTasknames').__repr__()
        #exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (target.HasBeenMemoryTuned =!= true) && (target.OriginalMemory >= %d)' %( memory_names_escaped, int(memory) ))
        #exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (target.HasBeenMemoryTuned =!= true) && (target.OriginalMemory =!= %d)' %( memory_names_escaped, int(memory) ))
        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && ((target.HasBeenMemoryTuned =!= true) || (target.OriginalMemory =!= %d))' %( memory_names_escaped, int(memory) )) ## just set to a different value
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd['set_HasBeenMemoryTuned'] = True
        anAd['set_HasBeenRouted'] = False
        anAd['set_OriginalMemory'] = int(memory)
        print anAd

    t_config = configs.get('time',{})
    for timing in t_config:
        wfs = t_config[timing]
        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = str("Set timing requirement to %s"% timing)
        anAd["TimeTasknames"] = map(str, wfs)
        time_names_escaped = anAd.lookup('TimeTasknames').__repr__()
        #exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (target.HasBeenTimingTuned =!= true) && (target.EstimatedSingleCoreMins <= %d)' %( time_names_escaped, int(timing) ))
        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && ((target.HasBeenTimingTuned =!= true) || (target.EstimatedSingleCoreMins <= %d))' %( time_names_escaped, int(timing) ))
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd['set_HasBeenTimingTuned'] = True
        anAd['set_HasBeenRouted'] = False
        #anAd['set_OriginalMaxWallTimeMins'] = int(timing)
        anAd['set_EstimatedSingleCoreMins'] = int(timing)
        anAd['set_OriginalMaxWallTimeMins'] = classad.ExprTree('EstimatedSingleCoreMins / OriginalCpus')
        print anAd

    s_config = configs.get('slope',{})
    for slope in s_config:
        wfs = s_config[slope]
        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = str("Set memory per thread requirement to %s"% slope)
        anAd["TimeTasknames"] = map(str, wfs)
        time_names_escaped = anAd.lookup('TimeTasknames').__repr__()
        #exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (target.HasBeenSlopeTuned =!= true)' %( time_names_escaped ))
        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (target.ExtraMemory =!= %d)' %( time_names_escaped , int(slope))) ## just set to a different value
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd['set_HasBeenSlopeTuned'] = True 
        anAd['set_HasBeenRouted'] = False
        anAd['set_ExtraMemory'] = int(slope)
        print anAd

def makeDrainAds():
    anAd = classad.ClassAd()                                                                                                                                                                       
    anAd["GridResource"] = "condor localhost localhost"
    anAd["TargetUniverse"] = 5                                                                                                                                                                     
    draining_agents = []#"vocms0303"]
    for agent in draining_agents:
        anAd["Name"] = str("Drain agent %s"%agent)
        exp = classad.ExprTree('regexp("%s",GlobalJobId) && JobStatus == 1'% agent)
        anAd["Requirements"] = classad.ExprTree(str(exp))
        ##regexp("vocms0303",GlobalJobId) && JobStatus == 1' -af GlobalJobId JobPrio | grep -v 303
        anAd["set_JobPrio"] = 500000
        anAd["set_HasBeenRouted"] = False
        print anAd

def makeAdhocAds():
    anAd = classad.ClassAd()
    anAd["GridResource"] = "condor localhost localhost"
    anAd["TargetUniverse"] = 5
    anAd["Name"] = str("Correcting memory requirement of 22092")
    anAd["Requirements"] = classad.ExprTree("OriginalMemory =?= 22092")
    anAd["set_OriginalMemory"] = 18000
    anAd["set_HasBeenRouted"] = False
    print anAd

    anAd = classad.ClassAd()
    anAd["GridResource"] = "condor localhost localhost"
    anAd["TargetUniverse"] = 5
    anAd["Name"] = str("Correcting memory requirement of 23260")
    anAd["Requirements"] = classad.ExprTree("OriginalMemory =?= 23260")
    anAd["set_OriginalMemory"] = 19500
    anAd["set_HasBeenRouted"] = False
    print anAd

    ############################################################
    ## if you want to reset the routing of eveything in the pool
    reset_routing = []#'HasBeenRouted','HasBeenRouted_Overflow','HasBeenMemoryTuned','HasBeenSlopeTuned', 'HasBeenTimingTuned','WMCore_ResizeJob','HasBeenReplaced','HasBeenReadTuned','HasBeenRaisedHighPrio']
    for which_route in reset_routing:
        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = str("Reset routing for %s"% which_route)
        exp = classad.ExprTree("%s is true"% which_route)
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd["set_HasBeenRouted"] = False
        anAd["set_%s"% which_route] = False
        print anAd




def makeAds(config):
    makeOverflowAds(config)
    makeSortAds()
    makePrioCorrectionsAds()
    makePerformanceCorrectionsAds(config)    
    makeResizeAds(config)
    makeReadAds(config)
    makeHoldAds(config)
    makeReleaseAds(config)
    makeHighPrioAds(config)
    makeAdhocAds()
    makeDrainAds()

if __name__ == "__main__":

    if 'UNIFIED_OVERFLOW_CONFIG' not in htcondor.param:
        sys.exit(0)

    config = json.load(urllib.urlopen(htcondor.param['UNIFIED_OVERFLOW_CONFIG']))
    makeAds(config)
