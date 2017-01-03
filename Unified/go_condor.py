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

#g_is_cern = socket.getfqdn().endswith("cern.ch")

def makeOverflowAds(config):
    # Mapping from source to a list of destinations.
    reversed_mapping = config['reversed_mapping']

    overflow_tasks = {}
    needs_site = defaultdict(set)
    for workflow, tasks in config['modifications'].items():
        for taskname,specs in tasks.items():
            anAd = classad.ClassAd()
            anAd["GridResource"] = "condor localhost localhost"
            anAd["TargetUniverse"] = 5
            exp = '(HasBeenReplaced isnt true)  && (target.WMAgent_SubTaskName =?= %s)' % classad.quote(str(taskname))
            anAd["Requirements"] = classad.ExprTree(str(exp))
            add_whitelist = specs.get("AddWhitelist")
            if "ReplaceSiteWhitelist" in specs:
                anAd["Name"] = str("Site Replacement for %s"% taskname)
                #if ("T2_CH_CERN_HLT" in specs['ReplaceSiteWhitelist']) and not g_is_cern: specs['ReplaceSiteWhitelist'].remove("T2_CH_CERN_HLT")
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
                for site in add_whitelist:
                    needs_site[site].add(taskname)

    common_mapping = {}
    common_origins = {}
    for site in needs_site:
        if not site in reversed_mapping:
            continue
        site = str(site)
        origins = set(str(origin) for origin in reversed_mapping[site])
        origins.add(site)
        origins = list(origins)
        origins.sort()
        s = hashlib.sha1()
        s.update(str(origins))
        key = s.hexdigest()
        common_origins[key] = origins
        similar_sites = common_mapping.setdefault(key, set())
        similar_sites.add(site)

    source_to_dests = {}
    for dest, sources in reversed_mapping.items():
        for source in sources:
            dests = source_to_dests.setdefault(source, set())
            dests.add(dest)
    tmp_source_to_dests = source_to_dests

    for whitelist_sites, tasks in overflow_tasks.items():
        whitelist_sites_set = set(whitelist_sites.split(","))
        source_to_dests = {}
        for source, dests in tmp_source_to_dests.items():
            new_dests = [str(i) for i in dests if i in whitelist_sites_set]
            if new_dests:
                source_to_dests[str(source)] = new_dests

        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = "Master overflow rule for %s" % str(whitelist_sites)
        anAd["OverflowTasknames"] = map(str, tasks)
        overflow_names_escaped = anAd.lookup('OverflowTasknames').__repr__()
        del anAd['OverflowTaskNames']
        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (HasBeenRouted_Overflow isnt true)' % overflow_names_escaped)
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd["eval_set_DESIRED_Sites"] = classad.ExprTree('ifThenElse(siteMapping("", []) isnt error, siteMapping(DESIRED_CMSDataLocations, %s), DESIRED_CMSDataLocations)' % str(classad.ClassAd(source_to_dests)))
        anAd['set_Rank'] = classad.ExprTree("stringlistmember(GLIDEIN_CMSSite, ExtDESIRED_Sites)")
        anAd['set_HasBeenRouted'] = False
        anAd['set_HasBeenRouted_Overflow'] = True
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
    for memory in configs.get('memory',[]):
        wfs = configs['memory'][memory]
        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = str("Set memory requirement to %s"% memory)
        anAd["MemoryTasknames"] = map(str, wfs)
        memory_names_escaped = anAd.lookup('MemoryTasknames').__repr__()
        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (target.HasBeenMemoryTuned =!= true) && (target.RequestMemory >= %d)' %( memory_names_escaped, int(memory) ))
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd['set_HasBeenMemoryTuned'] = True
        anAd['set_HasBeenRouted'] = False
        anAd['set_RequestMemory'] = int(memory)
        print anAd

    for timing in configs.get('time',[]):
        wfs = configs['time'][timing]
        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = str("Set timing requirement to %s"% timing)
        anAd["TimeTasknames"] = map(str, wfs)
        time_names_escaped = anAd.lookup('TimeTasknames').__repr__()
        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && (target.HasBeenTimingTuned =!= true) && (target.MaxWallTimeMins <= %d)' %( time_names_escaped, int(timing) ))
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd['set_HasBeenTimingTuned'] = True
        anAd['set_HasBeenRouted'] = False
        anAd['set_MaxWallTimeMins'] = int(timing)
        print anAd
        
def makeAds(config):
    makeOverflowAds(config)
    makeSortAds()
    makePrioCorrectionsAds()
    makePerformanceCorrectionsAds(config)    

if __name__ == "__main__":

    if 'UNIFIED_OVERFLOW_CONFIG' not in htcondor.param:
        sys.exit(0)

    config = json.load(urllib.urlopen(htcondor.param['UNIFIED_OVERFLOW_CONFIG']))
    makeAds(config)
