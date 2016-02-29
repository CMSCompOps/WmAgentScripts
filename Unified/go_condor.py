#!/usr/bin/python

import re
import sys
import json
import socket
import urllib
import classad
import htcondor
from collections import defaultdict

#g_is_cern = socket.getfqdn().endswith("cern.ch")

def makeAds(config):
    reversed_mapping = config['reversed_mapping']

    needs_site = defaultdict(set)
    for workflow, tasks in config['modifications'].items():
        for taskname,specs in tasks.items():
            anAd = classad.ClassAd()
            anAd["GridResource"] = "condor localhost localhost"
            anAd["TargetUniverse"] = 5
            exp = '(HasBeenReplaced isnt true)  && (target.WMAgent_SubTaskName =?= %s)' % classad.quote(str(taskname))
            anAd["Requirements"] = classad.ExprTree(str(exp))
            
            if "ReplaceSiteWhitelist" in specs:
                anAd["Name"] = str("Site Replacement for %s"% taskname)
                #if ("T2_CH_CERN_HLT" in specs['ReplaceSiteWhitelist']) and not g_is_cern: specs['ReplaceSiteWhitelist'].remove("T2_CH_CERN_HLT")
                anAd["eval_set_DESIRED_Sites"] = str(",".join(specs['ReplaceSiteWhitelist']))
                anAd['set_Rank'] = classad.ExprTree("stringlistmember(GLIDEIN_CMSSite, ExtDESIRED_Sites)")
                anAd["set_HasBeenReplaced"] = True
                anAd["set_HasBeenRouted"] = False
                print anAd
            elif "AddWhitelist" in specs:
                for site in specs['AddWhitelist']:
                    needs_site[site].add(taskname)
 

    for site in  needs_site:
        if not site in reversed_mapping: continue
        #if site == "T2_CH_CERN_HLT" and not g_is_cern: continue
        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = str("Overflow rule to go to %s"%site)
        anAd["OverflowTasknames"] = map(str, needs_site[site])
        overflow_names_escaped = anAd.lookup('OverflowTasknames').__repr__()
        del anAd['OverflowTaskNames']
        exprs = ['regexp(%s, target.ExtDESIRED_Sites)'% classad.quote(str(origin)) for origin in reversed_mapping[site]]
        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && ( %s ) && (target.HasBeenRouted_%s =!= true)' % (overflow_names_escaped, str("||".join( exprs )), str(site)))
        anAd["Requirements"] = classad.ExprTree(str(exp))
        anAd["copy_DESIRED_Sites"] = "Prev_DESIRED_Sites"
        anAd["eval_set_DESIRED_Sites"] = classad.ExprTree('ifThenElse(sortStringSet("") isnt error, sortStringSet(strcat(%s, ",", Prev_DESIRED_Sites)), strcat(%s, ",", Prev_DESIRED_Sites))' % (classad.quote(str(site)), classad.quote(str(site))))
        anAd['set_Rank'] = classad.ExprTree("stringlistmember(GLIDEIN_CMSSite, ExtDESIRED_Sites)")
        anAd['set_HasBeenRouted'] = False
        anAd['set_HasBeenRouted_%s' % str(site)] = True
        print anAd


def makeSortAd():
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


def makePrioCorrections():
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
    anAd["MaxJobs"] = 50
    print anAd


if __name__ == "__main__":

    if 'UNIFIED_OVERFLOW_CONFIG' not in htcondor.param:
        sys.exit(0)

    config = json.load(urllib.urlopen(htcondor.param['UNIFIED_OVERFLOW_CONFIG']))
    makeAds(config)
    makeSortAd()
    makePrioCorrections()
