#!/usr/bin/python

import re
import sys
import json
import urllib
import classad
import htcondor
from collections import defaultdict

def makeAds( config ):
    reversed_mapping = config['reversed_mapping']

    needs_site = defaultdict(set)
    for workflow, tasks in config['modifications'].items():
        for taskname,specs in tasks.items():
            anAd = classad.ClassAd()
            anAd["GridResource"] = "condor localhost localhost"
            anAd["TargetUniverse"] = 5
            exp = 'regexp(target.WMAgent_SubTaskName, %s)'% classad.quote(str(taskname))
            anAd["Requirements"] = classad.ExprTree(str(exp))
            
            if "ReplaceSiteWhitelist" in specs:
                anAd["Name"] = str("Site Replacement for %s"% taskname)
                anAd["eval_set_DESIRED_Sites"] = str(",".join(specs['ReplaceSiteWhitelist']))
                anAd['set_Rank'] = classad.ExprTree("stringlistmember(GLIDEIN_CMSSite, ExtDESIRED_Sites)")
                print anAd
            elif "AddWhitelist" in specs:
                for site in specs['AddWhitelist']:
                    needs_site[site].add(taskname)
 

    for site in  needs_site:
        if not site in reversed_mapping: continue
        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        anAd["Name"] = str("Overflow rule to go to %s"%site)
        anAd["OverflowTasknames"] = map(str, needs_site[site])
        overflow_names_escaped = anAd.lookup('OverflowTasknames').__repr__()
        #exp = classad.ExprTree('regexp(%s, ExtDESIRED_Sites) && member(target.WMAgent_SubTaskName, OverflowTasknames)' % classad.quote(str(site)))
        exprs = ['regexp(%s, target.ExtDESIRED_Sites)'% classad.quote(str(origin)) for origin in reversed_mapping[site]]
        exp = classad.ExprTree('member(target.WMAgent_SubTaskName, %s) && ( %s )' % (overflow_names_escaped, str("||".join( exprs ))))
        anAd["Requirements"] = classad.ExprTree(str(exp))
        #anAd["eval_set_DESIRED_Sites"] = classad.Function("strcat", str(",".join( reversed_mapping[site]+[''] )), classad.Attribute("ExtDESIRED_Sites"))
        anAd["eval_set_DESIRED_Sites"] = classad.Function("strcat", str(site), classad.Attribute("ExtDESIRED_Sites"))
        anAd['set_Rank'] = classad.ExprTree("stringlistmember(GLIDEIN_CMSSite, ExtDESIRED_Sites)")
        print anAd

if __name__ == "__main__":

    if 'UNIFIED_OVERFLOW_CONFIG' not in htcondor.param:
        sys.exit(0)

    config = json.load(urllib.urlopen(htcondor.param['UNIFIED_OVERFLOW_CONFIG']))
    makeAds(config)
