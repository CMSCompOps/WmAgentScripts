#!/usr/bin/python

import re
import sys
import json
import urllib
import classad
import htcondor
from collections import defaultdict

if 'UNIFIED_OVERFLOW_CONFIG' not in htcondor.param:
    sys.exit(0)

SITE_RE = re.compile("T[012]_[A-Z]+_[A-Za-z0-9_]+")
WORKFLOW_RE = re.compile("[-_A-Za-z0-9]+")

config = json.load(urllib.urlopen(htcondor.param['UNIFIED_OVERFLOW_CONFIG']))

reversed_mapping = config['reversed_mapping']

needs_site = defaultdict(set)
for workflow, tasks in config['modifications'].items():
    for taskname,specs in tasks.items():
        anAd = classad.ClassAd()
        anAd["GridResource"] = "condor localhost localhost"
        anAd["TargetUniverse"] = 5
        exp = 'regexp(target.WMAgent_SubTaskName, "%s")'% (taskname)
        anAd["Requirements"] = classad.ExprTree(str(exp))
        
        if "ReplaceSiteWhitelist" in specs:
            anAd["Name"] = str("Site Replacement for %s"% taskname)
            anAd["eval_set_DESIRED_Sites"] = str(",".join(specs['ReplaceSiteWhitelist']))
            print anAd
        elif "AddWhitelist" in specs:
            for site in specs['AddWhitelist']:
                needs_site[site].add(taskname)
 

for site in  needs_site:
    if not site in reversed_mapping: continue
    anAd = classad.ClassAd()
    anAd["GridResource"] = "condor localhost localhost"
    anAd["TargetUniverse"] = 5
    anAd["Name"] = str("Overflow rule for %s"%site)
    anAd["OverflowTasknames"] = map(str, needs_site[site])
    #anAd["OverflowSite"] = str(site)
    exp = classad.ExprTree('regexp("%s", ExtDESIRED_Sites) && member(target.WMAgent_SubTaskName, OverflowTasknames)'%str(site))
    anAd["Requirements"] = classad.ExprTree(str(exp))
    anAd["eval_set_DESIRED_Sites"] = classad.Function("strcat", str(",".join( reversed_mapping[site]+[''] )), classad.Attribute("ExtDESIRED_Sites"))
    print anAd
