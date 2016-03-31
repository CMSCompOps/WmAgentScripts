#!/usr/bin/env python
"""
    Quick request assignment, useful if you want to avoid assigning by
    Web interface and reqmgr.py is too unflexible.
    
"""
import sys
import optparse
import reqMgrClient as rqMgr
from pprint import pprint
from random import choice

T1_SITES = [
    "T1_DE_KIT",
    "T1_ES_PIC",
    "T1_FR_CCIN2P3",
    "T1_IT_CNAF",
    "T1_RU_JINR",
    "T1_UK_RAL",
    "T1_US_FNAL"
]

T2_SITES = [
    "T2_CH_CERN",
    "T2_DE_DESY",
    "T2_DE_RWTH",
    "T2_ES_CIEMAT",
    "T2_FR_CCIN2P3",
    "T2_FR_IPHC",
    "T2_IT_Bari",
    "T2_IT_Legnaro",
    "T2_IT_Pisa",
    "T2_IT_Rome",
    "T2_UK_London_Brunel",
    "T2_UK_London_IC",
    "T2_US_Caltech",
    "T2_US_Florida",
    "T2_US_MIT",
    "T2_US_Nebraska",
    "T2_US_Purdue",
    "T2_US_UCSD",
    "T2_US_Wisconsin"
]

ALL_SITES = T1_SITES + T2_SITES


def getRandomDiskSite(site=T1_SITES):
    """
        Gets a random disk site and append _Disk
    """
    s = choice(site)
    if s.startswith("T1"):
        s += "_Disk"
    return s


def assignRequest(url, workflow, team, sites, era, procversion, activity, lfn,
                  procstring, trust_site=False, replica=False, verbose=False):
    params = {"action": "Assign",
              "Team" + team: "checked",
              "SiteWhitelist": sites,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 50000,
              "MaxRSS": 4294967296,
              "MaxVSize": 4294967296,
              "SoftTimeout" : 159600,
              "AcquisitionEra": era,
              "Dashboard": activity,
              "ProcessingVersion": procversion,
              "ProcessingString": procstring,
              "checkbox" + workflow: "checked",
              "CustodialSites": []
              }
    # add xrootd (trustSiteList)
    if trust_site:
        params['TrustSitelists'] = True

    # if replica we add NonCustodial sites
    if replica:
        params["NonCustodialSites"] = getRandomDiskSite(),
        params["NonCustodialSubType"] = "Replica"

    if verbose:
        pprint(params)

    params['execute'] = True
    res = rqMgr.assignWorkflow(url, workflow, team, params)
    if res:
        print 'Assigned workflow:', workflow, 'to site:', sites, 'with processing version', procversion
    else:
        print 'could not assign the workflow',workflow


def main():
    url = 'cmsweb.cern.ch'
    url_tb = 'cmsweb-testbed.cern.ch'

    usage = "usage: %prog [options] [WORKFLOW]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-t', '--team', help='Type of Requests', dest='team')
    parser.add_option('-s', '--sites', help='Site List, comma separated (no spaces),\
                        or "t1" for Tier-1\'s and "t2" for Tier-2\'s', dest='sites')
    parser.add_option('-r', '--replica', action='store_true', dest='replica', default=False,
                      help='Adds a _Disk Non-Custodial Replica parameter')
    parser.add_option('-e', '--era', help='Acquistion era', dest='era')
    parser.add_option('-p', '--procversion', help='Processing Version, if empty it will leave the processing version\
                        that comes by defaul in the request', dest='procversion')
    parser.add_option('-a', '--activity', help='Dashboard Activity (reprocessing, production or test), if empty will\
                        set reprocessing as default', dest='activity')
    parser.add_option('-x', '--xrootd', help='Assign with trustSiteLocation=True (allows xrootd capabilities)',
                      action='store_true', default=False, dest='xrootd')
    parser.add_option('-l', '--lfn', help='Merged LFN base', dest='lfn')
    parser.add_option('-f', '--file', help='Text file with a list of wokflows. If this option is used, the same settings will be\
                        applyed to all workflows', dest='file')
    parser.add_option('-v', '--verbose', help='Verbose',
                      action='store_true', default=False, dest='verbose')
    parser.add_option('--testbed', help='Assign in testbed',
                      action='store_true', default=False, dest='testbed')
    (options, args) = parser.parse_args()

    if options.testbed:
        url = url_tb

    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    elif len(args) == 1:
        wfs = [args[0]]
    else:
        parser.error("Provide the workflow name or the file")

    if not options.team:
        parser.error("Provide the TEAM name")
        sys.exit(0)

    # parse site list
    if options.sites:
        if options.sites == "t1":
            sites = T1_SITES
        elif options.sites == "t2":
            sites = T2_SITES
        else:
            sites = [site for site in options.sites.split(',')]
    # default site list (all sites), comment out what you want to discard
    else:
        sites = ALL_SITES

    # activity reprocessing by default
    activity = 'reprocessing'
    if options.activity:
        activity = options.activity
    else:
        activity = 'reprocessing'

    #trustSiteListAsLocation = False
    trust_site = False
    if options.xrootd:
        trust_site = True

    team = options.team

    for wf in wfs:
        wf = rqMgr.Workflow(wf, url=url)
        # check options that were provided particularly
        if options.era:
            era = options.era
        else:
            era = wf.info['AcquisitionEra']
        # lfn backfill by default
        if options.lfn:
            lfn = options.lfn
        elif "MergedLFNBase" in wf.info:
            lfn = wf.info['MergedLFNBase']
        else:
            lfn = '/store/backfill/1'
        # given or default processing version
        if options.procversion:
            procversion = options.procversion
        else:
            procversion = wf.info['ProcessingVersion']
            
        procstring = wf.info['ProcessingString']
        assignRequest(url, wf.name, team, sites, era, procversion,
                      activity, lfn, procstring, trust_site, options.replica, options.verbose)

    sys.exit(0)

if __name__ == "__main__":
    main()
