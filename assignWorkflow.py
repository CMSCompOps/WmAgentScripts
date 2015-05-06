#!/usr/bin/env python
"""
    Quick request assignment, useful if you want to avoid assigning by
    Web interface and reqmgr.py is too unflexible.
    
"""
import urllib2,urllib, httplib, sys, re, os
import optparse
import reqMgrClient as rqMgr

def assignRequest(url, workflow, team, sites, era, procversion, activity, lfn, procstring):
    params = {"action": "Assign",
            "Team"+team: "checked",
            "SiteWhitelist": sites,
            "SiteBlacklist": [],
            "MergedLFNBase": lfn,
            "UnmergedLFNBase": "/store/unmerged",
            "MinMergeSize": 2147483648,
            "MaxMergeSize": 4294967296,
            "MaxMergeEvents": 50000,
            "MaxRSS": 4294967296,
            "MaxVSize": 4294967296,
            "AcquisitionEra": era,
            "Dashboard": activity,
            "ProcessingVersion": procversion,
            "ProcessingString" : procstring, 
            "checkbox"+workflow: "checked"
            }

    encodedParams = urllib.urlencode(params, True)

    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("POST",  "/reqmgr/assign/handleAssignmentPage", encodedParams, headers)
    response = conn.getresponse()
    if response.status != 200:
        print 'could not assign request with following parameters:'
        for item in params.keys():
            print item + ": " + str(params[item])
        print 'Response from http call:'
        print 'Status:',response.status,'Reason:',response.reason
        print 'Explanation:'
        data = response.read()
        print data
        print "Exiting!"
  	sys.exit(1)
    conn.close()
    print 'Assigned workflow:',workflow,'to site:',site,'with processing version',procversion
    return

def main():
    url='cmsweb.cern.ch'	
    parser = optparse.OptionParser()
    parser.add_option('-w', '--workflow', help='Workflow Name',dest='workflow')
    parser.add_option('-t', '--team', help='Type of Requests',dest='team')
    parser.add_option('-s', '--sites', help='Site List, comma separated (no spaces)',dest='sites')
    parser.add_option('-e', '--era', help='Acquistion era',dest='era')
    parser.add_option('-p', '--procversion', help='Processing Version',dest='procversion')
    parser.add_option('-a', '--activity', help='Dashboard Activity',dest='activity')
    parser.add_option('-l', '--lfn', help='Merged LFN base',dest='lfn')
    (options,args) = parser.parse_args()

    if not options.workflow:
	    print "Write a workflow is required"
	    sys.exit(0);
    workflow=options.workflow
    wf = rqMgr.Workflow(workflow)

    if not options.team:
        print "Write a team"
        sys.exit(0);

    #parse site list
    if options.sites:
        sites = [ site for site in options.sites.split(',') ]
    #default site list (all sites), comment out what you want to discard
    else:
        sites = [
            "T1_DE_KIT",
            "T1_ES_PIC",
            "T1_FR_CCIN2P3",
            "T1_IT_CNAF",
            "T1_RU_JINR",
            "T1_UK_RAL",
            "T1_US_FNAL",
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

    #check options that were provided
    if options.era:
        era = options.era
    else:
        era = wf.info['AcquisitionEra']

    if options.procversion:
        procversion = options.procversion
    else:
        procversion = wf.info['ProcessingVersion']
    
    #activity reprocessing by default
    activity='reprocessing'
    if options.activity:
        activity=options.activity
    else:
        activity='reprocessing'
    #lfn backfill by default
    if options.lfn:
        lfn = options.lfn
    elif "MergedLFNBase" in wf.info:
        lfn = wf.info['MergedLFNBase']
    else:
        lfn='/store/backfill/1'
    
    team=options.team
    procstring = wf.info['ProcessingString']

    print "Assigning with"
    print team, site, era, procversion, activity, lfn, procstring

    assignRequest(url, workflow, team, sites, era, procversion, activity, lfn, procstring)
    sys.exit(0);

if __name__ == "__main__":
    main()
