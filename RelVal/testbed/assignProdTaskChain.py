#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os
import json
import optparse

def assignRequest(url,workflow,team,site,era,procstr,procver,activity,lfn):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 50000,
              #"MaxRSS": 3772000,
              "MaxRSS": 2294967,
              "MaxVSize": 20294967,
              "AcquisitionEra": era,
              "ProcessingString": procstr,
              "ProcessingVersion": procver,
              "Dashboard": activity,
#              "useSiteListAsLocation" : "true",   ### when we want to use xrootd to readin input files
#              "CustodialSites": ['T1_US_FNAL'],
#              "CustodialSubType" : "Move",
#              "NonCustodialSites": ['T2_CH_CERN'],
#              "NonCustodialSubType" : "Replica",
#              "AutoApproveSubscriptionSites": ['T2_CH_CERN'],
#              "SubscriptionPriority": "Normal",
#              "BlockCloseMaxWaitTime" : 3600,
              "BlockCloseMaxWaitTime" : 64800,
              "BlockCloseMaxFiles" : 500,
              "BlockCloseMaxEvents" : 20000000,
              "BlockCloseMaxSize" : 5000000000000,
              "SoftTimeout" : 159600,
              "GracePeriod" : 1000,
              "checkbox"+workflow: "checked"}
    # Once the AcqEra is a dict, I have to make it a json objet 
    jsonEncodedParams = {}
    for paramKey in params.keys():    
        jsonEncodedParams[paramKey] = json.dumps(params[paramKey])

    encodedParams = urllib.urlencode(jsonEncodedParams, False)

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
    print 'Assigned workflow:',workflow,'to site:',site,'and team',team
    return

def getRequestDict(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.loads(r2.read())
    return request


def main():
    url='cmsweb.cern.ch'	
    ### Example: python assignWorkflow.py -w amaltaro_RVZTT_120404_163607_6269 -t testbed-relval -s T1_US_FNAL -e CMSSW_6_0_0_pre1_FS_TEST_WMA -p v1 -a relval -l /store/backfill/1
    parser = optparse.OptionParser()
    parser.add_option('-w', '--workflow', help='Workflow Name',dest='workflow')
    parser.add_option('-t', '--team', help='Type of Requests',dest='team')
    parser.add_option('-s', '--site', help='Site',dest='site')
    parser.add_option('-p', '--procversion', help='Processing Version',dest='procversion')
    parser.add_option('-a', '--activity', help='Dashboard Activity',dest='activity')
    parser.add_option('-l', '--lfn', help='Merged LFN base',dest='lfn')

    parser.add_option('--special', help='Use it for special workflows. You also have to change the code according to the type of WF',dest='special')
    parser.add_option('--test',action="store_true", help='Nothing is injected, only print infomation about workflow and AcqEra',dest='test')
    parser.add_option('--pu',action="store_true", help='Use it to inject PileUp workflows only',dest='pu')
    (options,args) = parser.parse_args()

    if not options.workflow:
        print "The workflow name is mandatory!"
        print "Usage: python assignProdTaskChain.py -w <requestName>"
        sys.exit(0);

    workflow=options.workflow
    team='production'
    site=["T1_DE_KIT","T1_ES_PIC","T1_FR_CCIN2P3","T1_IT_CNAF",
          "T1_RU_JINR","T1_UK_RAL","T1_US_FNAL","T2_CH_CERN",
          "T2_DE_DESY","T2_DE_RWTH","T2_ES_CIEMAT","T2_FR_IPHC",
          "T2_IT_Bari","T2_IT_Legnaro","T2_IT_Pisa","T2_IT_Rome",
          "T2_UK_London_Brunel","T2_UK_London_IC","T2_US_Caltech","T2_US_MIT",
          "T2_US_Nebraska","T2_US_Purdue","T2_US_UCSD","T2_US_Wisconsin","T2_US_Florida"]
    procversion=1
    activity='production'
    lfn='/store/mc'
    acqera = {}
    procstring = {}
    specialStr = ''

    ### Getting the original dictionary
    schema = getRequestDict(url,workflow)

    # Setting the AcqEra and ProcStr values per Task 
    for key, value in schema.items():
        if type(value) is dict and key.startswith("Task"):
            try:
                procstring[value['TaskName']] = value['ProcessingString'].replace("-","_")
                acqera[value['TaskName']] = value['AcquisitionEra']
            except KeyError:
                print "This request has no AcquisitionEra or ProcessingString defined into the Tasks, aborting..."
                sys.exit(1)

    # Adding the special string - in case it was provided in the command line 
    if options.special:
        #specialStr = '_03Jan2013'
        specialStr = '_'+str(options.special)
        for key,value in procstring.items():
            procstring[key] = value+specialStr

    # Handling the parameters given in the command line
    if options.team:
        team=options.team
    if options.site:
        site=options.site
    if options.procversion:
        procversion=int(options.procversion)
    if options.activity:
        activity=options.activity
    if options.lfn:
        lfn=options.lfn

    # If the --test argument was provided, then just print the information gathered so far and abort the assignment
    if options.test:
        print "%s \tAcqEra: %s \tProcStr: %s \tProcVer: %s" % (workflow, acqera, procstring, procversion)
        #print workflow, '\tAcqEra:', acqera, '\tProcStr:', procstring, '\tProcVer:', procversion
        print "LFN: %s \tTeam: %s \tSite: %s" % (lfn, team, site)
        #print '\tTeam:',team,  '\tSite:', site
        sys.exit(0);

    # Really assigning the workflow now
    print workflow, '\tAcqEra:', acqera, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tTeam:',team, '\tSite:', site
    assignRequest(url,workflow,team,site,acqera,procstring,procversion,activity,lfn)
    sys.exit(0);

if __name__ == "__main__":
    main()
