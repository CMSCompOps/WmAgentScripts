#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os
import json
import optparse
#from das_client import get_data
das_host='https://cmsweb.cern.ch'


def checkDatasetExistenceDAS(dataset):
    query="dataset dataset="+dataset+" status=*"
    das_data = get_data(das_host,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    if len(result['data'])==0:
	return False
    else:
	return True


def assignRequest(url,workflow,team,site,era,procstr,procver,activity,lfn):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 100000,
              #"MaxRSS": 2411724,
              "MaxRSS": 3072000,
              "MaxVSize": 20294967,
              "AcquisitionEra": era,
              "ProcessingString": procstr,
              "ProcessingVersion": procver,
              "Dashboard": activity,
              "useSiteListAsLocation" : "true",   ### when we want to use xrootd to readin input files
      #        "CustodialSites": ['T1_US_FNAL'],
      #        "NonCustodialSites": ['T2_CH_CERN'],
      #        "AutoApproveSubscriptionSites": ['T1_US_FNAL'],
      #        "SubscriptionPriority": "Medium",
      #        "CustodialSubType" : "Replica",
              "BlockCloseMaxWaitTime" : 28800,
              "BlockCloseMaxFiles" : 500,
              "BlockCloseMaxEvents" : 20000000,
              "BlockCloseMaxSize" : 5000000000000,
              "SoftTimeout" : 129600,
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
    parser.add_option('--correct_env',action="store_true",dest='correct_env')
    parser.add_option('--special', help='Use it for special workflows. You also have to change the code according to the type of WF',dest='special')
    parser.add_option('--test',action="store_true", help='Nothing is injected, only print infomation about workflow and AcqEra',dest='test')
    parser.add_option('--pu',action="store_true", help='Use it to inject PileUp workflows only',dest='pu')
    parser.add_option('--lsf',action="store_true", help='Use it to assign work to the LSF agent at CERN - vocms174, relvallsf team',dest='lsf')
    (options,args) = parser.parse_args()

    command=""
    for arg in sys.argv:
        command=command+arg+" "

    if not options.correct_env:
        os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
        sys.exit(0)

    data = False
    fastsim = False

    if not options.workflow:
        print "The workflow name is mandatory!"
        print "Usage: python assignRelValWorkflow.py -w <requestName>"
        sys.exit(0);
    workflow=options.workflow
    team='relval'
    site='T1_US_FNAL'
    procversion=1
    #procversion='v1'
    activity='relval'
    lfn='/store/relval'
    procstring = {}
    specialStr = ''

    ### Getting the original dictionary
    schema = getRequestDict(url,workflow)

    ### Dropping 2010 HeavyIon workflows or assigning 2011 to CERN/LSF 
    if 'RunHI2010' in schema['RequestString']:
        print workflow, '\t\tHEAVY ION CANNOT RUN AT FNAL --> REQUEST DROPPED!'
        sys.exit(0);
    elif 'RunHI2011' in schema['RequestString']:
        site='T2_CH_CERN'

    # If it's a MC or FS workflow, then assign it to T2_CH_CERN 
    if 'START' in schema['GlobalTag'] or 'POSTLS' in schema['GlobalTag']:
        site='T2_CH_CERN'
    elif 'PRE_SH' in schema['GlobalTag'] or 'PRE_ST' in schema['GlobalTag'] or 'RD53' in schema['GlobalTag']:
        site='T2_CH_CERN'

    # Setting the AcquisitionEra parameter - it will be always the same for all tasks inside the request
    acqera = schema['CMSSWVersion']

    # Setting the ProcessingString values per Task 
    for key, value in schema.items():
        if type(value) is dict and key.startswith("Task"):
            try:
                if 'ProcessingString' in value:
                    procstring[value['TaskName']] = value['ProcessingString'].replace("-","_")
                elif 'AcquisitionEra' in value and '-' in value['AcquisitionEra']:
                    #procstring[value['TaskName']] = value['AcquisitionEra'].split('-')[-1]
                    procstring[value['TaskName']] = value['AcquisitionEra'].split(schema['CMSSWVersion']+'-')[-1]
                    procstring[value['TaskName']] = procstring[value['TaskName']].replace("-","_")
                    #procstring[value['TaskName']] = 'START61_V8'
                else:
                    good=bad
            except KeyError:
                print "This request has no ProcessingString defined into the Tasks, aborting..."
                sys.exit(1)

    # Adding the "PU_" string into the ProcessingString value
    if options.pu:
        for key,value in procstring.items():
            if schema['Task1']['TaskName'] == 'ZmumuJets_Pt_20_300':
                continue
            if 'PU' not in value:
                procstring[key] = 'PU_'+value


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

    # Changing the team name and the site whitelist in case the --lsf parameter was given   
    if options.lsf:
        team='relvallsf'
        site='T2_CH_CERN'

#    site=['T2_US_Nebraska','T2_US_Florida']

    # If the --test argument was provided, then just print the information gathered so far and abort the assignment
    if options.test:
        print workflow, '\tAcqEra:', acqera, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tTeam:',team, '\tSite:', site
        sys.exit(0);

    # Really assigning the workflow now
    print workflow, '\tAcqEra:', acqera, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tTeam:',team, '\tSite:', site
    assignRequest(url,workflow,team,site,acqera,procstring,procversion,activity,lfn)
    sys.exit(0);

if __name__ == "__main__":
    main()
