#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse
import findCustodialLocation
from calendar import month_abbr

def assignRequest(url,workflow,team,site,era,procversion,activity,lfn):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 100000,
              "MaxRSS": 4294967296,
              "MaxVSize": 4294967296,
              "AcquisitionEra": era,
              "Dashboard": activity,
              "ProcessingVersion": procversion,
              "SoftTimeout" : 167000,
              "GracePeriod" : 1000,
              "checkbox"+workflow: "checked"}
    # Once the AcqEra is a dict, I have to make it a json objet 
    jsonEncodedParams = {}
    for paramKey in params.keys():
        jsonEncodedParams[paramKey] = json.write(params[paramKey])

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
    print 'Assigned workflow:',workflow,'to site:',site,'with processing version',procversion
    return

def getRequestDict(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.read(r2.read())
    return request

def main():
    url='cmsweb.cern.ch'	
    parser = optparse.OptionParser()
    parser.add_option('-w', '--workflow', help='Workflow Name',dest='workflow')
    parser.add_option('-v', '--version', help='Dataset version that is appended to the processing version (default is v1)',dest='version')
    parser.add_option('--test',action="store_true", help='Nothing is injected, it only prints infomation about the request to be injected',dest='test')
    (options,args) = parser.parse_args()

    if not options.workflow:
        print "The workflow name is mandatory!!!"
        print "Usage: python assignRereco_HLT_PR.py [--test] -w <workflowName> -v <integer_versionNumber>"
        print "Example: python assignRereco_HLT_PR.py -w smalik_HLT_RelVal_208686_130116_062129_6329 -v 3"
        sys.exit(0);
    else:
        workflow=options.workflow

    team='relval'             # for cmssrv113 - production of the highest priority
    activity='reprocessing'
    lfn='/store/data'

    try:
        aux=workflow.split('RelVal', 3)[1]
    except:
        print "NOPPSSS!! It's not a RelVal request. Aborting the program ..."
        sys.exit(0)
    if not options.version:
        version='-v1'
    else:
        version="-v"+str(options.version)

    #### Converting the date based on the request name #### 
    auxdate=aux.split('_')[-3]
    year = auxdate[:2]
    month = auxdate[2:4]
    day = auxdate[4:]
    date = day+month_abbr[int(month)]+"20"+year

    ### Getting the original dictionary
    schema = getRequestDict(url,workflow)

    if schema['RequestType'] == 'TaskChain':
        run = str(schema['Task1']['RunWhitelist'][0])
        pVer1 = date+'_HLT_R'+run+version
        pVer2 = date+'_HLTReference_R'+run+version
        pVer3 = date+'_HLTNewconditions_R'+run+version

        procversion = {'Task1' : pVer1, 'Task2' : pVer2, 'Task3' : pVer3}

        #### Getting the custodial site and AcqEra ####
        dataset = schema['Task1']['InputDataset']
        aux = dataset.split('/',3)[2]
        acqera = aux.split('-',2)[0]
        if acqera == "HIRun2013A":
            site=findCustodialLocation.findCustodialLocation(url,dataset).split('_MSS', 2)[0]
            pass
        elif acqera == "HIRun2013":
            site = 'T2_CH_CERN'
            team='relvallsf'        # vocms174 - LSF agent
            lfn='/store/hidata'

    elif schema['RequestType'] == 'ReReco':
        run = str(schema['RunWhitelist'][0])
        if '_PR_reference' in workflow:
            procversion=date+'_PRReference_R'+run+version
        elif '_PR_newconditions' in workflow:
            procversion=date+'_PRNewconditions_R'+run+version
        elif 'HLT_reference' in workflow:
            procversion=date+'_HLTReference_R'+run+version
        elif 'HLT_newconditions' in workflow:
            procversion=date+'_HLTNewconditions_R'+run+version
        elif 'HcalQpll_def' in workflow:
            procversion=date+'_HcalQpll_def_R'+run+version
        elif 'HcalQpll_mod' in workflow:
            procversion=date+'_HcalQpll_mod_R'+run+version
        else:
            print "There is a problem: this is NOT a PR or HLT request!!!"
            sys.exit(0);
        #### Getting the custodial site ####
        dataset = schema['InputDataset']
        aux = dataset.split('/',3)[2]
        acqera = aux.split('-',2)[0]
        if acqera == "HIRun2013A":
            site=findCustodialLocation.findCustodialLocation(url,dataset).split('_MSS', 2)[0]
            pass
        elif acqera == "HIRun2013":
            site = 'T2_CH_CERN'
            team='relvallsf'        # vocms174 - LSF agent
            lfn='/store/hidata'

    else:
        print "Sorry, I don't know what to do with "+schema['RequestType']+" request type"

    if site is None:
        print "There is no custodial place for this dataset. Please do something about it. Quitting..."
        sys.exit(0);
    if schema['RequestType'] == 'TaskChain':
        print workflow+"\tAcqEra: "+acqera+"\tTeam: "+team+"\tSite: "+site+"\tLFN: "+lfn
        print "ProcVer: " ,procversion
    else:
        print workflow+"\tAcqEra: "+acqera+"\tProcVer: "+procversion+"\tTeam: "+team+"\tSite: "+site+"\tLFN: "+lfn

    if options.test:
        print "Dataset: "+dataset+" Custodial at "+site
        sys.exit(0);

    assignRequest(url,workflow,team,site,acqera,procversion,activity,lfn)
    sys.exit(0);

if __name__ == "__main__":
    main()
