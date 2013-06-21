#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os
import json_Local as json_Local
import optparse

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
              "maxRSS": 4294967296,
              "maxVSize": 4294967296,
              "AcquisitionEra": era,
	          "dashboard": activity,
              "ProcessingVersion": procversion,
              "SoftTimeout" : 129600,
              "GracePeriod" : 1000,
              "checkbox"+workflow: "checked"}
#    print params
#    sys.exit(0);
    # Once the AcqEra is a dict, I have to make it a json objet 
    jsonEncodedParams = {}
    for paramKey in params.keys():    
        jsonEncodedParams[paramKey] = json_Local.write(params[paramKey])

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
    request = json_Local.read(r2.read())
    return request

def updateAcqEra(workflow, myAcqEra, typeWF):
    for key,value in myAcqEra.items():
        if typeWF == 'data':
            label = checkLabel(workflow)
            myAcqEra[key] = value+'_RelVal_'+label
        elif typeWF == 'mc':
            myAcqEra[key] = value
        elif typeWF == 'fs':
            myAcqEra[key] = value+'_FastSim'
    return myAcqEra


def checkLabel(workflow):
    wfLabel = {"WZMuSkim2010B" : "wzMu2010B",
               "RunMu2010B" : "mu2010B",
               "RunElectron2010B" : "electron2010B",
               "RunJet2010B" : "jet2010B",
               "WZEGSkim2010B" : "wzEG2010B",
               "WZMuSkim2010A" : "wzMu2010A",
               "RunPhoton2010B" : "photon2010B",
               "WZEGSkim2010A" : "wzEG2010A",
               "MinimumBias2010B" : "run2010B",
               "RunMinBias2010B" : "mb2010B",
               "RunMinBias2011A" : "mb2011A",
               "RunMu2011A" : "mu2011A",
               "RunElectron2011A" : "electron2011A",
               "RunPhoton2011A" : "photon2011A",
               "RunJet2011A" : "jet2011A",
               "RunCosmics2011A" : "cos2011A",
               "ValSkim2011A" : "run2011A",
               "WMuSkim2011A" : "wMu2011A",
               "WElSkim2011A" : "wEl2011A",
               "ZMuSkim2011A" : "zMu2011A",
               "ZElSkim2011A" : "zEl2011A",
               "HighMet2011A" : "hMet2011A",
               "RunMinBias2011B" : "mb2011B",
               "RunMu2011B" : "mu2011B",
               "RunElectron2011B" : "electron2011B",
               "RunPhoton2011B" : "photon2011B",
               "RunJet2011B" : "jet2011B",
               "ValSkim2011B" : "run2011B",
               "WMuSkim2011B" : "wMu2011B",
               "WElSkim2011B" : "wEl2011B",
               "ZMuSkim2011B" : "zMu2011B",
               "ZElSkim2011B" : "zEl2011B",
               "HighMet2011B" : "hMet2011B",
               "RunMinBias2012A" : "mb2012A",
               "RunTau2012A" : "tau2012A",
               "RunMET2012A" : "met2012A",
               "RunCosmicsA" : "cos2010A",
               "RunHI2011" : "hi2011",
               "MinimumBias2010A" : "run2010A",
               "RunMinBias2012B" : "mb2012B",
               "RunPhoton2012B" : "photon2012B",
               "RunEl2012B" : "electron2012B",
               "RunMu2012B" : "mu2012B",
               "RunElectron2012A" : "electron2012A",
               "RunJet2012A" : "jet2012A",
               "RunMu2012A" : "mu2012A",
               "RunJet2012B" : "jet2012B",
               "ZElSkim2012B" : "zEl2012B",
               "WElSkim2012B" : "wEl2012B",
               "RunMinBias2012C" : "mb2012C",
               "ZElSkim2012C" : "zEl2012C",
               "RunEl2012C" : "electron2012C",
               "WElSkim2012C" : "wEl2012C",
               "RunMu2012C" : "mu2012C",
               "RunJet2012C" : "jet2012C",
               "Photon2012C" : "photon2012C",
               "ZMuSkim2012A" : "zMu2012A",
               "ZElSkim2012Acalo" : "zEl2012Acalo",
               "RunPhoton2012Acalo" : "photon2012Acalo",
               "RunJetHT2012Bcalo" : "jetHT2012Bcalo",
               "RunElectron2012Bcalo" : "electron2012Bcalo",
               "RunJetHT2012Ccalo" : "jetHT2012Ccalo",
               "RunElectron2012Ccalo" : "electron2012Ccalo",
               "ZMuSkim2012Bv1" : "zMu2012Bv1",
               "ZMuSkim2012Bv2" : "zMu2012Bv2",
               "ZMuSkim2012Cv2" : "zMu2012Cv2",
               "ZMuSkim2012Cv3" : "zMu2012Cv3",
#               "ZMuSkim2012B" : "zMu2012B",
#               "ZMuSkim2012C" : "zMu2012C",
               "ZMuSkim2012D" : "zMu2012D",
               "DoubEle2012D" : "zElectron2012D",
               "MinimumBias2012D" : "mb2012D",
               "JetHT2012D" : "jetHT2012D",
               "SingMu2012D" : "mu2012D",
               "SingEle2012D" : "electron2012D",
               "SingPho2012D" : "photon2012D"
}

    for key,value in wfLabel.items():
        if key in workflow:
            return value
    print "******** There was no match between "+workflow+" and label. Aborting program... *********"
    sys.exit(0);


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
    parser.add_option('--data',action="store_true", help='Use it to inject standard data workflows only',dest='data')
    parser.add_option('--mc',action="store_true", help='Use it to inject standard MC workflows only',dest='mc')
    parser.add_option('--pu',action="store_true", help='Use it to inject PileUp workflows only',dest='pu')
    parser.add_option('--lsf',action="store_true", help='Use it to assign work to the LSF agent at CERN - vocms174, relvallsf team',dest='lsf')
    (options,args) = parser.parse_args()

    listFastSimTasks = ['TTbarFS','SingleMuPt1FS','SingleMuPt10FS','SingleMuPt100FS','SinglePiPt1FS','SinglePiPt10FS','SinglePiPt100FS','ZEEFS','ZTTFS','QCDFlatPt153000FS','QCD_Pt_80_120FS','QCD_Pt_3000_3500FS','H130GGgluonfusionFS','SingleGammaFlatPt10To10FS']

    if not options.workflow:
        print "The workflow name is mandatory!"
        print "Usage: python assignRelValWorkflow.py -w <requestName>"
        sys.exit(0);
    workflow=options.workflow
    team='relval'
    site='T1_US_FNAL'
    procversion='v1'
    activity='relval'
    lfn='/store/relval'
    specialStr = ''

    ### Getting the original dictionary
    request = getRequestDict(url,workflow)

    ### Dropping 2010 HeavyIon workflows or assigning 2011 to CERN/LSF 
    if 'RunHI2010' in request['RequestString']:
        print workflow, '\t\tHEAVY ION CANNOT RUN AT FNAL --> REQUEST DROPPED!'
        sys.exit(0);
    elif 'RunHI2011' in request['RequestString']:
        site='T2_CH_CERN'
        print workflow, '\t\tChanging to site: '+site

    ### Creating a dict of AcqEras
    myAcqEra = {}  
    for key, value in request.items():
        if key.startswith("Task"):
            #print key, value
            ### to skip the 'TaskChain' key
            if 'Chain' in key:  
                continue
            else:
                taskName = request[key]['TaskName']
                if options.pu:
#                    if 'Pyquen' in taskName:
#                        pileup =''
#                    else:
                    pileup = 'PU_'
                else:
                    pileup = ''
                try:
                    taskGT = request[key]['GlobalTag'].replace('::All','')
                except KeyError:
                    taskGT = request['GlobalTag'].replace('::All','')
                    pass
                myAcqEra[taskName] = request['CMSSWVersion']+'-'+pileup+taskGT
                
    ### TODO: handle different requests with these options below
#    specialStr = '_UpdateOldAPE'
    if options.special:
        specialStr = '_'+str(options.special)

    if options.data:
        myAcqEra = updateAcqEra(workflow, myAcqEra,'data')
    elif options.mc:
        myAcqEra = updateAcqEra(workflow, myAcqEra,'mc')

#    print request['Task1']
    for task in listFastSimTasks:
        if task in request['Task1']['TaskName']:
            myAcqEra = updateAcqEra(workflow, myAcqEra,'fs')

    for key,value in myAcqEra.items():
        myAcqEra[key] = value+specialStr

    if options.team:
        team=options.team
    if options.site:
        site=options.site
    if options.procversion:
        procversion='v'+str(options.procversion)
    if options.activity:
        activity=options.activity
    if options.lfn:
        lfn=options.lfn
    if options.lsf:
        team='relvallsf'
        site='T2_CH_CERN'

    if options.test:
        print workflow, '\tAcqEra:', myAcqEra, '\tProcVer:', procversion, '\tTeam:',team, '\tSite:', site
        sys.exit(0);

    print workflow, '\tAcqEra:', myAcqEra, '\tProcVer:', procversion, '\tTeam:',team, '\tSite:', site
    assignRequest(url,workflow,team,site,myAcqEra,procversion,activity,lfn)
    sys.exit(0);

if __name__ == "__main__":
    main()
