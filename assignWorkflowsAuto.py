#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse

def getStatusDataSet(dataset):
        output=os.popen("./dbssql --input='find dataset.status where dataset="+dataset+" and dataset.status=*'"+ "|awk '{print $2}' ").read()
        try:
                myStatus = output[output.find("'")+1:output.find("'",output.find("'")+1)]
                return myStatus
        except ValueError:
                return "Unknown"

def outputdatasetsWorkflow(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=' + workflow)
        r2=conn.getresponse()
        datasets = json.read(r2.read())
        if len(datasets)==0:
                print "ERROR: No output datasets for this workflow"
                sys.exit(0)
        return datasets

def getDatasetVersion(url, workflow, era, partialProcVersion):
        versionNum = 1
        outputs = outputdatasetsWorkflow(url, workflow)
        for output in outputs:
           if 'None-v0' not in output:
              print 'ERROR: Problem checking output datasets'
              sys.exit(0)
           bits = output.split('/')
           lastbit = bits[len(bits)-1]
           outputCheck = re.sub(r'None-v0', era+'-'+partialProcVersion+'*', output)
           output=os.popen("./dbssql --input='find dataset where dataset="+outputCheck+" and dataset.status=*' | grep "+lastbit).read()
           lines = output.split('\n')
           for line in lines:
              matchObj = re.match(r".*-v(\d+)/.*", line)
              if matchObj:
                 currentVersionNum = int(matchObj.group(1))
                 if versionNum <= currentVersionNum:
                    versionNum=versionNum+1

        return versionNum

def getScenario(ps):
        pss = 'Unknown'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59),probValue=cms.vdouble(2.56e-06,5.239e-06,1.42e-05,5.005e-05,0.0001001,0.0002705,0.001999,0.006097,0.01046,0.01383,0.01685,0.02055,0.02572,0.03262,0.04121,0.04977,0.05539,0.05725,0.05607,0.05312,0.05008,0.04763,0.04558,0.04363,0.04159,0.03933,0.03681,0.03406,0.03116,0.02818,0.02519,0.02226,0.01946,0.01682,0.01437,0.01215,0.01016,0.0084,0.006873,0.005564,0.004457,0.003533,0.002772,0.002154,0.001656,0.001261,0.0009513,0.0007107,0.0005259,0.0003856,0.0002801,0.0002017,0.0001439,0.0001017,7.126e-05,4.948e-05,3.405e-05,2.322e-05,1.57e-05,5.005e-06)),OOT_type=cms.untracked.string('Poisson'),":
           pss = 'PU_S10'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(averageNumber=cms.double(32.0)),sequential=cms.untracked.bool(False),type=cms.string('poisson'),":
           pss = 'PU_S9'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(2),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-3),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(averageNumber=cms.double(4.0)),sequential=cms.untracked.bool(False),type=cms.string('poisson'),":
           pss = 'PU_S8'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59),probValue=cms.vdouble(2.344e-05,2.344e-05,2.344e-05,2.344e-05,0.0004687,0.0004687,0.0007032,0.0009414,0.001234,0.001603,0.002464,0.00325,0.005021,0.006644,0.008502,0.01121,0.01518,0.02033,0.02608,0.03171,0.03667,0.0406,0.04338,0.0452,0.04641,0.04735,0.04816,0.04881,0.04917,0.04909,0.04842,0.04707,0.04501,0.04228,0.03896,0.03521,0.03118,0.02702,0.02287,0.01885,0.01508,0.01166,0.008673,0.00619,0.004222,0.002746,0.001698,0.0009971,0.0005549,0.0002924,0.0001457,6.864e-05,3.054e-05,1.282e-05,5.081e-06,1.898e-06,6.688e-07,2.221e-07,6.947e-08,2.047e-08)),OOT_type=cms.untracked.string('Poisson'),":
           pss = 'PU_S7'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49),probValue=cms.vdouble(0.003388501,0.010357558,0.024724258,0.042348605,0.058279812,0.068851751,0.072914824,0.071579609,0.066811668,0.060672356,0.054528356,0.04919354,0.044886042,0.041341896,0.0384679,0.035871463,0.03341952,0.030915649,0.028395374,0.025798107,0.023237445,0.020602754,0.0180688,0.015559693,0.013211063,0.010964293,0.008920993,0.007080504,0.005499239,0.004187022,0.003096474,0.002237361,0.001566428,0.001074149,0.000721755,0.000470838,0.00030268,0.000184665,0.000112883,6.74043e-05,3.82178e-05,2.22847e-05,1.20933e-05,6.96173e-06,3.4689e-06,1.96172e-06,8.49283e-07,5.02393e-07,2.15311e-07,9.56938e-08)),OOT_type=cms.untracked.string('Poisson'),":
           pss = 'PU_S6'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),seed=cms.untracked.int32(54321),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24),probValue=cms.vdouble(0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0630151648,0.0526654164,0.0402754482,0.0292988928,0.0194384503,0.0122016783,0.007207042,0.004003637,0.0020278322,0.0010739954,0.0004595759,0.0002229748,0.0001028162,4.5833715281e-05)),OOT_type=cms.untracked.string('Poisson'),":
           pss = 'PU_S4'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(0),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(0),bunchspace=cms.int32(450),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),seed=cms.untracked.int32(54321),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10),probValue=cms.vdouble(0.145168,0.251419,0.251596,0.17943,0.1,0.05,0.02,0.01,0.005,0.002,0.001)),sequential=cms.untracked.bool(False),type=cms.string('probFunction'),":
           pss = 'PU_S0'

        return pss

def getPileupDataset(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/view/showWorkload?requestName='+workflow)
        r2=conn.getresponse()
        workload=r2.read()
        list = workload.split('\n')

        pileupDataset = 'None'

        for line in list:
           if 'request.schema.MCPileup' in line:
              pileupDataset = line[line.find("[")+1:line.find("]")]

        return pileupDataset


def getPriority(url, workflow): 
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/view/showWorkload?requestName='+workflow)
        r2=conn.getresponse()
        workload=r2.read()
        list = workload.split('\n')
              
        priority = -1 
        
        for line in list:
           if 'request.schema.RequestPriority' in line:
              priority = line[line.find("=")+1:line.find("<br/")]

        priority = priority.strip()
        priority = re.sub(r'\'', '', priority)
        return priority

def getInputDataSet(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.read(r2.read())
        inputDataSets=request['InputDataset']
        if len(inputDataSets)<1:
                print "ERROR: No InputDataSet for workflow"
        else:   
                return inputDataSets

def findCustodialLocation(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
        r2=conn.getresponse()
        result = json.read(r2.read())
        request=result['phedex']
        if 'block' not in request.keys():
                return "No Site"
        if len(request['block'])==0:
                return "No Site"
        for replica in request['block'][0]['replica']:
                if replica['custodial']=="y" and replica['node']!="T0_CH_CERN_MSS":
                        return replica['node']
        return "None"

def getPrepID(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.read(r2.read())
        prepID=request['PrepID']
        return prepID

def getGlobalTag(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.read(r2.read())
        globalTag=request['GlobalTag']
        return globalTag

def getPileupScenario(url, workflow):
        cacheID = getCacheID(url, workflow)
        config = getConfig(url, cacheID)
        pileup = getPileup(config)
        scenario = getScenario(pileup)
        return scenario

def getPileup(config):
        lines = config.split('\n')
        want = ''
        i=0
        for line in lines:
           if 'MixingModule' in line and 'EDProducer' in line:
              i=1
           if 'fileNames' in line:
              i=0
           if i == 1:
              want = want + line

        want = re.sub(r'\s+', '', want)
        want = re.sub(r'\"', '', want)
        want = re.sub(r'\n', '', want)
           
        pu = want
        return pu

def getCacheID(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.read(r2.read())
        cacheID=request['StepOneConfigCacheID']
        return cacheID

def getConfig(url, cacheID):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/couchdb/reqmgr_config_cache/'+cacheID+'/configFile')
        r2=conn.getresponse()
        config = r2.read()
        return config

def assignRequest(url,workflow,team,site,era,procversion, activity, lfn, maxmergeevents, maxRSS, maxVSize):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              #"Memory": 2300.0,
              #"SizePerEvent": 342110,
              #"TimePerEvent": 17.5,
              "SoftTimeout": 171600,
              "GracePeriod": 300,
              "MaxMergeEvents": maxmergeevents,
	      "maxRSS": maxRSS,
              "maxVSize": maxVSize,
              "AcquisitionEra": era,
	      "dashboard": activity,
              "ProcessingVersion": procversion,
              "checkbox"+workflow: "checked"}

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
    print 'Assigned workflow:',workflow,'to site:',site,'acquisition era:',era,'processing version:',procversion,'lfn:',lfn,'maxmergeevents:',maxmergeevents,'maxRSS:',maxRSS,'maxVSize:',maxVSize
    return



def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-f', '--filename', help='Filename',dest='filename')
	parser.add_option('-t', '--team', help='Type of Requests',dest='team')
	parser.add_option('-s', '--site', help='Force workflow to run at this site',dest='site')
	parser.add_option('-p', '--procversion', help='Processing Version',dest='procversion')
	parser.add_option('-e', '--execute', help='Actually assign workflows',action="store_true",dest='execute')
	parser.add_option('-x', '--restrict', help='Only assign workflows for this site',dest='restrict')
	parser.add_option('-r', '--rssmax', help='Max RSS',dest='maxRSS')
	parser.add_option('-v', '--vsizemax', help='Max VMem',dest='maxVSize')
	(options,args) = parser.parse_args()
	if not options.filename:
		print "A filename is required"
		sys.exit(0)
	activity='reprocessing'
        if not options.restrict:
                restrict='None'
        else:
                restrict=options.restrict
        maxRSS = 2300000
        if not options.maxRSS:
                maxRSS = 2300000
        else:
                maxRSS=options.maxRSS
        maxVSize = 4100000000
        if not options.maxVSize:
                maxVSize = 4100000000
        else:
                maxVSize=options.maxVSize
	filename=options.filename

        # Valid Tier-1 sites
        sites = ['T1_DE_KIT', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_ES_PIC', 'T1_TW_ASGC', 'T1_UK_RAL', 'T1_US_FNAL']

        f=open(filename,'r')
        for workflow in f:
           workflow = workflow.rstrip('\n')
           siteUse=options.site
           team=options.team
           procversion=options.procversion

           inputDataset = getInputDataSet(url, workflow)

           # Check status of input dataset
           inputDatasetStatus = getStatusDataSet(inputDataset)
           if inputDatasetStatus != 'VALID' and inputDatasetStatus != 'PRODUCTION':
              print 'ERROR: Input dataset is not PRODUCTION or VALID'
              sys.exit(0)

           if not siteUse or siteUse == 'None':
              # Determine site where workflow should be run
              count=0
              for site in sites:
                 if site in workflow:
                    count=count+1
                    siteUse = site

              # Find custodial location of input dataset if workflow name contains no T1 site or multiple T1 sites
              if count==0 or count>1:
                 siteUse = findCustodialLocation(url, inputDataset)
                 if siteUse == 'None':
                    print 'ERROR: No custodial site found'
                    sys.exit(0)
                 siteUse = siteUse[:-4]

           # Extract required part of global tag
           gtRaw = getGlobalTag(url, workflow)
           gtBits = gtRaw.split('::')
           globalTag = gtBits[0]
         
           # Determine pileup scenario
           # - Fall11_R2 & Fall11_R4 don't add pileup so extract pileup scenario from input
           pileupDataset = getPileupDataset(url, workflow)
           pileupScenario = getPileupScenario(url, workflow)
           if pileupScenario == 'Unknown' and 'MinBias' in pileupDataset:
              print 'ERROR: unable to determine pileup scenario'
              sys.exit(0)
           elif 'Fall11_R2' in workflow or 'Fall11_R4' in workflow:
              inDataSet = getInputDataSet(url, workflow)
              matchObj = re.match(r".*Fall11-(.*)_START.*", inDataSet)
              if matchObj:
                 pileupScenario = matchObj.group(1)
              else:
                 pileupScenario == 'Unknown'
           elif pileupScenario == 'Unknown' and 'MinBias' not in pileupDataset:
              pileupScenario = 'NoPileUp'

           # Decide which team to use if not already defined
           if not team:
              priority = int(getPriority(url, workflow))
              if priority < 100000:
                 team = 't1'
              else:
                 team = 't1_highprio'

           specialName = ''

           # Set era, lfn and campaign-dependent part of name if necessary
           if 'Summer12_DR51X' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'

           if 'Summer12_DR52X' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'

           if 'Summer12_DR53X' in workflow:
              era = 'Summer12_DR53X'
              lfn = '/store/mc'

           if 'Fall11_R' in workflow:
              era = 'Fall11'
              lfn = '/store/mc'

           if 'Summer11_R' in workflow:
              era = 'Summer11'
              lfn = '/store/mc'

           if 'LowPU2010_DR42' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'
              specialName = 'LowPU2010_DR42_'

           # Construct processed dataset version
           if not procversion:
              procversion = specialName+pileupScenario+'_'+globalTag+'-v'
              iVersion = getDatasetVersion(url, workflow, era, procversion)
              procversion = procversion+str(iVersion)

           # Set max number of merge events
           maxmergeevents = 50000
           if 'Fall11_R1' in workflow:
              maxmergeevents = 6000

           # Checks
           if not era:
              print 'ERROR: era is not defined'
              sys.exit(0)

           if not lfn:
              print 'ERROR: lfn is not defined'
              sys.exit(0)

           if siteUse not in sites:
              print 'ERROR: invalid site'
              sys.exit(0)

           if pileupScenario == 'Unknown':
              print 'ERROR: unable to determine pileup scenario'
              sys.exit(0)

           if options.execute:
              if restrict == 'None' or restrict == siteUse:
	         assignRequest(url,workflow,team,siteUse,era,procversion, activity, lfn, maxmergeevents, maxRSS, maxVSize)
              else:
                 print 'Skipping workflow ',workflow
           else:
              if restrict == 'None' or restrict == siteUse:
                 print 'Would assign ',workflow,' with ',era,procversion,lfn,siteUse,team,maxmergeevents,activity,maxRSS,maxVSize
              else:
                 print 'Would skip workflow ',workflow

	sys.exit(0)

if __name__ == "__main__":
	main()
