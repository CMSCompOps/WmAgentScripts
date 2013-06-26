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
              print 'ERROR: Problem checking output datasets: ',output
           #   sys.exit(0)
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

        if ps == 'SimGeneral.MixingModule.mix_E8TeV_AVE_16_BX_25ns_cfi':
           pss = 'PU140Bx25'
        if ps == 'SimGeneral.MixingModule.mix_2012_Summer_50ns_PoissonOOTPU_cfi':
           pss = 'PU_S10'
        if ps == 'SimGeneral.MixingModule.mix_E7TeV_Fall2011_Reprocess_50ns_PoissonOOTPU_cfi':
           pss = 'PU_S6'

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

def getCampaign(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.read(r2.read())
        campaign=request['Campaign']
        return campaign

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
        [pileup,meanPileUp] = getPileup(config)
        scenario = getScenario(pileup)
	if scenario == 'PU140Bx25' and meanPileUp != 'Unknown':
	   scenario = 'PU' + meanPileUp + 'bx25'
        if scenario == 'PU140bx25' and 'Upgrade' in workflow:
           scenario = 'PU140Bx25'
        return scenario

def getPileup(config):
        pu = 'Unknown'
        vmeanpu = 'None'
        lines = config.split('\n')
        for line in lines:
           if 'process.load' and 'MixingModule' in line:
              pu = line[line.find("'")+1:line.find("'",line.find("'")+1)]
           if 'process.mix.input.nbPileupEvents.averageNumber' in line:
              meanpu = line[line.find("(")+1:line.find(")")].split('.', 1)
              vmeanpu = meanpu[0]

        return [pu,vmeanpu]

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

def assignRequest(url ,workflow ,team ,site ,era, procversion, procstring, activity, lfn, maxmergeevents, maxRSS, maxVSize, useX, siteCust):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "CustodialSites": siteCust,
              "Priority" : "Normal",
              "SoftTimeout": 129600,
              "GracePeriod": 300,
              "MaxMergeEvents": maxmergeevents,
	      "maxRSS": maxRSS,
              "maxVSize": maxVSize,
              "AcquisitionEra": era,
	      "dashboard": activity,
              "ProcessingVersion": procversion,
              "ProcessingString": procstring,
              "checkbox"+workflow: "checked"}

    if useX == 1:
       print "- Using xrootd for input dataset"
       params['useSiteListAsLocation'] = "true"

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
    print 'Assigned workflow:',workflow,'to site:',site,'custodial site:',siteCust,'acquisition era:',era,'team',team,'processin string:',procstring,'processing version:',procversion,'lfn:',lfn,'maxmergeevents:',maxmergeevents,'maxRSS:',maxRSS,'maxVSize:',maxVSize
    return



def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-f', '--filename', help='Filename',dest='filename')
	parser.add_option('-w', '--workflow', help='Workflow',dest='userWorkflow')
	parser.add_option('-t', '--team', help='Type of Requests',dest='team')
	parser.add_option('-s', '--site', help='Force workflow to run at this site',dest='site')
	parser.add_option('-c', '--custodial', help='Custodial site',dest='siteCust')
	parser.add_option('-p', '--procstring', help='Process String',dest='inprocstring')
	parser.add_option('-m', '--procversion', help='Process Version',dest='inprocversion')
	parser.add_option('-n', '--specialstring', help='Special Process String',dest='specialprocstring')
	parser.add_option('-e', '--execute', help='Actually assign workflows',action="store_true",dest='execute')
	parser.add_option('-x', '--restrict', help='Only assign workflows for this site',dest='restrict')
	parser.add_option('-r', '--rssmax', help='Max RSS',dest='maxRSS')
	parser.add_option('-v', '--vsizemax', help='Max VMem',dest='maxVSize')
	parser.add_option('-a', '--extension', help='Use _ext special name',action="store_true",dest='extension')
        parser.add_option('-o', '--xrootd', help='Read input using xrootd',action="store_true",dest='xrootd')
	(options,args) = parser.parse_args()
	if not options.filename and not options.userWorkflow:
		print "A filename or workflow is required"
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

        if not options.xrootd:
           useX = 0
        else:
           useX = 1

        # Valid Tier-1 sites
        sites = ['T1_DE_KIT', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_ES_PIC', 'T1_TW_ASGC', 'T1_UK_RAL', 'T1_US_FNAL']

        if options.filename:
           f=open(filename,'r')
        else:
           f=[options.userWorkflow]

        for workflow in f:
           workflow = workflow.rstrip('\n')
           siteUse=options.site
           if siteUse == 'T2_US':
              siteUse =  ['T2_US_Caltech', 'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T3_US_Omaha', 'T2_US_Purdue', 'T2_US_UCSD', 'T2_US_Vanderbilt', 'T2_US_Wisconsin']
              if not options.siteCust:
                 print 'ERROR: A custodial site must be specified'
                 sys.exit(0)
              siteCust = options.siteCust

           team=options.team

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
     
           # Set the custodial location if necessary
           if not options.site or options.site != 'T2_US':
              if not options.siteCust:
                 siteCust = siteUse
              else:
                 siteCust = options.siteCust

           # Extract required part of global tag
           gtRaw = getGlobalTag(url, workflow)
           gtBits = gtRaw.split('::')
           globalTag = gtBits[0]

           # Get campaign name
           campaign = getCampaign(url, workflow)
         
           # Determine pileup scenario
           # - Fall11_R2 & Fall11_R4 don't add pileup so extract pileup scenario from input
           pileupScenario = ''
           if not options.inprocstring:
              pileupDataset = getPileupDataset(url, workflow)
              pileupScenario = getPileupScenario(url, workflow)
              if campaign == 'Summer12_DR53X_RD':
                 pileupScenario = 'PU_RD1'
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

              if pileupScenario == 'Unknown':
                 pileupScenario = ''

           # Decide which team to use if not already defined
           if not team:
              priority = int(getPriority(url, workflow))
              if priority < 100000:
                 team = 'reproc_lowprio'
              else:
                 team = 'reproc_highprio'

           specialName = ''

           #era = 'Summer12'
           #lfn = '/store/mc'

           # Set era, lfn and campaign-dependent part of name if necessary
           if 'Summer12_DR51X' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'

           if 'Summer12_DR52X' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'

           if 'Summer12_DR53X' in workflow or ('Summer12' in workflow and 'DR53X' in workflow):
              era = 'Summer12_DR53X'
              lfn = '/store/mc'

           if 'Fall11_R' in workflow:
              era = 'Fall11'
              lfn = '/store/mc'

           if 'Summer13dr53X' in workflow:
              era = 'Summer13dr53X'
              lfn = '/store/mc'

           if 'Fall11_HLTMuonia' in workflow:
              era = 'Fall11'
              lfn = '/store/mc'
              specialName = 'HLTMuonia_'

           if 'Summer11_R' in workflow:
              era = 'Summer11'
              lfn = '/store/mc'

           if 'LowPU2010_DR42' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'
              specialName = 'LowPU2010_DR42_'

           if 'UpgradeL1TDR_DR6X' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'

           if 'HiWinter13' in inputDataset:
              era = 'HiWinter13'
              lfn = '/store/himc'

           if 'Winter13_DR53X' in workflow:
              era = 'HiWinter13'
              lfn = '/store/himc'
           if 'HiWinter13_DR53X' in workflow:
              pileupScenario = ''  
           if 'pAWinter13_DR53X' in workflow:
              pileupScenario = 'pa' # not actually the pileup scenario of course
           if 'ppWinter13_DR53X' in workflow:
              pileupScenario = 'pp' # not actually the pileup scenario of course

           if campaign == 'UpgradePhase2LB4PS_2013_DR61SLHCx':
              era = 'Summer13'
              lfn = '/store/mc'
              specialName = campaign + '_'

           if campaign == 'UpgradePhase2BE_2013_DR61SLHCx':
              era = 'Summer13'
              lfn = '/store/mc'
              specialName = campaign + '_'

           if campaign == 'UpgradePhase2LB6PS_2013_DR61SLHCx':
              era = 'Summer13'
              lfn = '/store/mc'
              specialName = campaign + '_'
  
           if campaign == 'UpgradePhase1Age0DES_DR61SLHCx':
              era = 'Summer13'
              lfn = '/store/mc'
              specialName = campaign + '_'
           
           if campaign == 'UpgradePhase1Age0START_DR61SLHCx':
              era = 'Summer13'
              lfn = '/store/mc'
              specialName = campaign + '_'

           if campaign == 'UpgradePhase1Age3H_DR61SLHCx':
              era = 'Summer13'
              lfn = '/store/mc'
              specialName = campaign + '_'

           # Construct processed dataset version
           if pileupScenario != '':
              pileupScenario = pileupScenario+'_' 
           if options.specialprocstring:
              specialName = options.specialprocstring + '_'
           extTag = ''
           if options.extension:
              extTag = '_ext'

           # ProcessingString
           if not options.inprocstring:
              procstring = specialName+pileupScenario+globalTag+extTag
           else:
              procstring = options.inprocstring

           # ProcessingVersion
           if not options.inprocversion:
              procversion = getDatasetVersion(url, workflow, era, procstring)
           else:
              procversion = options.inprocversion

           # Set max number of merge events
           maxmergeevents = 50000
           if 'Fall11_R1' in workflow:
              maxmergeevents = 6000
           if 'DR61SLHCx' in workflow:
              maxmergeevents = 5000

           # Checks
           if not era:
              print 'ERROR: era is not defined'
              sys.exit(0)

           if not lfn:
              print 'ERROR: lfn is not defined'
              sys.exit(0)

           if siteUse not in sites and options.site != 'T2_US':
              print 'ERROR: invalid site'
              sys.exit(0)

           if pileupScenario == 'Unknown':
              print 'ERROR: unable to determine pileup scenario'
              sys.exit(0)

           if options.execute:
              if restrict == 'None' or restrict == siteUse:
	         assignRequest(url, workflow, team, siteUse, era, procversion, procstring, activity, lfn, maxmergeevents, maxRSS, maxVSize, useX, siteCust)
              else:
                 print 'Skipping workflow ',workflow
           else:
              if restrict == 'None' or restrict == siteUse:
                 print 'Would assign ',workflow,' with ','Acquisition Era:',era,'ProcessingString:',procstring,'ProcessingVersion:',procversion,'lfn:',lfn,'Site(s):',siteUse,'Custodial Site:',siteCust,'team:',team,'maxmergeevents:',maxmergeevents
              else:
                 print 'Would skip workflow ',workflow

	sys.exit(0)

if __name__ == "__main__":
	main()
