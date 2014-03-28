#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse
import reqMgrClient
from dbs.apis.dbsClient import DbsApi

dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

def checkAcceptedSubscriptionRequest(url, dataset, site):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&type=xfer')
        r2=conn.getresponse()
        result = json.loads(r2.read())
        requests=result['phedex']
        if 'request' not in requests.keys():
                return [False, False]
        ourNode=False
        otherNode=False
        for request in result['phedex']['request']:
                for node in request['node']:
                        if node['name']==site and node['decision']=='approved':
                                ourNode=True
                        elif 'Disk' in node['name'] and node['decision']=='approved':
                                otherNode=True
        return[ourNode, otherNode]

def getDatasetStatus(dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve dataset summary
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*',detail=True)
        return reply[0]['dataset_access_type']

def getDatasets(dataset):
       # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve dataset summary
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*')
        return reply

def getDatasetVersion(url, workflow, era, partialProcVersion):
        versionNum = 1
        outputs = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        for output in outputs:
           bits = output.split('/')
           outputCheck = '/'+bits[1]+'/'+era+'-'+partialProcVersion+'*/'+bits[len(bits)-1]

           datasets = getDatasets(outputCheck)
           for dataset in datasets:
              datasetName = dataset['dataset']
              matchObj = re.match(r".*-v(\d+)/.*", datasetName)
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
        if ps == 'SimGeneral.MixingModule.mix_E8TeV_AVE_10_BX_25ns_300ns_spread_cfi':
           pss = 'PU10bx25'
        if ps == 'SimGeneral.MixingModule.mix_E8TeV_AVE_10_BX_50ns_300ns_spread_cfi':
           pss = 'PU10bx50'
        if ps == 'SimGeneral.MixingModule.mix_2011_FinalDist_OOTPU_cfi':
           pss = 'PU_S13'	
        if ps == 'SimGeneral.MixingModule.mix_fromDB_cfi':
           pss = 'PU_RD1'
        if ps == 'SimGeneral.MixingModule.mix_2012C_Profile_PoissonOOTPU_cfi':
           pss = 'PU2012CExt'
        if ps == 'SimGeneral.MixingModule.mixNoPU_cfi':
           pss = 'NoPileUp'
        if ps == 'SimGeneral.MixingModule.mix_POISSON_average_cfi':
           pss = 'PU'


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

def findCustodialLocation(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
        r2=conn.getresponse()
        result = json.loads(r2.read())
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
        request = json.loads(r2.read())
        prepID=request['PrepID']
        return prepID

def getCampaign(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        campaign=request['Campaign']
        return campaign

def getGlobalTag(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        globalTag=request['GlobalTag']
        return globalTag

def getPileupScenario(url, workflow):
        cacheID = getCacheID(url, workflow)
        config = getConfig(url, cacheID)
        [pileup,meanPileUp,bunchSpacing,cmdLineOptions] = getPileup(config)
        scenario = getScenario(pileup)
        if scenario == 'PU140Bx25' and meanPileUp != 'Unknown':
           scenario = 'PU' + meanPileUp + 'bx25'
        if scenario == 'PU140bx25' and 'Upgrade' in workflow:
           scenario = 'PU140Bx25'
        if scenario == 'PU':
           scenario = 'PU' + meanPileUp + 'bx' + bunchSpacing
           if meanPileUp == 'None' or bunchSpacing == 'None':
              print 'ERROR: unexpected pileup settings in config'
              sys.exit(0)
        if scenario == 'PU_RD1' and cmdLineOptions != 'None':
           if '--runsAndWeightsForMC [(190482,0.924) , (194270,4.811), (200466,7.21), (207214,7.631)]' in cmdLineOptions:
              scenario = 'PU_RD2'
        return scenario

def getPileup(config):
        pu = 'Unknown'
        vmeanpu = 'None'
        bx = 'None'
        cmdLineOptions = 'None'
        lines = config.split('\n')
        for line in lines:
           if 'process.load' and 'MixingModule' in line:
              pu = line[line.find("'")+1:line.find("'",line.find("'")+1)]
           if 'process.mix.input.nbPileupEvents.averageNumber' in line:
              meanpu = line[line.find("(")+1:line.find(")")].split('.', 1)
              vmeanpu = meanpu[0]
           if 'process.mix.bunchspace' in line:
              bx = line[line.find("(")+1:line.find(")")]
           if 'with command line options' in line:
              cmdLineOptions = line
        return [pu,vmeanpu,bx,cmdLineOptions]

def getCacheID(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        cacheID=request['StepOneConfigCacheID']
        return cacheID

def getConfig(url, cacheID):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/couchdb/reqmgr_config_cache/'+cacheID+'/configFile')
        r2=conn.getresponse()
        config = r2.read()
        return config

def assignRequest(url ,workflow ,team ,site ,era, procversion, procstring, activity, lfn, maxmergeevents, maxRSS, maxVSize, useX, siteCust):

    if "Upgrade" in workflow:
       softTimeout = 159600
    else:
       softTimeout = 144000

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
              "SoftTimeout": softTimeout,
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
	parser.add_option('-s', '--site', help='Force workflow to run at this site. For HLT/AI just put use HLT.',dest='site')
	parser.add_option('-c', '--custodial', help='Custodial site',dest='siteCust')
	parser.add_option('-p', '--procstring', help='Process String',dest='inprocstring')
	parser.add_option('-m', '--procversion', help='Process Version',dest='inprocversion')
	parser.add_option('-n', '--specialstring', help='Special Process String',dest='specialprocstring')
	parser.add_option('-e', '--execute', help='Actually assign workflows',action="store_true",dest='execute')
	parser.add_option('-x', '--restrict', help='Only assign workflows for this site',dest='restrict')
	parser.add_option('-r', '--rssmax', help='Max RSS',dest='maxRSS')
	parser.add_option('-v', '--vsizemax', help='Max VMem',dest='maxVSize')
	parser.add_option('-a', '--extension', help='Use _ext special name',dest='extension')
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
	maxRSSdefault = maxRSS
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
        sites = ['T1_DE_KIT', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_ES_PIC', 'T1_TW_ASGC', 'T1_UK_RAL', 'T1_US_FNAL', 'T2_CH_CERN', 'HLT']

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

           inputDataset = reqMgrClient.getInputDataSet(url, workflow)

           # Check status of input dataset
           inputDatasetStatus = getDatasetStatus(inputDataset)
           if inputDatasetStatus != 'VALID' and inputDatasetStatus != 'PRODUCTION':
              print 'ERROR: Input dataset is not PRODUCTION or VALID, value is',inputDatasetStatus
              sys.exit(0)

           if '-ext' in inputDataset and not options.extension:
              print 'WARNING: Input dataset is an extension and extension option is not specified'

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
           if options.site == 'HLT':
              siteUse = ['T2_CH_CERN_AI', 'T2_CH_CERN_HLT', 'T2_CH_CERN']
              team = 'hlt'

           # Check if input dataset subscribed to disk endpoint
           if 'T2_CH_CERN' in siteUse:
              siteSE = 'T2_CH_CERN'
           else:
              siteSE = siteUse + '_Disk'
           [subscribedOurSite, subscribedOtherSite] = checkAcceptedSubscriptionRequest(url, inputDataset, siteSE)
           if not subscribedOurSite and not options.xrootd and 'Fall11R2' not in workflow:
              print 'ERROR: input dataset not subscribed/approved to required Disk endpoint'
              sys.exit(0)
           if options.xrootd and not subscribedOtherSite:
              print 'ERROR: input dataset not subscribed/approved to any Disk endpoint'
              sys.exit(0)

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
              if pileupScenario == 'Unknown' and 'MinBias' in pileupDataset and 'LowPU2010DR42' not in workflow:
                 print 'ERROR: unable to determine pileup scenario'
                 sys.exit(0)
              elif 'Fall11_R2' in workflow or 'Fall11_R4' in workflow or 'Fall11R2' in workflow or 'Fall11R4' in workflow:
                 matchObj = re.match(r".*Fall11-(.*)_START.*", inputDataset)
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

           era = 'Summer12'
           lfn = '/store/mc'

           #delete era and lfn so it can't reuse the ones from the previous workflow
	   del era
	   del lfn

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

           #this is incorrect for HiFall11 workflows, but is changed further down
           if 'Fall11_R' in workflow or 'Fall11R' in workflow:
              era = 'Fall11'
              lfn = '/store/mc'

           if 'Summer13dr53X' in workflow:
              era = 'Summer13dr53X'
              lfn = '/store/mc'

           if 'Summer11dr53X' in workflow:
              era = 'Summer11dr53X'
              lfn = '/store/mc'

           if 'Fall11_HLTMuonia' in workflow:
              era = 'Fall11'
              lfn = '/store/mc'
              specialName = 'HLTMuonia_'

           if 'Summer11_R' in workflow:
              era = 'Summer11'
              lfn = '/store/mc'

           if 'LowPU2010_DR42' in workflow or 'LowPU2010DR42' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'
              specialName = 'LowPU2010_DR42_'
              pileupScenario = 'PU_S0'

           if 'UpgradeL1TDR_DR6X' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'

           if 'HiWinter13' in inputDataset:
              era = 'HiWinter13'
              lfn = '/store/himc'

           if 'Winter13' in workflow and 'DR53X' in workflow:
              era = 'HiWinter13'
              lfn = '/store/himc'
           if 'HiWinter13' in workflow and 'DR53X' in workflow:
              pileupScenario = ''  
           if 'pAWinter13' in workflow and 'DR53X' in workflow:
              pileupScenario = 'pa' # not actually the pileup scenario of course
           if 'ppWinter13' in workflow and 'DR53X' in workflow:
              pileupScenario = 'pp' # not actually the pileup scenario of course

           if 'Summer11LegDR' in campaign:
              era = 'Summer11LegDR'
              lfn = '/store/mc'

           if 'UpgradePhase1Age' in campaign:
              era = 'Summer13'
	      lfn = '/store/mc'
              specialName = campaign + '_'

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

           if campaign == 'UpgradePhase1Age5H_DR61SLHCx':
              era = 'Summer13'
              lfn = '/store/mc'
              specialName = campaign + '_'

           if campaign == 'UpgradePhase1Age1K_DR61SLHCx':
              era = 'Summer13'
              lfn = '/store/mc'
              specialName = campaign + '_'

           if campaign == 'UpgradePhase1Age3K_DR61SLHCx':
              era = 'Summer13'
              lfn = '/store/mc'
              specialName = campaign + '_'

           #change back to old campaign names for UpgradePhase1
           if 'UpgradePhase1Age' in campaign and 'dr61SLHCx' in specialName:
              specialName = specialName.replace("dr61SLHCx","_DR61SLHCx")
           if 'dr61SLHCx' in specialName:
              print 'WARNING: using new campaign name format'		   

           if campaign == 'HiFall11_DR44X' or campaign == 'HiFall11DR44':
              era = 'HiFall11'
              lfn = '/store/himc'
              specialName = 'HiFall11_DR44X' + '_'

           if campaign == 'UpgFall13d':
              era = campaign
              lfn = '/store/mc'

           if campaign == 'Fall13dr':
              era = campaign
              lfn = '/store/mc'
              if '_castor_tsg_' in workflow:
                 specialName = 'castor_tsg_'
              elif '_castor_' in workflow:
                 specialName = 'castor_'
              elif '_tsg_' in workflow:
                 specialName = 'tsg_'
              elif '__' in workflow:
                 specialName = ''
              else:
                 print 'ERROR: unexpected special name string in workflow name'
                 sys.exit(0)

           # Handle NewG4Phys
           if campaign == 'Summer12DR53X' and 'NewG4Phys' in workflow:
              specialName = 'NewG4Phys_'

           # Handle BS2011
           if campaign == 'LowPU2010DR42' and 'BS2011' in workflow:
              specialName = 'LowPU2010_DR42_BS2011_'

           # Construct processed dataset version
           if pileupScenario != '':
              pileupScenario = pileupScenario+'_' 
           if options.specialprocstring:
              specialName = options.specialprocstring + '_'
           extTag = ''
           if options.extension:
              extTag = '_ext'+options.extension

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

	   #reset maxRSS to default, so it can't reuse the custom value from a previous workflow
	   maxRSS = maxRSSdefault
           if 'HiFall11' in workflow and 'IN2P3' in siteUse:
              maxRSS = 4000000

           # Set max number of merge events
           maxmergeevents = 50000
           #if 'Fall11_R1' in workflow:
           #   maxmergeevents = 6000
           if 'DR61SLHCx' in workflow:
              maxmergeevents = 5000

           # Checks
           if not era:
              print 'ERROR: era is not defined'
              sys.exit(0)

           if not lfn:
              print 'ERROR: lfn is not defined'
              sys.exit(0)

           if siteUse not in sites and options.site != 'T2_US' and siteUse != ['T2_CH_CERN_AI', 'T2_CH_CERN_HLT', 'T2_CH_CERN']:
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
                 print 'Would assign ',workflow,' with ','Acquisition Era:',era,'ProcessingString:',procstring,'ProcessingVersion:',procversion,'lfn:',lfn,'Site(s):',siteUse,'Custodial Site:',siteCust,'team:',team,'maxmergeevents:',maxmergeevents,'maxRSS:',maxRSS
              else:
                 print 'Would skip workflow ',workflow

	sys.exit(0)

if __name__ == "__main__":
	main()
