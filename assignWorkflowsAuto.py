#!/usr/bin/env python
import urllib2, urllib, httplib, sys, re, os, time, json
import optparse
import reqMgrClient
import phedexClient
from dbs.apis.dbsClient import DbsApi
from changePriorityWorkflow import changePriorityWorkflow
from utils import workflowInfo

dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

def getCampaign(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        if 'Campaign' in request:
           campaign=request['Campaign']
           return campaign
        return 'None'

def getWorkflows(url):
   conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
   r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype?stale=update_after')
   r2=conn.getresponse()
   data = json.loads(r2.read())
   items = data['rows']

   workflows = []
   for item in items:
      if 'assignment-approved' in item['key'] and 'ReDigi' in item['key']:
         workflows.append(item['key'][0])

   return workflows

def getReplicaFileCount(site,datasetName):
        url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas?dataset=' + datasetName+'&node='+site
        result = json.loads(urllib2.urlopen(url).read())
        blocks=result['phedex']['block']
        cnt=0
        if blocks:
           for block in blocks:
              if 'files' in block:
                 cnt = cnt + int(block['files'])
        return cnt

def getFileCount(dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve dataset summary
        reply = dbsapi.listBlockSummaries(dataset=dataset,detail=True)
        cnt=0
        for block in reply:
           cnt = cnt + int(block['num_file'])
        return cnt

def getSizeAtSite(site, dataset):
        actualFiles = getFileCount(dataset)
        haveFiles = getReplicaFileCount(site, dataset)
        if actualFiles > 0:
           return 100.0*float(haveFiles)/float(actualFiles)
        return 0

def getSiteWithMostInput(dataset, threshold):
        sites = phedexClient.getBlockReplicaSites(dataset)
        for site in sites:
           if 'MSS' not in site and 'Export' not in site and 'Buffer' not in site and 'EC2' not in site and 'CERN' not in site and (('T1' in site and 'AODSIM' not in dataset) or 'AODSIM' in dataset):
              completion = getSizeAtSite(site, dataset)
              if (completion == 100.0 or completion > threshold):
                 site = site.replace('_Disk', '')
                 return [site, completion]
        return ['None', 0]

def changeSplitting(url, workflow, eventsPerJob):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/StepOneProc", "splittingAlgo":"EventBased", "lumits_per_job":"", "timeout":"", "include_parents":"False", "files_per_job":"",'halt_job_on_file_boundaries':'True','events_per_job':str(eventsPerJob)}
        headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
        encodedParams = urllib.urlencode(params)
        conn.request("POST", "/reqmgr/view/handleSplittingPage", encodedParams, headers)
        response = conn.getresponse()   
        print response.status, response.reason
        data = response.read()
        print data
        conn.close()
        return

def getLFNbase(url, dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve file
        reply = dbsapi.listFiles(dataset=dataset)
        file = reply[0]['logical_file_name']
        # determine lfn
        lfn = '/store/mc'
        if '/store/himc' in file:
           lfn = '/store/himc'
        if '/store/data' in file:
           lfn = '/store/data'
        return lfn

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

def getDatasetVersion(url, workflow, era, procstring):
        versionNum = 1
        outputs = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        for output in outputs:
           bits = output.split('/')
           outputCheck = '/'+bits[1]+'/'+era+'-'+procstring+'*/'+bits[len(bits)-1]

           datasets = getDatasets(outputCheck)
           for dataset in datasets:
              datasetName = dataset['dataset']
              matchObj = re.match(r".*-v(\d+)/.*", datasetName)
              if matchObj:
                 currentVersionNum = int(matchObj.group(1))
                 if versionNum <= currentVersionNum:
                    versionNum=versionNum+1

        return versionNum

def getPileupDataset(url, workflow):
	return workflowInfo( url,workflow).getPileupDataset()

def getPriority(url, workflow): 
	return workflowInfo( url,workflow).getPriority()

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

def getEra(url, workflow):
	return workflowInfo( url, workflow).getEra()

def getCurrentStatus(url, workflow):
	return workflowInfo( url,workflow).getCurrentStatus()

def getProcString(url, workflow):
	return workflowInfo( url,workflow).getProcString()

def getRequestNumEvents(url, workflow):
	return workflowInfo( url,workflow).getRequestNumEvents()

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
	      "CustodialSubType" : 'Replica',
              "Priority" : "Normal",
              "SoftTimeout": softTimeout,
              "GracePeriod": 300,
              "MaxMergeEvents": maxmergeevents,
	      "MaxRSS": maxRSS,
              "MaxVSize": maxVSize,
              "AcquisitionEra": era,
	      "Dashboard": activity,
              "ProcessingVersion": procversion,
              "ProcessingString": procstring,
              "checkbox"+workflow: "checked"}
              
              
              
    # we don't want to subscribe these to tape and we certainly don't want move subscriptions ripping things out of T2's.
              
    if params["CustodialSites"] == 'None' or params["CustodialSites"] == '': 
       del params["CustodialSites"]
       siteCust='None'        
              
    if useX == 1:
       print "- Using xrootd for input dataset"
       params['useSiteListAsLocation'] = True

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
  	return
    conn.close()
    print 'Assigned workflow:',workflow,'to site:',site,'custodial site:',siteCust,'acquisition era:',era,'team',team,'processing string:',procstring,'processing version:',procversion,'lfn:',lfn
    return

def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-f', '--filename', help='Filename',dest='filename')
	parser.add_option('-w', '--workflow', help='Workflow',dest='userWorkflow')
	parser.add_option('-t', '--team', help='Type of Requests',dest='team')
	parser.add_option('-s', '--site', help='Force workflow to run at this site. For HLT/AI just put use HLT.',dest='site')
	parser.add_option('-k', '--ignore-restrictions', help='Ignore site restrictions',action="store_true",dest='ignoresite')
        parser.add_option('-u', '--new-priority', help='Change workflow priority to #',dest='newpriority')
	parser.add_option('-c', '--custodial', help='Custodial site',dest='siteCust')
	parser.add_option('-p', '--procstring', help='Process String',dest='inprocstring')
	parser.add_option('-m', '--procversion', help='Process Version',dest='inprocversion')
	parser.add_option('-e', '--execute', help='Actually assign workflows',action="store_true",dest='execute')
	parser.add_option('-x', '--restrict', help='Only assign workflows for this site',dest='restrict')
	parser.add_option('-z', '--threshold', help='Threshold for completeness of input dataset at site',dest='threshold')
	parser.add_option('-r', '--rssmax', help='Max RSS',dest='maxRSS')
	parser.add_option('-v', '--vsizemax', help='Max VMem',dest='maxVSize')
        parser.add_option('-o', '--xrootd', help='Read input using xrootd',action="store_true",dest='xrootd')
        parser.add_option('-i', '--ignore', help='Ignore any errors',action="store_true",dest='ignore')
	(options,args) = parser.parse_args()

	activity='reprocessing'

        if not options.restrict:
                restrict='None'
        else:
                restrict=options.restrict
        maxRSS = 2800000
        if not options.maxRSS:
                maxRSS = 3000000
        else:
                maxRSS=options.maxRSS
	maxRSSdefault = maxRSS
        maxVSize = 4100000000
        if not options.maxVSize:
                maxVSize = 4100000000
        else:
                maxVSize=options.maxVSize
	filename=options.filename

        ignore = 0
        if options.ignore:
           ignore = 1

        ignoresiterestrictions = 0
        if options.ignoresite:
           ignoresiterestrictions = 1
           
        if not options.newpriority:
           newpriority=0
        else: 
           newpriority=options.newpriority

        # Valid Tier-1 sites
        sites = ['T1_DE_KIT', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_ES_PIC', 'T1_TW_ASGC', 'T1_UK_RAL', 'T1_US_FNAL', 'T1_RU_JINR', 'T2_CH_CERN', 'HLT']

        # only assign workflows from these campaigns
        valids = ['Fall11R1', 'Fall11R2', 'Fall11R4', 'Spring14dr', 'Fall13dr', 'Summer12DR53X', 'pAWinter13DR53X', 'Cosmic70DR', 'HiFall13DR53X', 'Phys14DR', 'Summer11LegDR','Fall14DR', 'Fall14DR73', 'TP2023SHCALDR', '2019GEMUpg14DR', 'HiWinter13DR53X', 'RunIWinter15DR', '2023SHCALUpg14DR']

        # Tier-1s with no tape left, so use CERN instead
        sitesNoTape = ['T1_RU_JINR']

        if options.filename:
           f = open(filename,'r')
        elif options.userWorkflow:
           f = [options.userWorkflow]
        else:
           f = getWorkflows(url)

        workflowsNotAssignedInput = []
        workflowsAssigned = {}

        for workflow in f:
           workflow = workflow.rstrip('\n')

           if not options.xrootd:
              useX = 0
           else:
              useX = 1

           # Double check that the workflow really is in assignment-approved
           currentStatus = getCurrentStatus(url, workflow)
           if currentStatus != 'assignment-approved':
              print 'NOTE: Due to workflow status (',currentStatus,') skipping',workflow
              continue

           # Only automatically assign workflows from specified campaigns
           campaign = getCampaign(url, workflow)
           if campaign not in valids and not options.userWorkflow and not options.filename:
              print 'NOTE: Due to campaign skipping',workflow
              continue

           siteUse=options.site
           if siteUse == 'T2_US':
              siteUse =  ['T2_US_Caltech', 'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T3_US_Omaha', 'T2_US_Purdue', 'T2_US_UCSD', 'T2_US_Vanderbilt', 'T2_US_Wisconsin']
              if not options.siteCust:
                 print 'ERROR: A custodial site must be specified'
                 continue
              siteCust = options.siteCust

           # Check status of input dataset
           inputDataset = reqMgrClient.getInputDataSet(url, workflow)
           inputDatasetStatus = getDatasetStatus(inputDataset)
           if inputDatasetStatus != 'VALID' and inputDatasetStatus != 'PRODUCTION':
              print 'ERROR: Unable to assign',workflow,' because input dataset is not PRODUCTION or VALID, value is',inputDatasetStatus
              continue

           if not siteUse or siteUse == 'None':
              # Find site to run workflow if no site specified
              threshold = 98.0
              if options.threshold:
                 threshold = options.threshold
              [siteUse,completeness] = getSiteWithMostInput(inputDataset, threshold)
              if siteUse == 'None' or ('T1_' not in siteUse and 'T2_CH_CERN' not in siteUse and campaign != 'Spring14miniaod'):
                 workflowsNotAssignedInput.append(workflow)
                 continue
              if completeness < 100.0:
                 print 'Input dataset is < 100% complete (',completeness,') so enabling xrootd'
                 useX = 1
     
           # Set the custodial location if necessary
           if not options.site or options.site != 'T2_US':
              if not options.siteCust:
                 siteCust = siteUse
              else:
                 siteCust = options.siteCust
           if options.site == 'HLT':
              siteUse = ['T2_CH_CERN_HLT', 'T2_CH_CERN']

           # Some sites have no free space on tape, so send the data to CERN
           if siteUse in sitesNoTape:
              siteCust = 'T0_CH_CERN'

           # Don't specify a custodial site for miniaod
           if campaign == 'Spring14miniaod':
              siteCust = 'None'

           # Check if input dataset subscribed to disk endpoint
           siteSE = siteUse
           if 'T1' in siteUse:
              siteSE = siteSE + '_Disk'
           [subscribedOurSite, subscribedOtherSite] = checkAcceptedSubscriptionRequest(url, inputDataset, siteSE)
           if not subscribedOurSite and not options.xrootd and not ignore:
              print 'ERROR: input dataset not subscribed/approved to required Disk endpoint and xrootd option not enabled (',subscribedOurSite,subscribedOtherSite,workflow,siteSE,')'
              workflowsNotAssignedInput.append(workflow)
              continue
           if options.xrootd and not subscribedOtherSite and not ignore:
              print 'ERROR: input dataset not subscribed/approved to any Disk endpoint (',subscribedOurSite,subscribedOtherSite,')'
              continue

           # Check if pileup dataset subscribed to disk endpoint
           pileupDataset = getPileupDataset(url, workflow)
           if pileupDataset != 'None':
              [subscribedOurSite, subscribedOtherSite] = checkAcceptedSubscriptionRequest(url, pileupDataset, siteSE)
              if not subscribedOurSite and not ignore:
                 print 'ERROR: pileup dataset (',pileupDataset,') not subscribed/approved to required Disk endpoint',siteSE,' for workflow',workflow
                 continue
         
           # Decide which team to use if not already defined
           # - currently we only use production for all workflows
           if options.team:
              team = options.team
           else:
              team = 'production'

           # Get LFN base from input dataset
           lfn = getLFNbase(url, inputDataset)

	   # Set maxRSS
	   maxRSS = maxRSSdefault
           if ('HiFall11' in workflow or 'HiFall13DR53X' in workflow) and 'IN2P3' in siteUse:
              maxRSS = 4000000

           # Set max number of merge events
           maxmergeevents = 50000
           if 'Fall11R1' in workflow:
              maxmergeevents = 6000
           if 'DR61SLHCx' in workflow:
              maxmergeevents = 5000

           # Acquisition era
           era = getCampaign(url, workflow)

           # Correct situations where campaign name cannot be used as acquisition era
           if era == '2019GEMUpg14DR':
              era = 'GEM2019Upg14DR'
           if era == '2023SHCALUpg14DR':
              era = 'SHCAL2023Upg14DR'

           if era == 'None':
              print 'ERROR: unable to get campaign for workflow', workflow
              continue

           # Processing string
           if options.inprocstring: 
              procstring = options.inprocstring
           else:
              procstring = getProcString(url, workflow)

           # ProcessingVersion
           if not options.inprocversion:
              procversion = getDatasetVersion(url, workflow, era, procstring)
           else:
              procversion = options.inprocversion
 
           # Handle run-dependent MC
           if 'PU_RD' in procstring:
              numEvents = getRequestNumEvents(url, workflow)
              reqJobs = 500
              if 'PU_RD2' in procstring:
                 reqJobs = 2000
              eventsPerJob = int(numEvents/(reqJobs*1.4))
              if eventsPerJob < 2000:
                 if options.execute:
                    print 'Changing splitting to',eventsPerJob,'events per job'
                    changeSplitting(url, workflow, eventsPerJob)
                 else:
                    print 'Would change splitting to',eventsPerJob,'events per job'

           # Site checking
           if siteUse not in sites and options.site != 'T2_US' and siteUse != ['T2_CH_CERN_T0', 'T2_CH_CERN_HLT', 'T2_CH_CERN'] and not ignoresiterestrictions and siteUse != ['T2_CH_CERN_HLT', 'T2_CH_CERN']:
              if 'AODSIM' not in inputDataset:
                 print 'ERROR: invalid site',siteUse
                 continue

           workflowsAssigned[workflow] = siteUse

           if options.execute:
              if restrict == 'None' or restrict == siteUse:
	          assignRequest(url, workflow, team, siteUse, era, procversion, procstring, activity, lfn, maxmergeevents, maxRSS, maxVSize, useX, siteCust)
                  if (newpriority !=0 ):
                     changePriorityWorkflow(url,workflow,newpriority)
                     print "Priority reset to %i" % newpriority
              else:
                     print 'Skipping workflow ',workflow
           else:
              if restrict == 'None' or restrict == siteUse:
                 print 'Would assign ',workflow,' with ','Acquisition Era:',era,'ProcessingString:',procstring,'ProcessingVersion:',procversion,'lfn:',lfn,'Site(s):',siteUse,'Custodial Site:',siteCust,'team:',team
                 if (newpriority !=0 ):
                    print "Would reset priority to %i" % newpriority
              else:
                 print 'Would skip workflow ',workflow

        print ''
        print 'SUMMARY'
        print ''

        # List assigned workflows
        if len(workflowsAssigned) > 0:
           if options.execute:
              print 'Workflows assigned:'
           else:
              print 'Workflows which can be assigned:'
           for workflow in workflowsAssigned:
              print ' ',workflow,workflowsAssigned[workflow]
     
        # List workflows not assigned because of input dataset
        if len(workflowsNotAssignedInput) > 0:
           print 'Workflows not assigned because input datasets are not complete on any site:'
           for workflow in workflowsNotAssignedInput:
              print ' ',workflow

	sys.exit(0)

if __name__ == "__main__":
	main()
