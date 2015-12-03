#!/usr/bin/env python
import json
import httplib, sys, re, os, random
import dbs3Client, reqMgrClient, phedexClient

"""

    Close out script:
    gathers all completed scripts and check one by one if it is ready for
    closing out:
    - Has the expected number of events
    - Datasets are properly registered on Phedex
    - Datasets are healthy (no duplicate lumis)
    This uses DBS3 client, reqMgrClient and phedexClient now instead of dbsTest.py
    and phedexSubscription.py.
    For running the previous version look for closeOutScript_leg.py

"""

T1_Disk = ['T1_DE_KIT_Disk',
            'T1_ES_PIC_Disk',
            'T1_FR_CCIN2P3_Disk',
            'T1_IT_CNAF_Disk',
#            'T1_RU_JINR_Disk',
            'T1_UK_RAL_Disk',
            'T1_US_FNAL_Disk']
T1_MSS = ['T0_CH_CERN_MSS',
            'T1_DE_KIT_MSS',
            'T1_ES_PIC_MSS',
            'T1_FR_CCIN2P3_MSS',
            'T1_IT_CNAF_MSS',
#            'T1_RU_JINR_MSS',
            'T1_UK_RAL_MSS',
            'T1_US_FNAL_MSS']


def getOverviewRequestsWMStats(url):
    """
    Retrieves workflows overview from WMStats
    by querying couch db JSON direcly
    """
    #TODO use the couch API from WMStatsClient instead of wmstats URL
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                     key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET",
                 "/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype?stale=update_after")
    response = conn.getresponse()
    data = response.read()
    conn.close()
    myString=data.decode('utf-8')
    workflows=json.loads(myString)['rows']
    return workflows


def classifyCompletedRequests(url, requests):
    """
    Sorts completed requests using the type.
    returns a dic cointaining a list for each
    type of workflows.
    """
    workflows={'ReDigi':[],'MonteCarloFromGEN':[],'MonteCarlo':[] , 'ReReco':[], 'LHEStepZero':[], 'StoreResults':[],
                'TaskChain':[]}
    for request in requests:

        name=request['id']
        #if a wrong or weird name
        if len(request['key'])<3:
            print request
            continue
        
        #discard RelVals
        if 'RVCMSSW' in name:
            continue
        
        status=request['key'][1]
        #only completed requests
        if status=='completed':
            requestType=request['key'][2]
            #sort by type
            if requestType=='MonteCarlo':
                #MonteCarlo's which datasets end with /GEN
                #are Step0
                try:
                    datasets = reqMgrClient.outputdatasetsWorkflow(url, name)
                    m = re.search('.*/GEN$', datasets[0])
                    if m:
                        workflows['LHEStepZero'].append(name)
                    else:
                        workflows[requestType].append(name)
                    #TODO identify MonteCarlo with two output
                except Exception as e:
                    print "Error on wf", name
                    continue
            elif requestType=='TaskChain':
                #only taskchains with MC or ReDigi subType
                subType = reqMgrClient.getWorkflowSubType(url, name)
                if subType in ['MC','ReDigi']:
                    workflows[requestType].append(name)
            elif requestType in ['MonteCarloFromGEN', 'LHEStepZero', 'ReDigi', 'ReReco', 'StoreResults']:
                workflows[requestType].append(name)
            
    return workflows

def validateClosingWorkflow(url, workflow, closePercentage = 0.95, checkEqual=False, 
            checkDuplicates=True, checkLumiNumb=False, checkPhedex='custodial'):
    """
    Validates if a workflow can be closed out, using different parameters of validation.
    returns the response as a dict.
    checkPhedex can be 'custodial', 'any' or False
    """
    #inputDataset = reqMgrClient.getInputDataSet(url, workflow)
    result = {'name':workflow.name, 'datasets': {}}
    result['datasets'] = dict( (ds,{}) for ds in workflow.outputDatasets)
    closeOutWorkflow = True
    #check if dataset is ready
    #TODO validate here if workflow is MonteCarlo from GEN with two output
    for dataset in workflow.outputDatasets:
        closeOutDataset = False
        try:
            percentage = workflow.percentageCompletion(dataset, skipInvalid=True)
        except Exception as e:
            print 'Error getting information from DBS', workflow, dataset
            percentage = 0.0
        #retrieve either custodial or all subscriptions.
        try:
            if checkPhedex == 'custodial':
                phedexReqs = phedexClient.getCustodialSubscriptionRequestSite(dataset)
            elif checkPhedex == 'any':
                phedexReqs = phedexClient.getSubscriptionSites(dataset)
            else:
                phedexReqs = None
        except Exception:
            print 'Error getting phedex info,: ', dataset
            phedexReqs = None
        duplicate = None
        correctLumis = None
        transPerc = None
        missingSubs = False
        equalFiles = None

        dbsFiles = dbs3Client.getFileCountDataset(dataset)
        phdFiles = phedexClient.getFileCountDataset(url,dataset)
        equalFiles = (dbsFiles == phdFiles)

        #Check first percentage
        if ((checkEqual and percentage == closePercentage)
            or (not checkEqual and percentage >= closePercentage)
            or dataset.endswith("DQMIO") ): #DQMIO are exceptions (have 0 events)
            #if we need to check duplicates
            if checkDuplicates:
                try:
                    duplicate = dbs3Client.duplicateRunLumi(dataset, skipInvalid=True)
                except Exception:
                    print "Error in checking duplicate lumis for", dataset
            #if we need to check for correct lumi number
            if checkLumiNumb:
                correctLumis = checkCorrectLumisEventGEN(dataset)
           
            #dataset healthy means:
            # checkDuplicates -> no duplicates
            # checkLumiNumb -> correct
            if (not (checkDuplicates and duplicate) and
                not ( checkLumiNumb and not correctLumis)):
                #if phedex check not required we can closeout
                if not checkPhedex:
                    #last check, that files are equal
                    closeOutDataset = equalFiles
                #if phedex check is required and has it
                elif checkPhedex and phedexReqs:
                    try:
                        transPerc = phedexClient.getTransferPercentage(url, dataset, phedexReqs[0])
                    except:
                        transPerc = None
                    #last check, that files are equal
                    closeOutDataset = equalFiles
                else:
                    #TODO only missing subscription if equal # of files
                    missingSubs = equalFiles
        #if at least one dataset is not ready wf cannot be closed out
        closeOutWorkflow = closeOutWorkflow and closeOutDataset
        #load results in a dict        
        result['datasets'][dataset]["percentage"] = percentage
        result['datasets'][dataset]["duplicate"] = duplicate
        result['datasets'][dataset]["phedexReqs"] = phedexReqs
        result['datasets'][dataset]["closeOutDataset"] = closeOutDataset
        result['datasets'][dataset]["transPerc"] = transPerc
        result['datasets'][dataset]["correctLumis"] = correctLumis
        result['datasets'][dataset]["missingSubs"] = missingSubs
        result['datasets'][dataset]["dbsFiles"] = dbsFiles
        result['datasets'][dataset]["phedexFiles"] = phdFiles

    result['closeOutWorkflow'] = closeOutWorkflow
    return result


def printResult(result):
    """
    Prints the result of analysing a workflow
    """
    for dsname, ds in result['datasets'].items():
        print ('| %80s | %100s | %4s | %5s| %3s | %5s| %5s|%5s| ' % 
           (result["name"], dsname,
            "%.1f"%(ds["percentage"]*100),
            "?" if ds["duplicate"] is None else ds["duplicate"],
            "?" if ds["correctLumis"] is None else ds["correctLumis"],
             ','.join(ds["phedexReqs"]) if ds["phedexReqs"] else str(ds["phedexReqs"]),
            "?" if ds["transPerc"] is None else str(int(ds["transPerc"]*100)),
            ds["closeOutDataset"]))

def closeOutReRecoWorkflows(url, workflows):
    """
    closes rereco workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        if 'RelVal' in wf:
            continue
        if 'TEST' in wf:
            continue        
        #first validate if effectively is completed
        workflow = reqMgrClient.ReReco(wf)
        if workflow.status != 'completed':
            continue
        #closeout workflow, checking percentage equalst 100%
        result = validateClosingWorkflow(url, workflow, closePercentage=1.0, 
            checkEqual=True, checkDuplicates=False)
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)
    print '-'*180
    return noSiteWorkflows

def closeOutRedigiWorkflows(url, workflows):
    """
    Closes Redigi workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        #first validate if effectively is completed
        workflow = reqMgrClient.ReDigi(wf)
        if workflow.status != 'completed':
            continue
        #if miniaod
        if 'miniaod' in workflow.name:
            #we don't check for custodial subscription
            result = validateClosingWorkflow(url, workflow, 0.95, checkPhedex=False)        
        else:
            #check dataset health, duplicates, subscription, etc.       
            result = validateClosingWorkflow(url, workflow, 0.95)
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)
    print '-'*180
    return noSiteWorkflows

def closeOutMonterCarloRequests(url, workflows, fromGen):
    """
    Closes either montecarlo or montecarlo from gen
    workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        #get all info from ReqMgr
        if not fromGen:  
            workflow = reqMgrClient.MonteCarlo(wf, url)
        else:
            workflow = reqMgrClient.MonteCarloFromGen(wf, url)
        #validate if complete
        if workflow.status != 'completed':
            continue
        #skip montecarlos on a special queue
        if workflow.team == 'analysis':
            continue
        # validation for SMS montecarlos
        if 'SMS' in workflow.outputDatasets[0]:
            closePercentage= 1.00
        else:
            closePercentage = 0.95
        #check dataset health, duplicates, subscription, etc.       
        result = validateClosingWorkflow(url, workflow, closePercentage=closePercentage)           
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)
            
    #separation line
    print '-'*180
    return noSiteWorkflows


def closeOutStep0Requests(url, workflows):
    """
    Closes either montecarlo step0 requests
    """
    noSiteWorkflows = []
    for wf in workflows:
        #info from reqMgr
        workflow = reqMgrClient.StepZero(wf, url)
        #first validate if effectively is completed
        if workflow.status != 'completed':
            continue
        #skip montecarlos on a special queue
        if workflow.team == 'analysis':
            continue
        #check dataset health, duplicates, subscription, etc.       
        result = validateClosingWorkflow(url, workflow, checkLumiNumb=True)
  
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)

    print '-'*180
    return noSiteWorkflows

def closeOutStoreResultsWorkflows(url, workflows):
    """
    Closeout StoreResults workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        #info from reqMgr            
        workflow = reqMgrClient.StoreResults(wf, url)
        #first validate if effectively is completed
        if workflow.status != 'completed':
            continue
        #closeout workflow, checking percentage equalst 100%
        result = validateClosingWorkflow(url, workflow, closePercentage=1.0, 
            checkEqual=True, checkDuplicates=False, checkPhedex='any')
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)
    print '-'*180
    return noSiteWorkflows

def closeOutTaskChain(url, workflows):
    """
    Closeout taskchained workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        #first validate if effectively is completed
        workflow = reqMgrClient.TaskChain(wf, url)
        if workflow.status != 'completed':
            continue
        result = validateClosingTaskChain(url, workflow)   
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)
    print '-'*180
    return noSiteWorkflows 


def validateClosingTaskChain(url, workflow):
    """
    Calculates a Percentage completion for a taskchain.
    Taking step/filter efficiency into account.
    test with pdmvserv_task_SUS-Summer12WMLHE-00004__v1_T_141003_120119_9755
    """
    inputEvents = workflow.getInputEvents()
    
    #if subtype doesn't come with the request, we decide based on dataset names
    fromGen = False
    #if no output dataset ends with GEN or LHE
    if not re.match('.*/(GEN|LHE)$', workflow.outputDatasets[0]):
        fromGen = False
    elif (re.match('.*/(GEN|LHE)$', workflow.outputDatasets[0])
        and re.match('.*/(GEN-SIM|GEN)$', workflow.outputDatasets[1])):
        fromGen = True

    #task-chain 1 (without filterEff)
    if not fromGen:
        #validate with the regular procedure
        result = validateClosingWorkflow(url, workflow, 0.95)
        return result
    #task-chain 2 GEN, GEN-SIM, GEN-SIM-RAW, AODSIM, DQM
    else:
        #GEN/LHE and GEN-SIM
        result = {'name':workflow.name, 'datasets': {}}
        result['datasets'] = dict( (ds,{}) for ds in workflow.outputDatasets)
        closeOutWorkflow = True
        i = 1
        for dataset in workflow.outputDatasets:
            closeOutDataset = False
            #percentage
            outputEvents = workflow.getOutputEvents(dataset)
            filterEff = workflow.getFilterEfficiency('Task%d'%i)
            #GEN/LHE and GEN-SIM
            if 1 <= i <= 2:
                #decrease filter eff
                inputEvents *= filterEff
            #percentage
            percentage = outputEvents/float(inputEvents) if inputEvents > 0 else 0.0
            #phedex request
            phedexReqs = phedexClient.getCustodialSubscriptionRequestSite(dataset)
            #all validations
            duplicate = None
            correctLumis = None
            transPerc = None
            missingSubs = False
            
            #TODO test
            dbsFiles = dbs3Client.getFileCountDataset(dataset)
            phdFiles = phedexClient.getFileCountDataset(url,dataset)
            equalFiles = (dbsFiles == phdFiles)

            #Check first percentage
            if percentage >= 0.95:
                #if we need to check duplicates            
                duplicate = dbs3Client.duplicateRunLumi(dataset)         
                #dataset healthy means:
                # checkDuplicates -> no duplicates
                if not duplicate:
                    #if phedex check not required we can closeout
                    if phedexReqs:
                        try:
                            transPerc = phedexClient.getTransferPercentage(url, dataset, phedexReqs[0])
                        except:
                            transPerc = None
                        #last check if files are equal
                        closeOutDataset = equalFiles
                    else:
                        missingSubs = True
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            #load results in a dict        
            result['datasets'][dataset]["percentage"] = percentage
            result['datasets'][dataset]["duplicate"] = duplicate
            result['datasets'][dataset]["phedexReqs"] = phedexReqs
            result['datasets'][dataset]["closeOutDataset"] = closeOutDataset
            result['datasets'][dataset]["transPerc"] = transPerc
            result['datasets'][dataset]["correctLumis"] = correctLumis
            result['datasets'][dataset]["missingSubs"] = missingSubs
            result['datasets'][dataset]["dbsFiles"] = dbsFiles
            result['datasets'][dataset]["phedexFiles"] = phdFiles
            i += 1
        result['closeOutWorkflow'] = closeOutWorkflow
        return result

def checkCorrectLumisEventGEN(dataset):
    """
    Checks that the dataset has more than 300 events per lumi
    """
    numlumis = dbs3Client.getLumiCountDataSet(dataset)
    numEvents = dbs3Client.getEventCountDataSet(dataset)
    # numEvents / numLumis >= 300
    if numlumis >= numEvents / 300.0:
        return True
    else:
        return False
  
def percentageCompletion(url, workflow, dataset):
    """
    Calculates Percentage of completion for a given workflow
    taking a particular output dataset
    """
    
    inputEvents = reqMgrClient.getInputEvents(url, workflow)
    outputEvents = reqMgrClient.getOutputEvents(url, workflow, dataset)
    
    if inputEvents == 0:
        return 0
    if not outputEvents:
        return 0
    perc = outputEvents/float(inputEvents)
    return perc

def listWorkflows(workflows):
    for wf in workflows:
        for ds in wf.outputDatasets:
            print '| %80s | %100s |'%(wf.name,ds)
    print '-'*150

def listSubscriptions(subs):
    for ds, site in subs:
        print '| %80s | %100s |'%(ds,site)
    print '-'*150

def makeSubscriptions(url, workflows):
    result = []    
    for wf in workflows:
        comments = 'Output of %s'%wf.name
        
        #if the wf has input - where the input was subscribed
        if 'InputDataset' in wf.info:
            site = phedexClient.getCustodialSubscriptionRequestSite(wf.inputDataset)
            if not site:
                site_MSS = random.choice(T1_MSS)# "T1_US_FNAL_MSS
            r = phedexClient.makeReplicaRequest(url, site_MSS, wf.outputDatasets, comments, custodial='y')
            for ds in wf.outputDatasets:
                result.append((ds, site_MSS))    
        #if the workflow does not have input
        else:
            site_disk =random.choice(T1_Disk)# "T1_US_FNAL_Disk"
            site_MSS = random.choice(T1_MSS)# "T1_US_FNAL_MSS"        
            print "Making subscriptions",wf.name
            print "To",site_disk, site_MSS
            
            #create move to disk and replica to tape
            #r = phedexClient.makeMoveRequest(url, site_disk, workflow.outputDatasets, comments, custodial='n')
            r = phedexClient.makeReplicaRequest(url, site_MSS, wf.outputDatasets, comments, custodial='y')
            #result
            for ds in wf.outputDatasets:
                result.append((ds, site_disk+', '+site_MSS))
    return result


def main():
    url='cmsweb.cern.ch'
    print "Gathering Requests"
    requests = getOverviewRequestsWMStats(url)
    print "Classifying Requests"
    workflowsCompleted = classifyCompletedRequests(url, requests)

    #print header
    print '-'*220
    print '| Request'+(' '*74)+'| OutputDataSet'+(' '*86)+'|%Compl|Dupl|Correct|Subscr|Tran|ClosOu|'
    print '-'*220
    noSiteWorkflows = closeOutReRecoWorkflows(url, workflowsCompleted['ReReco'])
    workflowsCompleted['NoSite-ReReco'] = noSiteWorkflows

    noSiteWorkflows = closeOutRedigiWorkflows(url, workflowsCompleted['ReDigi'])
    workflowsCompleted['NoSite-ReDigi'] = noSiteWorkflows
    noSiteWorkflows = closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarlo'], fromGen=False)
    workflowsCompleted['NoSite-MonteCarlo'] = noSiteWorkflows
    noSiteWorkflows = closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarloFromGEN'], fromGen=True)
    workflowsCompleted['NoSite-MonteCarloFromGEN'] = noSiteWorkflows
    noSiteWorkflows = closeOutTaskChain(url, workflowsCompleted['TaskChain'])
    workflowsCompleted['NoSite-TaskChain'] = noSiteWorkflows
    noSiteWorkflows = closeOutStep0Requests(url, workflowsCompleted['LHEStepZero'])
    workflowsCompleted['NoSite-LHEStepZero'] = noSiteWorkflows
    noSiteWorkflows = closeOutStoreResultsWorkflows(url, workflowsCompleted['StoreResults'])
    workflowsCompleted['NoSite-StoreResults'] = noSiteWorkflows

    print "MC Workflows for which couldn't find Custodial Tier1 Site"
    listWorkflows(workflowsCompleted['NoSite-ReReco'])
    listWorkflows(workflowsCompleted['NoSite-ReDigi'])
    listWorkflows(workflowsCompleted['NoSite-MonteCarlo'])
    listWorkflows(workflowsCompleted['NoSite-MonteCarloFromGEN'])
    listWorkflows(workflowsCompleted['NoSite-TaskChain'])
    listWorkflows(workflowsCompleted['NoSite-LHEStepZero'])

    print "StoreResults Workflows for which couldn't find PhEDEx Subscription"
    listWorkflows(workflowsCompleted['NoSite-StoreResults'])

    sys.exit(0);

if __name__ == "__main__":
    main()

