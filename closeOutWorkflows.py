#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
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

def getOverviewRequestsWMStats(url):
    """
    Retrieves workflows overview from WMStats
    by querying couch db JSON direcly
    """
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                     key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET",
                 "/couchdb/wmstats/_design/WMStats/_view/requestByStatusAndType?stale=update_after")
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
    workflows={'ReDigi':[],'MonteCarloFromGEN':[],'MonteCarlo':[] , 'ReReco':[], 'LHEStepZero':[], 'StoreResults':[]}
    for request in requests:
        name=request['id']
        #if a wrong or weird name
        if len(request['key'])<3:
            print request
            continue
        status=request['key'][1]
        #only completed requests
        if status=='completed':
            requestType=request['key'][2]
            #sort by type
            if requestType=='MonteCarlo':
                #MonteCarlo's which datasets end with /GEN
                #are Step0
                datasets = reqMgrClient.outputdatasetsWorkflow(url, name)
                m = re.search('.*/GEN$', datasets[0])
                if m:
                    workflows['LHEStepZero'].append(name)
                else:
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
    #datasets
    datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
    #inputDataset = reqMgrClient.getInputDataSet(url, workflow)
    result = {'name':workflow, 'datasets': {}}
    result['datasets'] = dict( (ds,{}) for ds in datasets)
    closeOutWorkflow = True
    #check if dataset is ready
    for dataset in datasets:
        closeOutDataset = False            
        percentage = percentageCompletion(url, workflow, dataset)
        #retrieve either custodial or all subscriptions.
        if checkPhedex == 'custodial':
            phedexReqs = phedexClient.getCustodialSubscriptionRequestSite(dataset)
        elif checkPhedex == 'any':
            phedexReqs = phedexClient.getSubscriptionSites(dataset)
        else:
            phedexRequs = None
        duplicate = None
        correctLumis = None
        transPerc = None
        missingSubs = False
        #Check first percentage
        if ((checkEqual and percentage == closePercentage)
            or (not checkEqual and percentage >= closePercentage) ):
            #if we need to check duplicates            
            if checkDuplicates:
                duplicate = dbs3Client.duplicateRunLumi(dataset)         
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
                    closeOutDataset = True
                #if phedex check is required and has it
                elif checkPhedex and phedexReqs:
                    try:
                        transPerc = phedexClient.getTransferPercentage(url, dataset, phedexReqs[0])
                    except:
                        transPerc = None
                    closeOutDataset = True
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
    for workflow in workflows:
        if 'RelVal' in workflow:
            continue
        if 'TEST' in workflow:
            continue        
        #first validate if effectively is completed
        status = reqMgrClient.getWorkflowStatus(url, workflow)
        if status != 'completed':
            continue
        #closeout workflow, checking percentage equalst 100%
        result = validateClosingWorkflow(url, workflow, closePercentage=1.0, 
            checkEqual=True, checkDuplicates=False)
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow)
        #populate the list without subs
        for (ds,info) in result['datasets'].items():
            if info['missingSubs']:
                noSiteWorkflows.append((workflow,ds))
    print '-'*180
    return noSiteWorkflows

def closeOutRedigiWorkflows(url, workflows):
    """
    Closes Redigi workflows
    """
    noSiteWorkflows = []
    for workflow in workflows:
        #first validate if effectively is completed
        status = reqMgrClient.getWorkflowStatus(url, workflow)
        if status != 'completed':
            continue
        #check dataset health, duplicates, subscription, etc.       
        result = validateClosingWorkflow(url, workflow, 0.95)           
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow)
        #populate the list without subs
        for (ds,info) in result['datasets'].items():
           if info['missingSubs']:
                noSiteWorkflows.append((workflow,ds))
    print '-'*180
    return noSiteWorkflows

def closeOutMonterCarloRequests(url, workflows):
    """
    Closes either montecarlo or montecarlo from gen
    workflows
    """
    noSiteWorkflows = []
    for workflow in workflows:
        #first validate if effectively is completed
        status = reqMgrClient.getWorkflowStatus(url, workflow)
        if status != 'completed':
            continue
        #skip montecarlos on a special queue
        if reqMgrClient.getRequestTeam(url, workflow) == 'analysis':
            continue
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        # validation for SMS montecarlos
        if 'SMS' in datasets[0]:
            closePercentage= 1.00
        else:
            closePercentage = 0.95
        #check dataset health, duplicates, subscription, etc.       
        result = validateClosingWorkflow(url, workflow, closePercentage=closePercentage)           
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow)
        #populate the list without subs
        for (ds,info) in result['datasets'].items():
            if info['missingSubs']:
                noSiteWorkflows.append((workflow,ds))
    #separation line
    print '-'*180
    return noSiteWorkflows

def closeOutStep0Requests(url, workflows):
    """
    Closes either montecarlo step0 requests
    """
    noSiteWorkflows = []
    for workflow in workflows:
        #first validate if effectively is completed
        status = reqMgrClient.getWorkflowStatus(url, workflow)
        if status != 'completed':
            continue
        #skip montecarlos on a special queue
        if reqMgrClient.getRequestTeam(url, workflow) == 'analysis':
            continue
        
        #if miniaod
        if 'miniaod' in workflow:
            #we don't check for custodial subscription
            result = validateClosingWorkflow(url, workflow, checkLumiNumb=True, checkPhedex=False)            
        else        
            #check dataset health, duplicates, subscription, etc.       
            result = validateClosingWorkflow(url, workflow, checkLumiNumb=True)
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow)
        #populate the list without subs
        for (ds,info) in result['datasets'].items():
            if info['missingSubs']:
                noSiteWorkflows.append((workflow,ds))

    print '-'*180
    return noSiteWorkflows

def closeOutStoreResultsWorkflows(url, workflows):
    """
    Closeout StoreResults workflows
    """
    noSiteWorkflows = []
    for workflow in workflows:
        #first validate if effectively is completed
        status = reqMgrClient.getWorkflowStatus(url, workflow)
        if status != 'completed':
            continue
        #closeout workflow, checking percentage equalst 100%
        result = validateClosingWorkflow(url, workflow, closePercentage=1.0, 
            checkEqual=True, checkDuplicates=False, checkPhedex='any')
        printResult(result)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow)
        #populate the list without subs
        for (ds,info) in result['datasets'].items():
            if info['missingSubs']:
                noSiteWorkflows.append((workflow,ds))
    print '-'*180
    return noSiteWorkflows


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
    percentage = outputEvents/float(inputEvents)
    return percentage

def listWorkflows(workflows):
    for (wf,ds) in workflows:
        print '| %80s | %100s |'%(wf,ds)
    print '-'*150

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

    noSiteWorkflows = closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarlo'])
    workflowsCompleted['NoSite-MonteCarlo'] = noSiteWorkflows

    noSiteWorkflows = closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarloFromGEN'])
    workflowsCompleted['NoSite-MonteCarloFromGEN'] = noSiteWorkflows
    
    noSiteWorkflows = closeOutStep0Requests(url, workflowsCompleted['LHEStepZero'])
    workflowsCompleted['NoSite-LHEStepZero'] = noSiteWorkflows

    noSiteWorkflows = closeOutStoreResultsWorkflows(url, workflowsCompleted['StoreResults'])
    workflowsCompleted['NoSite-StoreResults'] = noSiteWorkflows

    print "MC Workflows for which couldn't find Custodial Tier1 Site"
    listWorkflows(workflowsCompleted['NoSite-ReReco'])
    listWorkflows(workflowsCompleted['NoSite-ReDigi'])
    listWorkflows(workflowsCompleted['NoSite-MonteCarlo'])
    listWorkflows(workflowsCompleted['NoSite-MonteCarloFromGEN'])
    listWorkflows(workflowsCompleted['NoSite-LHEStepZero'])

    print "StoreResults Workflows for which couldn't find PhEDEx Subscription"
    listWorkflows(workflowsCompleted['NoSite-StoreResults'])

    sys.exit(0);

if __name__ == "__main__":
    main()

