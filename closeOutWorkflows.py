#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, duplicateEventsGen
from xml.dom.minidom import getDOMImplementation
import dbs3Client, reqMgrClient, phedexClient

"""

    Close out script:
    gathers all completed scripts and check one by one if it is ready for
    closing out:
    - Has the expected number of events
    - Datasets are properly registered on Phedex
    - Datasets are healthy (no duplicate lumis)

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
    workflows={'ReDigi':[],'MonteCarloFromGEN':[],'MonteCarlo':[] , 'ReReco':[], 'LHEStepZero':[]}
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
            elif requestType in ['MonteCarloFromGEN', 'LHEStepZero', 'ReDigi', 'ReReco']:
                workflows[requestType].append(name)
    return workflows

def closeOutReRecoWorkflows(url, workflows):
    """
    Closeout ReReco workflows
    """
    for workflow in workflows:
        if 'RelVal' in workflow:
            continue
        if 'TEST' in workflow:
            continue        
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        inputDataset = reqMgrClient.getInputDataSet(url, workflow)
        closeOutWorkflow = True
        #check if dataset is ready
        for dataset in datasets:
            duplicate = False
            closeOutDataset = True
            percentage = percentageCompletion(url, workflow, dataset)
            phedexSubscription = phedexClient.hasCustodialSubscription(dataset)
            closeOutDataset = False
            #dataset can be closed out only with 100% of events
            if percentage == 1 and phedexSubscription and not duplicate:
                closeOutDataset = True
            else:
                closeOutDataset = False
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(percentage*100)),
                                                    str(phedexSubscription), 100, duplicate, closeOutDataset)
        #workflow can only be closed out if all datasets are ready
        if closeOutWorkflow:
            reqMgrClient.closeOutWorkflow(url, workflow)
    print '-'*180

def closeOutRedigiWorkflows(url, workflows):
    """
    Closes out a list of redigi workflows
    """
    for workflow in workflows:
        closeOutWorkflow = True
        inputDataset = reqMgrClient.getInputDataSet(url, workflow)
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        for dataset in datasets:
            closeOutDataset = False
            percentage = percentageCompletion(url, workflow, dataset)
            phedexSubscription = phedexClient.hasCustodialSubscription(dataset)
            duplicate = None
            # if dataset has subscription and more than 95% events we check
            # duplicates
            if phedexSubscription and percentage >= float(0.95):
                duplicate = dbs3Client.duplicateRunLumi(dataset)
                #if not duplicate events, dataset is ready
                if not duplicate:
                    closeOutDataset = True
                else:
                    closeOutDataset = False
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(percentage*100)),
                                                    str(PhedexSubscription), 100, duplicate, closeOutDataset)
        #workflow can only be closed out if all datasets are ready
        if closeOutWorkflow:
            reqMgrClient.closeOutWorkflow(url, workflow)
    print '-'*180

def closeOutMonterCarloRequests(url, workflows):
    """
    Closes either montecarlo or montecarlo from gen
    workflows
    """
    for workflow in workflows:
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        closeOutWorkflow = True
        #skip montecarlos on a special queue
        if reqMgr.getRequestTeam(url, workflow) == 'analysis':
            continue
        for dataset in datasets:
            closePercentage = 0.95
            # validation for SMS montecarlos
            if 'SMS' in dataset:
                closePercentage= 1.00
            percentage = percentageCompletion(url, workflow, dataset)
            phedexSubscription = phedexClient.getCustodialMoveSubscriptionSite(dataset)
            transPerc = 0
            closedBlocks = None
            duplicate = None
            # if dataset has subscription and enough events we check
            # duplicates, transfer percentage and closed blocks
            if phedexSubscription and percentage >= float(closePercentage):
                transPerc = phedexClient.getTransferPercentage(url, dataset, phedexSubscription)
                duplicate = dbs3Client.duplicateLumi(dataset)
                closedBlocks = dbs3Client.hasAllBlocksClosed(dataset)
                #TODO validate closed blocks
                if not duplicate:
                    closeOutDataset = True
                else:
                    closeOutDataset = False
            else:
                closeOutDataset = False
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| %5s|' % (workflow, dataset,str(int(percentage*100)),
                        str(PhedexSubscription), str(int(transPerc*100)), duplicate, closedBlocks, closeOutDataset)
        #workflow can only be closed out if all datasets are ready
        if closeOutWorkflow:
            reqMgrClient.closeOutWorkflow(url, workflow)
    #separation line
    print '-'*180

def closeOutStep0Requests(url, workflows):
    """
    Closes either montecarlo step0 requests
    """
    for workflow in workflows:
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        closeOutWorkflow = True
        #skip montecarlos on a special queue
        if reqMgr.getRequestTeam(url, workflow) == 'analysis':
            continue
        for dataset in datasets:
            closeOutDataset = False
            percentage = percentageCompletion(url, workflow, dataset)
            phedexSubscription = phedexClient.getCustodialMoveSubscriptionSite(dataset)
            transPerc = 0
            closedBlocks = None
            duplicate = None
            correctLumis = None
            # if dataset has subscription and enough events we check
            # duplicates, transfer percentage, closed blocks and lumis
            if phedexSubscription and percentage >= float(0.95):
                transPerc = TransferPercentage(url, dataset, site)
                duplicate = dbs3Client.duplicateLumi(dataset)
                correctLumis = checkCorrectLumisEventGEN(dataset)
                #TODO validate closed blocks
                if not duplicate and correctLumis:
                    closeOutDataset = True
                else:
                    closeOutDataset = False
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            print '| %80s | %100s | %4s | %5s| %3s | %5s| %5s| ' % (workflow, dataset,str(int(Percentage*100)),
                        str(PhedexSubscription), str(correctLumis), duplicate, closeOutDataset)
        #workflow can only be closed out if all datasets are ready
        if closeOutWorkflow:
            reqMgrClient.closeOutWorkflow(url, workflow)
    print '-'*180


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


def main():
    url='cmsweb.cern.ch'
    print "Gathering Requests"
    requests = getOverviewRequestsWMStats(url)
    print "Classifying Requests"
    workflowsCompleted = classifyCompletedRequests(url, requests)
    print '-'*220
    print '| Request                                                                          | OutputDataSet                                                                                        |%Compl|Subscr|Tran|Dupl|Blocks|ClosOu|'
    print '-'*220
    closeOutReRecoWorkflows(url, workflowsCompleted['ReReco'])	
    closeOutRedigiWorkflows(url, workflowsCompleted['ReDigi'])
    closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarlo'])
    closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarloFromGEN'])
    closeOutStep0Requests(url, workflowsCompleted['LHEStepZero'])
    print "MC Workflows for which couldn't find Custodial Tier1 Site"
    if 'NoSite' in workflowsCompleted['MonteCarlo']:
        print workflowsCompleted['MonteCarlo']['NoSite']
    if 'NoSite' in workflowsCompleted['MonteCarloFromGEN']:
        print workflowsCompleted['MonteCarloFromGEN']['NoSite']
    sys.exit(0);

if __name__ == "__main__":
    main()

