#!/usr/bin/env python
from closeOutWorkflows import *

"""
    Runs Closeout script only with a selected list of workflows.
    This can be usefil when workflows get stuck
"""

def classifyAndFilterCompletedRequests(url, requests, filtered):
    """
    Sorts completed requests using the type.
    returns a dic cointaining a list for each
    type of workflows.
    """

    #filter only requests that are in the file
    workflows={'ReDigi':[],'MonteCarloFromGEN':[],'MonteCarlo':[] , 'ReReco':[], 'LHEStepZero':[]}
    for request in requests:
        name=request['id']
        #skip the ones that are not in the file
        if name not in filtered:
            continue
        #if a wrong or weird name
        if len(request['key'])<3:
            print request
            continue
        status=request['key'][1]
        if status != "completed":
            continue
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


def main():
    print "Getting requests from file"
    #get file from parameters
    wfsFile = open(sys.argv[1],'r')
    wfsList = [wf.strip() for wf in wfsFile.readlines() if wf.strip()]
    url='cmsweb.cern.ch'
    print "Gathering Requests"
    requests=getOverviewRequestsWMStats(url)
    print "Classifying Requests"
    workflowsCompleted=classifyAndFilterCompletedRequests(url, requests, wfsList)
    #print header    
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

