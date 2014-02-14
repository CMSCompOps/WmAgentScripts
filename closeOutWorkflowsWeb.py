#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os
import time, shutil
from closeOutWorkflows import *
from xml.dom.minidom import getDOMImplementation

"""
    Runs Closeout script generating the output to a Web Page.
    Useful for sharing close out script information
"""

outputfile = '/afs/cern.ch/user/j/jbadillo/www/closeout.html'
tempfile = '/afs/cern.ch/user/j/jbadillo/www/temp.html'

def closeOutReRecoWorkflowsWeb(url, workflows, output):
    """
    Closeout ReReco workflows. Generates web output.
    """
    noSiteWorkflows = []
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
            
            #validate when percentage is ok but has not phedex subscription
            if percentage == 1 and not phedexSubscription:
                noSiteWorkflows.append(workflow)

            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(percentage*100)),
                                                    str(phedexSubscription), 100, duplicate, closeOutDataset)
            #web output
            output.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td></td><td>%s</td></tr>'%
                 (workflow, dataset,str(int(percentage*100)),str(phedexSubscription), 100, duplicate, closeOutDataset))
            
        #workflow can only be closed out if all datasets are ready
        if closeOutWorkflow:
            reqMgrClient.closeOutWorkflowCascade(url, workflow)
    print '-'*180
    return noSiteWorkflows


def closeOutRedigiWorkflowsWeb(url, workflows, output):
    """
    Closes out a list of redigi workflows. generates web output
    """
    noSiteWorkflows = []
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
            #validate when percentage is ok but has not phedex subscription
            if percentage >= float(0.95) and not phedexSubscription:
                noSiteWorkflows.append(workflow)
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(percentage*100)),
                                                    str(phedexSubscription), 100, duplicate, closeOutDataset)
            #web output
            output.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td></td><td>%s</td></tr>'%
                (workflow, dataset,str(int(percentage*100)),str(phedexSubscription), 100, duplicate, closeOutDataset))
        #workflow can only be closed out if all datasets are ready
        if closeOutWorkflow:
            reqMgrClient.closeOutWorkflowCascade(url, workflow)
    print '-'*180
    return noSiteWorkflows

def closeOutMonterCarloRequestsWeb(url, workflows,output):
    """
    Closes either montecarlo or montecarlo from gen
    workflows
    """
    noSiteWorkflows = []
    for workflow in workflows:
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        closeOutWorkflow = True
        #skip montecarlos on a special queue
        if reqMgrClient.getRequestTeam(url, workflow) == 'analysis':
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
            #validate when percentage is ok but has not phedex subscription
            if percentage >= float(closePercentage) and not phedexSubscription:
                noSiteWorkflows.append(workflow)
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| %5s|' % (workflow, dataset,str(int(percentage*100)),
                        str(phedexSubscription), str(int(transPerc*100)), duplicate, closedBlocks, closeOutDataset)
            #web output
            output.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'%
              (workflow, dataset,str(int(percentage*100)),str(phedexSubscription), 
                str(int(transPerc*100)), duplicate, closedBlocks, closeOutDataset))
        #workflow can only be closed out if all datasets are ready
        if closeOutWorkflow:
            reqMgrClient.closeOutWorkflowCascade(url, workflow)
    #separation line
    print '-'*180
    return noSiteWorkflows

def closeOutStep0RequestsWeb(url, workflows, output):
    """
    Closes either montecarlo step0 requests
    """
    noSiteWorkflows = []
    for workflow in workflows:
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        closeOutWorkflow = True
        #skip montecarlos on a special queue
        if reqMgrClient.getRequestTeam(url, workflow) == 'analysis':
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
                transPerc = phedexClient.getTransferPercentage(url, dataset, phedexSubscription)
                duplicate = dbs3Client.duplicateLumi(dataset)
                correctLumis = checkCorrectLumisEventGEN(dataset)
                #TODO validate closed blocks
                if not duplicate and correctLumis:
                    closeOutDataset = True
                else:
                    closeOutDataset = False
            #validate when percentage is ok but has not phedex subscription
            if percentage >= float(0.95) and not phedexSubscription:
                noSiteWorkflows.append(workflow)
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            print '| %80s | %100s | %4s | %5s| %3s | %5s| %5s| ' % (workflow, dataset,str(int(percentage*100)),
                        str(phedexSubscription), str(correctLumis), duplicate, closeOutDataset)
            #web output
            output.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td></td><td>%s</td></tr>'%
                (workflow, dataset,str(int(percentage*100)),str(phedexSubscription), str(correctLumis), duplicate, closeOutDataset))
        #workflow can only be closed out if all datasets are ready
        if closeOutWorkflow:
            reqMgrClient.closeOutWorkflowCascade(url, workflow)
    print '-'*180
    return noSiteWorkflows

def writeHTMLHeader(output):
    output.write('<html>')
    output.write('<head>')
    output.write('<link rel="stylesheet" type="text/css" href="style.css" />')
    output.write('</head>')
    output.write('<body>')


def listWorkflows(workflows, output):
    for wf in workflows:
        print wf
        output.write('<tr><td>'+wf+'</td></tr>')
    output.write('<tr><td></td></tr>')

def main():
    output = open(tempfile,'w')
    url='cmsweb.cern.ch'
    print "Gathering Requests"
    requests=getOverviewRequestsWMStats(url)
    print "Classifying Requests"
    workflowsCompleted=classifyCompletedRequests(url, requests)
    #header
    writeHTMLHeader(output)
    #print header
    print '-'*220
    print '| Request                                                                          | OutputDataSet                                                                                        |%Compl|Subscr|Tran|Dupl|Blocks|ClosOu|'
    print '-'*220
    output.write('<table border=1> <tr><th>Request</th><th>OutputDataSet</th><th>%Compl</th>'
                '<th>Subscr</th><th>Tran</th><th>Dupl</th><th>Blocks</th><th>ClosOu</th></tr>')
    
    output.write('<tr><th colspan="8">ReReco </th></tr>')
    noSiteWorkflows = closeOutReRecoWorkflowsWeb(url, workflowsCompleted['ReReco'], output)
    workflowsCompleted['NoSite-ReReco'] = noSiteWorkflows
    
    output.write('<tr><th colspan="8">ReDigi </th></tr>')
    noSiteWorkflows = closeOutRedigiWorkflowsWeb(url, workflowsCompleted['ReDigi'], output)
    workflowsCompleted['NoSite-ReDigi'] = noSiteWorkflows
    
    output.write('<tr><th colspan="8">MonteCarlo </th></tr>')
    noSiteWorkflows = closeOutMonterCarloRequestsWeb(url, workflowsCompleted['MonteCarlo'], output)
    workflowsCompleted['NoSite-MonteCarlo'] = noSiteWorkflows
    
    output.write('<tr><th colspan="8">MonteCarloFromGEN </th></tr>')
    noSiteWorkflows = closeOutMonterCarloRequestsWeb(url, workflowsCompleted['MonteCarloFromGEN'], output)
    workflowsCompleted['NoSite-MonteCarloFromGEN'] = noSiteWorkflows
    
    output.write('<tr><th colspan="8">LHEStepZero </th></tr>')
    noSiteWorkflows = closeOutStep0RequestsWeb(url, workflowsCompleted['LHEStepZero'],output)
    workflowsCompleted['NoSite-LHEStepZero'] = noSiteWorkflows
    output.write('</table><br><br>')
    print "MC Workflows for which couldn't find Custodial Tier1 Site"

    output.write("<table border=1> <tr><th>MC Workflows for which couldn't find Custodial Tier1 Site</th></tr>")
    listWorkflows(workflowsCompleted['NoSite-ReReco'], output)
    listWorkflows(workflowsCompleted['NoSite-ReDigi'], output)
    listWorkflows(workflowsCompleted['NoSite-MonteCarlo'], output)
    listWorkflows(workflowsCompleted['NoSite-MonteCarloFromGEN'], output)
    listWorkflows(workflowsCompleted['NoSite-LHEStepZero'], output)
    output.write('</table>')
  
    output.write('<p>Last update: '+time.strftime("%c")+' CERN time</p>')
    output.write('</body>')
    output.write('</html>')
    output.close()
    #copy temporal to definitive file, avoid unavailability when running
    shutil.copy(tempfile, outputfile)  
    sys.exit(0);

if __name__ == "__main__":
	main()

