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
        #first validate if effectively is completed
        status = reqMgrClient.getWorkflowStatus(url, workflow)
        if status != 'completed':
            continue
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        inputDataset = reqMgrClient.getInputDataSet(url, workflow)
        closeOutWorkflow = True
        #check if dataset is ready
        for dataset in datasets:
            closeOutDataset = False            
            percentage = percentageCompletion(url, workflow, dataset)
            phedexReqs = phedexClient.getCustodialSubscriptionRequestSite(dataset)
            duplicate = None
            #Check first 100% events
            if percentage == 1:
                #if correct %, check duplicate lumis
                if not duplicate:
                    #if dataset healthy, check for subscription
                    if phedexReqs:
                        closeOutDataset = True
                    else:
                        #add to the missing-site workflows
                        noSiteWorkflows.append(workflow)
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            phedexReqs = ','.join(phedexReqs) if phedexReqs else str(phedexReqs)
            print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(percentage*100)),
                                                    duplicate,None, phedexReqs, closeOutDataset)
            #web output
            output.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'%
                 (workflow, dataset,str(int(percentage*100)), duplicate, None, phedexReqs, closeOutDataset))
            
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
        #first validate if effectively is completed
        status = reqMgrClient.getWorkflowStatus(url, workflow)
        if status != 'completed':
            continue
        closeOutWorkflow = True
        inputDataset = reqMgrClient.getInputDataSet(url, workflow)
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        for dataset in datasets:
            closeOutDataset = False
            percentage = percentageCompletion(url, workflow, dataset)
            phedexReqs = phedexClient.getCustodialSubscriptionRequestSite(dataset)
            duplicate = None
            #Check first 95% events
            if percentage >= float(0.95):
                #if correct %, check duplicate lumis
                duplicate = dbs3Client.duplicateLumi(dataset)
                if not duplicate:
                    #if dataset healthy, check for subscription
                    if phedexReqs:
                        closeOutDataset = True
                    else:
                        #add to the missing-site workflows
                        noSiteWorkflows.append(workflow)
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            phedexReqs = ','.join(phedexReqs) if phedexReqs else str(phedexReqs)
            print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(percentage*100)),
                                                    duplicate,None, phedexReqs, closeOutDataset)
            #web output
            output.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'%
                (workflow, dataset,str(int(percentage*100)),duplicate,None, phedexReqs, closeOutDataset))
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
        #first validate if effectively is completed
        status = reqMgrClient.getWorkflowStatus(url, workflow)
        if status != 'completed':
            continue
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        closeOutWorkflow = True
        #skip montecarlos on a special queue
        if reqMgrClient.getRequestTeam(url, workflow) == 'analysis':
            continue
        for dataset in datasets:
            closeOutDataset = False
            closePercentage = 0.95
            # validation for SMS montecarlos
            if 'SMS' in dataset:
                closePercentage= 1.00
            percentage = percentageCompletion(url, workflow, dataset)
            phedexReqs = phedexClient.getCustodialSubscriptionRequestSite(dataset)
            transPerc = None
            duplicate = None
            #Check first % events
            if percentage >= closePercentage:
                #if correct %, check duplicate lumis
                duplicate = dbs3Client.duplicateLumi(dataset)
                if not duplicate:
                    #if dataset healthy, check for subscription
                    if phedexReqs:
                        closeOutDataset = True
                        #get transfer percentage
                        transPerc = phedexClient.getTransferPercentage(url, dataset, phedexReqs[0])
                        transPerc = int(transPerc*100)
                    else:
                        #add to the missing-site workflows
                        print "here!!!!", phedexReqs
                        noSiteWorkflows.append(workflow)
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            #format for printing
            phedexReqs = ','.join(phedexReqs) if phedexReqs else str(phedexReqs)
            print '| %80s | %100s | %4s | %5s| %3s | %5s| %5s|' % (workflow, dataset,str(int(percentage*100)),
                      duplicate, transPerc, phedexReqs, closeOutDataset)
            #web output
            output.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'%
              (workflow, dataset,str(int(percentage*100)), duplicate, transPerc, phedexReqs, closeOutDataset))
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
        #first validate if effectively is completed
        status = reqMgrClient.getWorkflowStatus(url, workflow)
        if status != 'completed':
            continue
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        closeOutWorkflow = True
        #skip montecarlos on a special queue
        if reqMgrClient.getRequestTeam(url, workflow) == 'analysis':
            continue
        for dataset in datasets:
            closeOutDataset = False
            percentage = percentageCompletion(url, workflow, dataset)
            phedexReqs = phedexClient.getCustodialSubscriptionRequestSite(dataset)
            transPerc = None
            duplicate = None
            correctLumis = None
            #Check first % events
            if percentage >= float(0.95):
                #if correct %, check duplicate and correct lumis
                duplicate = dbs3Client.duplicateLumi(dataset)
                correctLumis = checkCorrectLumisEventGEN(dataset)
                if not duplicate and correctLumis:
                    #if dataset healthy, check for subscription
                    if phedexReqs:
                        closeOutDataset = True
                        #get transfer percentage
                        #transPerc = phedexClient.getTransferPercentage(url, dataset, phedexReqs[0])
                        #transPerc = int(transPerc*100)
                    else:
                        #add to the missing-site workflows
                        print "here!"
                        noSiteWorkflows.append(workflow)
            #if at least one dataset is not ready wf cannot be closed out
            closeOutWorkflow = closeOutWorkflow and closeOutDataset
            phedexReqs = ','.join(phedexReqs) if phedexReqs else str(phedexReqs)
            print '| %80s | %100s | %4s | %5s| %3s | %5s| %5s| ' % (workflow, dataset,str(int(percentage*100)),
                        duplicate, str(correctLumis), str(phedexReqs), closeOutDataset)
            #web output
            output.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'%
                (workflow, dataset,str(int(percentage*100)), duplicate, str(correctLumis), phedexReqs, closeOutDataset))
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
    print '| Request'+(' '*74)+'| OutputDataSet'+(' '*86)+'|%Compl|Dupl|Tran|Subscr|ClosOu|'
    print '-'*220
    output.write('<table border=1> <tr><th>Request</th><th>OutputDataSet</th><th>%Compl</th>'
                '<th>Subscr</th><th>Tran</th><th>Dupl</th><th>ClosOu</th></tr>')
    
    output.write('<tr><th colspan="7">ReReco </th></tr>')
    noSiteWorkflows = closeOutReRecoWorkflowsWeb(url, workflowsCompleted['ReReco'], output)
    workflowsCompleted['NoSite-ReReco'] = noSiteWorkflows
    
    output.write('<tr><th colspan="7">ReDigi </th></tr>')
    noSiteWorkflows = closeOutRedigiWorkflowsWeb(url, workflowsCompleted['ReDigi'], output)
    workflowsCompleted['NoSite-ReDigi'] = noSiteWorkflows

    output.write('<tr><th colspan="7">MonteCarlo </th></tr>')
    noSiteWorkflows = closeOutMonterCarloRequestsWeb(url, workflowsCompleted['MonteCarlo'], output)
    workflowsCompleted['NoSite-MonteCarlo'] = noSiteWorkflows
    
    output.write('<tr><th colspan="7">MonteCarloFromGEN </th></tr>')
    noSiteWorkflows = closeOutMonterCarloRequestsWeb(url, workflowsCompleted['MonteCarloFromGEN'], output)
    workflowsCompleted['NoSite-MonteCarloFromGEN'] = noSiteWorkflows
    
    output.write('<tr><th colspan="7">LHEStepZero </th></tr>')
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
re
if __name__ == "__main__":
	main()

