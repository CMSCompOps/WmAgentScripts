#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
from closeOutWorkflows import *

"""
    Runs Closeout script only with a selected list of workflows.
    This can be usefil when workflows get stuck
"""

def main():
    print "Getting requests from file"
    #get file from parameters
    wfsFile = open(sys.argv[1],'r')
    wfsList = [wf.strip() for wf in wfsFile.readlines()]
    url='cmsweb.cern.ch'
    print "Gathering Requests"
    requests=getOverviewRequestsWMStats(url)
    print "Classifying Requests"
    workflowsCompleted=classifyCompletedRequests(url, requests)
    #filter only requests that are in the file
    workflows={'ReDigi':[],'MonteCarloFromGEN':[],'MonteCarlo':[] , 'ReReco':[], 'LHEStepZero':[]}
    for key in workflowsCompleted:
        for wf in workflowsCompleted[key]:
            if wf in wfsList:
                workflows[key].append(wf)
    workflowsCompleted = workflows
    #print header    
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

