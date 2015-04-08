#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, shutil, time, reqMgrClient
from xml.dom.minidom import getDOMImplementation
import closeOutWorkflows

outputfile = '/afs/cern.ch/user/j/jbadillo/www/undealt.html'
tempfile = '/afs/cern.ch/user/j/jbadillo/www/temp_undealt.html'

def classifyRunningRequests(url, requests):
    """
    Creates an index for running requests
    The key is the request string
    """
    workflows={}
    for request in requests:
        #name of the request
        name=request['id']
        if len(request['key'])<3:
            print request
            continue
        #status
        status=request['key'][1]

        #add to the index
        if status in ['running-closed', 'running-open', 'assigned','acquired','assignment-approved']:
            #if it has the same request string add to a list            
            reqString = getRequestString(name)
            if reqString not in workflows:
                workflows[reqString] = [name]
            else:
                workflows[reqString].append(name)
    return workflows

def filterUndealtWorkflows(workflowsCompleted, workflowsRunning, wfType):
    """
    Filter's workflows that have no acdc running
    """
    wfs = workflowsCompleted[wfType]
    result = []
    #check for everyone if it has one runnign with the same strng name
    for wf in wfs:
        reqString = getRequestString(wf)
        #check how many acdcs have
        #print wf
        if reqString in workflowsRunning:
            #print workflowsRunning[reqString]
            pass
        else:
            #print 'no acdcs running'
            #retrieve status to double validate
            status = reqMgrClient.getWorkflowStatus(url, wf)
            if status == 'completed':
                result.append(wf)
    return result

def writeHTMLHeader(output):
    output.write('<html>')
    output.write('<head>')
    output.write('<link rel="stylesheet" type="text/css" href="style.css" />')
    output.write('</head>')
    output.write('<body>')


import re
p = re.compile(r'[a-z_]+(?:ACDC_|Merge_|EXT_)*([a-zA-Z0-9_\-]+)')
p2 = re.compile(r'_\d{6}_[0-9_]+')


def getRequestString(request):
    """
    Extracts the request string from the request name
    """
    m = p2.search(request)
    if not m:
        print request, 'NOT MATCH!!!'
        return request
    s = m.group(0)    
    s = request.replace(s,'')
    m = p.match(s)
    if not m:
        print s
        return request
    return m.group(1)


def listWorkflows(workflows, output):
    for wf in workflows:
        print wf
        output.write('<tr><td>'+wf+'</td></tr>')
    output.write('<tr><td></td></tr>')

def main():
    output = open(tempfile,'w')
    url='cmsweb.cern.ch'
    print "Gathering Requests"
    requests=closeOutWorkflows.getOverviewRequestsWMStats(url)
    print "Classifying Requests"
    workflowsCompleted = closeOutWorkflows.classifyCompletedRequests(url, requests)
    workflowsRunning = classifyRunningRequests(url, requests)
    writeHTMLHeader(output)
    print "Getting no duplicated requests"
    print "Workflows that are completed, but don't have ACDC's"
    output.write("<table border=1> <tr><th>Workflows that are completed, but don't have ACDC's</th></tr>")

    undealtWFs = filterUndealtWorkflows(workflowsCompleted, workflowsRunning,'ReReco')
    print "---------------------------------------------------"
    print "ReReco's"
    print "---------------------------------------------------"
    listWorkflows(undealtWFs,output)

    undealtWFs = filterUndealtWorkflows(workflowsCompleted, workflowsRunning,'ReDigi')
    print "---------------------------------------------------"
    print "ReDigi's"
    print "---------------------------------------------------"
    output.write("<tr><th>ReDigi's</th></tr>")
    listWorkflows(undealtWFs,output)    

    undealtWFs = filterUndealtWorkflows(workflowsCompleted, workflowsRunning,'MonteCarloFromGEN')
    print "---------------------------------------------------"
    print "MonteCarloFromGEN"
    print "---------------------------------------------------"
    output.write("<tr><th>MonteCarloFromGEN</th></tr>")
    listWorkflows(undealtWFs,output)

    undealtWFs = filterUndealtWorkflows(workflowsCompleted, workflowsRunning,'MonteCarlo')
    print "---------------------------------------------------"
    print "MonteCarlo"
    print "---------------------------------------------------"
    output.write("<tr><th>MonteCarlo</th></tr>") 
    listWorkflows(undealtWFs,output)

    undealtWFs = filterUndealtWorkflows(workflowsCompleted, workflowsRunning,'LHEStepZero')
    print "---------------------------------------------------"
    print "LHEStepZero"
    print "---------------------------------------------------"
    output.write("<tr><th>LHEStepZero</th></tr>") 
    listWorkflows(undealtWFs,output)
    
    output.write('</table>')
    output.write('<p>Last update: '+time.strftime("%c")+' CERN time</p>')
    output.write('</body>')
    output.write('</html>')
    output.close()
    #copy from temp file
    shutil.copy(tempfile, outputfile)
    sys.exit(0);

if __name__ == "__main__":
	main()
"""wfs = open('wfs')
    for wf in wfs.readlines():
        wf = wf.strip()
        print wf, getRequestString(wf)
"""
