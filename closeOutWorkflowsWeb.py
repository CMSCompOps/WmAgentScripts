#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest
import time, shutil, closeOutWorkflows_leg as closeOutWorkflows
from xml.dom.minidom import getDOMImplementation

outputfile = '/afs/cern.ch/user/j/jbadillo/www/closeout.html'
tempfile = '/afs/cern.ch/user/j/jbadillo/www/temp.html'

def closeOutReRecoWorkflows(url, workflows, output):
	output.write('<tr><th colspan="8">ReReco</th></tr>')
	noSiteWorkflows = []
	for workflow in workflows:
		if 'RelVal' in workflow:
			continue
    		if 'TEST' in workflow:
			continue
		datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
		closeOutWorkflow=True
		InputDataset=dbsTest.getInputDataSet(url, workflow)
		for dataset in datasets:
			duplicate=False
			closeOutDataset=True
			Percentage=closeOutWorkflows.PercentageCompletion(url, workflow, dataset)
			PhedexSubscription=closeOutWorkflows.testOutputDataset(dataset)
			closeOutDataset=False
			if Percentage==1 and PhedexSubscription and not duplicate:
				closeOutDataset=True
			else:
				closeOutDataset=False
				if Percentage == 1 and not PhedexSubscription:
					noSiteWorkflos.add(workflow)
			closeOutWorkflow=closeOutWorkflow and closeOutDataset
			print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), 100, duplicate, closeOutDataset)
			s = '<td> %s </td><td> %s </td><td> %s </td><td> %s</td><td> %s </td><td> %s</td><td></td><td> %s</td>' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), 100, duplicate, closeOutDataset)
			output.write('<tr>'+s+'</tr>')
		if closeOutWorkflow:
			phedexSubscription.closeOutWorkflow(url, workflow)
	print '----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'
	return noSiteWorkflows

def closeOutRedigiWorkflows(url, workflows, output):
	output.write('<tr><th colspan="8">ReDigi </th></tr>')
	noSiteWorkflows = []
	for workflow in workflows:
		#print workflow
		closeOutWorkflow=True
		InputDataset=dbsTest.getInputDataSet(url, workflow)
		datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
		for dataset in datasets:
			closeOutDataset=False
			Percentage=closeOutWorkflows.PercentageCompletion(url, workflow, dataset)
			PhedexSubscription=closeOutWorkflows.testOutputDataset(dataset)
			duplicate=True
			if PhedexSubscription!=False and Percentage>=float(0.95):
				duplicate=dbsTest.duplicateRunLumi(dataset)
			elif PhedexSubscription==False and Percentage>=float(0.95):
				noSiteWorkflows.append(workflow)
			if Percentage>=float(0.95) and PhedexSubscription and not duplicate:
				closeOutDataset=True
			else:
		 		closeOutDataset=False
			closeOutWorkflow=closeOutWorkflow and closeOutDataset
			print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), 100, duplicate, closeOutDataset)
			s = '<td> %s </td><td> %s </td><td> %s </td><td> %s</td><td> %s </td><td> %s</td><td></td><td> %s</td>' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), 100, duplicate, closeOutDataset)
			output.write('<tr>'+s+'</tr>')
		if closeOutWorkflow:
			phedexSubscription.closeOutWorkflow(url, workflow)
	print '----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'
	return noSiteWorkflows	

def closeOutMonterCarloRequests(url, workflows,output):
	output.write('<tr><th colspan="8">Monte Carlo</th></tr>')
	noSiteWorkflows = []
	for workflow in workflows:
		datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
		closeOutWorkflow=True
		if closeOutWorkflows.getRequestTeam(url, workflow)!='analysis':#If request is not in special queue
			for dataset in datasets:
				ClosePercentage=0.95
				#if 'SMS' in dataset:
				#	ClosePercentage=1
				closeOutDataset=True
				Percentage=closeOutWorkflows.PercentageCompletion(url, workflow, dataset)
				PhedexSubscription=closeOutWorkflows.CustodialMoveSubscriptionCreated(dataset)
				TransPercen=0
				closedBlocks=False
				if PhedexSubscription!=False:
					site=PhedexSubscription
					TransPercen=closeOutWorkflows.TransferPercentage(url, dataset, site)
				else:
					noSiteWorkflows.append(workflow)
				duplicate=True
				if PhedexSubscription!=False and Percentage>=float(0.9):
					duplicate=dbsTest.duplicateLumi(dataset)
					closedBlocks = dbsTest.hasAllBlocksClosed(dataset)
				if Percentage>=float(ClosePercentage) and PhedexSubscription!=False and not duplicate:
					closeOutDataset=True
				else:
		 			closeOutDataset=False
				closeOutWorkflow=closeOutWorkflow and closeOutDataset
				print '| %80s | %100s | %4s | %5s| %3s | %5s|%5s| %5s|' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), str(int(TransPercen*100)), duplicate, closedBlocks, closeOutDataset)
				s = '<td> %s </td><td> %s </td><td> %s </td><td> %s</td><td> %s </td><td> %s </td><td> %s </td><td> %s </td>' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), str(int(TransPercen*100)), duplicate, closedBlocks, closeOutDataset)
				output.write('<tr>'+s+'</tr>')
			if closeOutWorkflow:
				phedexSubscription.closeOutWorkflow(url, workflow)
	print '-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'  
	return noSiteWorkflows

def closeOutStep0Requests(url, workflows, output):
	output.write('<tr><th colspan="8">Step 0 </th></tr>')
	noSiteWorkflows = []
	for workflow in workflows:
		#print workflow
		datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
		closeOutWorkflow=True
		if closeOutWorkflows.getRequestTeam(url, workflow)!='analysis':#If request is not in special queue
			for dataset in datasets:
				closeOutDataset=False
				Percentage=closeOutWorkflows.PercentageCompletion(url, workflow, dataset)
				PhedexSubscription=closeOutWorkflows.CustodialMoveSubscriptionCreated(dataset)
				if PhedexSubscription!=False:
					site=PhedexSubscription
					TransPercen=closeOutWorkflows.TransferPercentage(url, dataset, site)
				else:
					noSiteWorkflows.append(workflow)
				duplicate=dbsTest.duplicateLumi(dataset)
				correctLumis=dbsTest.checkCorrectLumisEventGEN(dataset)
				if Percentage>=float(0.95) and PhedexSubscription!=False and not duplicate and correctLumis:
					closeOutDataset=True
				else:
		 			closeOutDataset=False
				closeOutWorkflow=closeOutWorkflow and closeOutDataset
				print '| %80s | %100s | %4s | %5s| %3s | %5s| %5s| ' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), str(correctLumis), duplicate, closeOutDataset)
				s = '<td> %s </td><td> %s </td><td> %s </td><td> %s</td><td> %s </td><td> %s</td><td></td><td> %s</td>' % (workflow, dataset,str(int(Percentage*100)), str(PhedexSubscription), str(correctLumis), duplicate, closeOutDataset)
				output.write('<tr>'+s+'</tr>')
			if closeOutWorkflow:
				phedexSubscription.closeOutWorkflow(url, workflow)	
	print '-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'
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
    requests=closeOutWorkflows.getOverviewRequestsWMStats(url)
    print "Classifying Requests"
    workflowsCompleted=closeOutWorkflows.classifyCompletedRequests(url, requests)
    #header
    writeHTMLHeader(output)
    print '-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'
    print '| Request                                                                          | OutputDataSet                                                                                        |%Compl|Subscr|Tran|Dupl|Blocks|ClosOu|'
    print '-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'
    output.write('<table border=1> <tr><th>Request</th><th>OutputDataSet</th><th>%Compl</th>'
                '<th>Subscr</th><th>Tran</th><th>Dupl</th><th>Blocks</th><th>ClosOu</th></tr>')
    noSiteWorkflows = closeOutReRecoWorkflows(url, workflowsCompleted['ReReco'], output)
    workflowsCompleted['NoSite-ReReco'] = noSiteWorkflows
    noSiteWorkflows = closeOutRedigiWorkflows(url, workflowsCompleted['ReDigi'], output)
    workflowsCompleted['NoSite-ReDigi'] = noSiteWorkflows
    noSiteWorkflows = closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarlo'], output)
    workflowsCompleted['NoSite-MonteCarlo'] = noSiteWorkflows
    noSiteWorkflows = closeOutMonterCarloRequests(url, workflowsCompleted['MonteCarloFromGEN'], output)
    workflowsCompleted['NoSite-MonteCarloFromGEN'] = noSiteWorkflows
    noSiteWorkflows = closeOutStep0Requests(url, workflowsCompleted['LHEStepZero'],output)
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

