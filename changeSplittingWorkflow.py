#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
from xml.dom.minidom import getDOMImplementation



def getWorkflowType(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	Requesttype=request['RequestType']
	return Requesttype


def changeSplittingWorkflow(url, workflow, split):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	RequestType=getWorkflowType(url,workflow)
	if RequestType=='MonteCarlo':
		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/Production", "splittingAlgo":"EventBased", "events_per_job":str(split), "timeout":""}
	elif RequestType=='ReDigi':
		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/StepOneProc", "splittingAlgo":"LumiBased", "lumis_per_job":str(split), "timeout":"", "include_parents":"False", "files_per_job":"",'halt_job_on_file_boundaries':'True','events_per_job':''}
	elif RequestType=='MonteCarloFromGEN':
		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/MonteCarloFromGEN", "splittingAlgo":"LumiBased", "lumis_per_job":str(split), "timeout":"", "include_parents":"False", "files_per_job":"",'halt_job_on_file_boundaries':'True'}
#        elif RequestType=='ReReco':
#		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/DataProcessing", "splittingAlgo":"FileBased", "files_per_job":str(split), "timeout":"", "include_parents":"False", 'halt_job_on_file_boundaries':'True','events_per_job':''}
#        elif RequestType=='ReReco':
#		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/DataProcessing", "splittingAlgo":"LumiBased", "lumis_per_job":str(split), "timeout":"", "include_parents":"False", "files_per_job":"",'halt_job_on_file_boundaries':'True','events_per_job':''}
        elif RequestType=='ReReco':
		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/DataProcessing", "splittingAlgo":"EventAwareLumiBased", "lumis_per_job":"", "include_parents":"False", "files_per_job":"",'halt_job_on_file_boundaries':'True','events_per_job':str(split), 'avg_events_per_job':str(split), 'max_events_per_lumi':10000, 'halt_job_on_file_boundaries_event_aware':"True"}
	elif RequestType=='Resubmission':
		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/DataProcessing", "splittingAlgo":"LumiBased", "lumis_per_job":str(split), "timeout":"", "include_parents":"False", "files_per_job":"",'halt_job_on_file_boundaries':'True','events_per_job':''}
#	elif RequestType=='Resubmission':
#		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/DataProcessing", "splittingAlgo":"EventAwareLumiBased", "lumis_per_job":"", "timeout":"", "include_parents":"False", "files_per_job":"",'halt_job_on_file_boundaries':'True','events_per_job':'', 'avg_events_per_job':str(split), 'max_events_per_lumi':10000}
    	headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
	encodedParams = urllib.urlencode(params)
    	conn.request("POST", "/reqmgr/view/handleSplittingPage", encodedParams, headers)
    	response = conn.getresponse()	
    	print response.status, response.reason
    	data = response.read()
    	print data
    	conn.close()


def main():
	args=sys.argv[1:]
	if not len(args)==2:
		print "usage: workflowname split"
		sys.exit(0)
	workflow=args[0]
	split=args[1]
	url='cmsweb.cern.ch'
	changeSplittingWorkflow(url, workflow, split)
	sys.exit(0);

if __name__ == "__main__":
	main()
