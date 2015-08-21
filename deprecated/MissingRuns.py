#!/usr/bin/env python

import urllib2, urllib, httplib, sys, re, os, json
from deprecated import phedexSubscription
from deprecated import dbsTest
from xml.dom.minidom import getDOMImplementation
from das_client import get_data
das_host='https://cmsweb.cern.ch'
#Return true if a run is not present in any of the output datasets of a request, false if it is present in at least one

def runsNotPresent(url, workflow):
    runWhitelist=deprecated.dbsTest.getRunWhitelist(url, workflow)
    newRunWhiteList=[]
    for run in runWhitelist:
	if not runNotinAllDatasets(url, run, workflow):
		newRunWhiteList.append(run)
		print run
    print newRunWhiteList

def runNotinAllDatasets(url, run, workflow):
    Datasets=deprecated.phedexSubscription.outputdatasetsWorkflow(url, workflow)
    InputDataset=deprecated.dbsTest.getInputDataSet(url, workflow)
    runInputDataset=runInDataset(url, run, InputDataset)
    if not runInputDataset:
	return True
    for dataset in Datasets:
	if runInDataset(url, run, dataset):#the run is in at least one of the output datasets
		return True
    return False


def runInDataset(url, run, dataset):
    query="file run="+str(run)+ " dataset="+dataset
    output = {}
    das_data = get_data(das_host,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
    	if len(result['data'])>0:
		return True
	else:
		return False


def main():
    url='cmsweb.cern.ch'
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:runsMissing workflowname"
        sys.exit(0);
    workflow=args[0]
    runsNotPresent(url, workflow)
    sys.exit(0);
if __name__ == "__main__":
    main()
