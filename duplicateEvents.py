#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest
from dbsTest import get_data


das_url='https://cmsweb.cern.ch'
def testEventCountWorkflow(url, workflow):
    inputEvents=0
    #inputEvents=inputEvents+dbsTest.getInputEvents(url, workflow)
    datasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
    duplicate = False
    for dataset in datasets:
        #outputEvents=dbsTest.getEventCountDataSet(das_url, dataset)
        #percentage=outputEvents/float(inputEvents)
        print 'workflow:',workflow        
        print 'dataset :',dataset		
        #if float(percentage)>float(1):
        #	print "Workflow: " + workflow+" duplicate events in outputdataset: "+dataset +" percentage: "+str(outputEvents/float(inputEvents)*100) +"%"
        if duplicateRunLumi(dataset):
            duplicate = True
            #TODO fast check
            return True
    return duplicate



def duplicateRunLumi(dataset):
    """
checks if output dataset has duplicate lumis
for every run.
"""
    RunlumisChecked={}
    duplicate=False
    query="file run lumi dataset="+dataset
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        preresult=result['data']
    #check ever file in dataset
    for filename in preresult:
        run=filename['run'][0]['run_number']
        filename2 = filename['file'][0]['name']       
        #add run if new
        if run not in RunlumisChecked:
            RunlumisChecked[run]={}
        newLumis=filename['lumi'][0]['number']
        #check every lumi on range
        for lumiRange in newLumis:
            newlumiRange=range(lumiRange[0], lumiRange[1]+1)
            for lumi in newlumiRange:
                #if already checked in the same run
                if lumi in RunlumisChecked[run]:
                    print 'lumi ', lumi,'duplicated in these files:'
                    print RunlumisChecked[run][lumi]
                    print filename2
                    duplicate = True
                    #TODO fast check
                    return True
                else:
                    RunlumisChecked[run][lumi] = filename2
    if not duplicate:
        print 'Not duplicate lumi found'
    return duplicate

def duplicateLumi(dataset):
    """
checks if output dataset has a duplicate lumi
"""
    #registry of lumis checked, better a set
    lumisChecked=set()
    #get dtaset info frm das
    query="file lumi dataset="+dataset
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        preresult=result['data']
    #check each file
    for filename in preresult:
        newLumis=filename['lumi'][0]['number']
        #for each file we check each lumi range.
        for lumiRange in newLumis:
            newlumiRange=[lumiRange[0]]
            if lumiRange[0]<lumiRange[1]:
                newlumiRange=range(lumiRange[0], lumiRange[1])
            #check each lumi, if its in the lumiset
            for lumi in newlumiRange:
                if lumi in lumisChecked:
                    print 'lumi ',lumi
                    return True
                else:
                    lumisChecked.add(lumi)
    return False



def main():
    args=sys.argv[1:]
    if not len(args)==2:
        print "usage file"
    filename=args[0]
    url='cmsweb.cern.ch'
    workflows=phedexSubscription.workflownamesfromFile(filename)
    duplicated = open(args[1],'w')
    for workflow in workflows:
        if testEventCountWorkflow(url, workflow):
            duplicated.write(workflow+'\n')
    duplicated.close()
    sys.exit(0);

if __name__ == "__main__":
	main()
