#!/usr/bin/env python
"""
    Validates if a given workflow has duplicate events in its output
    datasets. That is if a lumi is present in more than one file.

"""

import json
import urllib2,urllib, httplib, sys, re, os
import dbs3Client, reqMgrClient


def testEventCountWorkflow(url, workflow, verbose=False):
    """
    Shows where the workflow hs duplicate events    
    """
    inputEvents = 0
    datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
    duplicate = False
    print 'workflow:',workflow
    #check e
    for dataset in datasets:
        print 'dataset :', dataset		
        #if dbs3Client.duplicateLumi(dataset, verbose):
        if dbs3Client.duplicateRunLumi(dataset, verbose, skipInvalid=True):
            duplicate = True
            #fast check, one dataset duplicated
            if not verbose:
                print 'Has duplicated lumis'
                return True
    if not duplicate:
        print "No duplicate found"
    return duplicate

def main():
    args=sys.argv[1:]
    verbose = False
    if not len(args)>=2:
        print "usage input_file output_file [options]"
        return

    if '-v' in args:
       verbose = True
 
    url = 'cmsweb.cern.ch'
    #read the file
    input_file = args[0]
    #strip carriage return, spaces and empty lines
    workflows = [wf.strip() for wf in open(input_file).readlines() if wf.strip()]
    output_file = open(args[1],'w')
    #print verbose
    for workflow in workflows:
        if testEventCountWorkflow(url, workflow, verbose):
            #save which workflows had duplicate lumis
            output_file.write(workflow+'\n')
    output_file.close()
    sys.exit(0);

if __name__ == "__main__":
	main()
