#!/usr/bin/env python
"""
    Validates if a given workflow has duplicate events in its output
    datasets. That is if a lumi is present in more than one file.

"""

import sys
import optparse
import dbs3Client
import reqMgrClient


def duplicateLumisWorkflow(url, workflow, verbose=False):
    """
    Shows where the workflow hs duplicate events    
    """
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

def duplicateLumisDataset(url, dataset, verbose=False):
    print 'dataset :', dataset        
    #if dbs3Client.duplicateLumi(dataset, verbose):
    if dbs3Client.duplicateRunLumi(dataset, verbose, skipInvalid=True):
        #fast check, one dataset duplicated
        if not verbose:
            print 'Has duplicated lumis'
            return True
    else:
        print "No duplicate found"
    return False

def main():
    
    usage = "usage: %prog [options] [WORKFLOW]"
    parser = optparse.OptionParser(usage=usage)

    parser.add_option('-f', '--file', help='Text file with a list of wokflows.', dest='file')
    parser.add_option('-d', '--dataset', help='Analyse a given dataset instead of a workflow.', dest='dataset')
    parser.add_option('-v', '--verbose', help='Generates a printout of duplicated lumis', dest='verbose',
                      action='store_true', default=False)
    options, args = parser.parse_args()
    
    url = 'cmsweb.cern.ch'
   
    if options.file:
        workflows = [l.strip() for l in open(options.file) if l.strip()]
    elif args:
        workflows = args
    elif options.dataset:
        duplicateLumisDataset(url, options.dataset, options.verbose)
        sys.exit(0)
    else:
        parser.error("Provide workflows or datasets to analyse")
        sys.exit(0)
    
    for workflow in workflows:
        if duplicateLumisWorkflow(url, workflow, options.verbose):
            print workflow, "has duplicated lumis"
        else:
            print "No duplicate found"
    

if __name__ == "__main__":
    main()
