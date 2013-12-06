#!/usr/bin/env python
import optparse
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription
from xml.dom.minidom import getDOMImplementation
sys.path.append("..")
import dbsTest
import time



def closeOutTaskChainWorkflows(url, workflow):
    #for workflow in workflows:
    #print workflow + " can be closed-out"
    phedexSubscription.closeOutWorkflow(url, workflow)

def getStatus(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
    r2=conn.getresponse()
    data = r2.read()
    s = json.loads(data)
    t = s['RequestStatus']
    return t

def main():
    url='cmsweb.cern.ch'
    parser = optparse.OptionParser()
    parser.add_option('--test',action="store_true", help='Nothing is closed out. Only test if the workflows are ready to be closed out.',dest='test')
    parser.add_option('--verbose',action="store_true", help='Print out details about the number of events expected and produced.',dest='verbose')
    (options,args) = parser.parse_args()

    if len(args) != 1:
        print "Usage:"
        print "python closeOutTaskChainWorkflows.py [--test] [--verbose] <inputFile_containing_a_list_of_workflows>"
        sys.exit(0)

    inputFile=args[0]
    
    f = open(inputFile, 'r')

    closedOut = []
    nonClosedOut = []
    tooManyEvents = []
    running = []
    for line in f:
        workflow = line.rstrip('\n')
        print "checking workflow " + workflow
        outputDatasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
        inputEvents=0
        while inputEvents==0:
            inputEvents=dbsTest.getInputEvents(url, workflow)
            if inputEvents==0:
                print "failed das query"
                time.sleep(100)
        closeOut = True
        tooMany = False
        for dataset in outputDatasets:
            print "    checking dataset  " + dataset
            # we cannot calculate completion of ALCARECO samples
            if 'ALCARECO' in dataset:
                continue
            outputEvents=0
            while outputEvents==0:
                outputEvents=dbsTest.getOutputEvents(url, workflow, dataset)
                if outputEvents==0:
                    print "failed das query"
                    time.sleep(100)

            if options.verbose:        
                print "        input events:  " + str(inputEvents)
                print "        output events: " + str(outputEvents)
                
            if outputEvents == inputEvents:
                pass
            elif outputEvents < inputEvents :
                closeOut = False
            elif outputEvents > inputEvents :
                closeOut = False
                tooMany = True
                break

        if closeOut:
            closedOut.append(workflow)
        else:
            nonClosedOut.append(workflow)
        if tooMany:
            tooManyEvents.append(workflow)

    print '-------------------------------------------------------------------------------------------------------------------------------------'
    print '| Request                                                                                   | Closed-out? | Current status          |'
    print '-------------------------------------------------------------------------------------------------------------------------------------'

    for workflow in closedOut:
        status = getStatus(url, workflow)
        if status == 'completed':
            if not options.test:
                closeOutTaskChainWorkflows(url, workflow)
        else:
            pass
        print "%90s\tYES\t\t%s" % (workflow, status)
    for workflow in nonClosedOut:
        status = getStatus(url, workflow)
        print "%90s\tNO\t\t%s" % (workflow, status)

    print '-------------------------------------------------------------------------------------------------------------------------------------'

    for workflow in tooManyEvents:
        outputDatasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
        inputEvents=dbsTest.getInputEvents(url, workflow)
        while inputEvents==0:
            inputEvents=dbsTest.getInputEvents(url, workflow)
            time.sleep(1)
        while outputEvents==0:
            outputEvents=dbsTest.getOutputEvents(url, workflow, dataset)
            time.sleep(1)
        for dataset in outputDatasets:
        # we cannot calculate completion of ALCARECO samples
            if 'ALCARECO' in dataset:
                continue
            outputEvents=dbsTest.getOutputEvents(url, workflow, dataset)
            if inputEvents!=0:
                if outputEvents > inputEvents:
                    print "WARNING about workflow " + workflow  + ": The dataset " + dataset + " contains MORE events than expected. " + str(inputEvents) + " events were expected and " + str(outputEvents) + " were found."
    f.close
    sys.exit(0)

if __name__ == "__main__":
    main()
