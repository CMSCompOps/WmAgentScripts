#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, dbsTest, phedexSubscription
from xml.dom.minidom import getDOMImplementation

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
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage: python closeOutTaskChainWorkflows.py <inputFile_containing_a_list_of_workflows>"
        sys.exit(0)
    inputFile=args[0]
    f = open(inputFile, 'r')

    print '-------------------------------------------------------------------------------------------------------------------------------------'
    print '| Request                                                                                   | Closed-out? | Current status          |'
    print '-------------------------------------------------------------------------------------------------------------------------------------'

    closedOut = []
    nonClosedOut = []
    running = []
    for line in f:
        workflow = line.rstrip('\n')
        outputDatasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
        inputEvents=dbsTest.getInputEvents(url, workflow)
        closeOut = True
        for dataset in outputDatasets:
            # we cannot calculate completion of ALCARECO samples
            if 'ALCARECO' in dataset:
                continue
            outputEvents=dbsTest.getOutputEvents(url, workflow, dataset)
            print "dataset = " + dataset
            print "inputEvents = " + str(inputEvents)
            print "outputEvents = " + str(outputEvents)
                                         
            if inputEvents!=0:
                if (outputEvents/float(inputEvents)*100) >= 100.0:
                    pass
                    #print dataset+" match: "+str(outputEvents/float(inputEvents)*100) +"%"
                else:
                    #print dataset + " it is less than 99.9999% completed, keeping it in the current status" 
                    closeOut = False
                    break
            else:
                print "Input Events 0"

        if closeOut:
            closedOut.append(workflow)
        else:
            nonClosedOut.append(workflow)

    for workflow in closedOut:
        status = getStatus(url, workflow)
        #if status == 'completed':
        #    closeOutTaskChainWorkflows(url, workflow)
        #else:
        #    pass
        print "%90s\tYES\t\t%s" % (workflow, status)
    for workflow in nonClosedOut:
        status = getStatus(url, workflow)
        print "%90s\tNO\t\t%s" % (workflow, status)
    f.close
    print '-------------------------------------------------------------------------------------------------------------------------------------'
    sys.exit(0)

if __name__ == "__main__":
    main()
