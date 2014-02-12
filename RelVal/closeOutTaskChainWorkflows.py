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

def getWorkflowJson(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
    r2=conn.getresponse()
    data = r2.read()
    s = json.loads(data)
    return s

def main():
    url='cmsweb.cern.ch'
    parser = optparse.OptionParser()
    parser.add_option('--test',action="store_true", help='Nothing is closed out. Only test if the workflows are ready to be closed out.',dest='test')
    parser.add_option('--verbose',action="store_true", help='Print out details about the number of events expected and produced.',dest='verbose')
    parser.add_option('--correct_env',action="store_true",dest='correct_env')
    (options,args) = parser.parse_args()

    command=""
    for arg in sys.argv:
        command=command+arg+" "

    if not options.correct_env:
        os.system("source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh; source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
        sys.exit(0)

    if len(args) != 1:
        print "Usage:"
        print "python closeOutTaskChainWorkflows.py [--test] [--verbose] <inputFile_containing_a_list_of_workflows>"
        sys.exit(0)

    inputFile=args[0]
    
    f = open(inputFile, 'r')

    closedOut = []
    nonClosedOut = []
    tooManyEventsOrLumis = []
    running = []
    for line in f:
        workflow = line.rstrip('\n')
        print "checking workflow " + workflow
        jsn = getWorkflowJson(url, workflow)
        if jsn['RequestType'] != 'TaskChain':
            print "workflow type is not TaskChain, exiting"
            sys.exit(0)
        outputDatasets=phedexSubscription.outputdatasetsWorkflow(url, workflow)

        if 'RequestNumEvents' in jsn['Task1'] and 'InputDataset' in jsn['Task1']:
            print "both RequestNumEvents and InputDataset in Task1, exiting"
            sys.exit(1)
        if 'RequestNumEvents' in jsn['Task1']:
            inputEvents = jsn['Task1']['RequestNumEvents']

            closeOut = True
            tooMany = False
            for dataset in outputDatasets:
                print "    checking dataset  " + dataset
                # we cannot calculate completion of ALCARECO samples
                if 'ALCARECO' in dataset:
                    continue
                outputEvents=0
                tries=0
                while outputEvents==0 and tries < 3:
                    outputEvents=dbsTest.getOutputEvents(dataset)
                    if outputEvents==0:
                        print "0 output lumis"
                        time.sleep(50)
                    tries=tries+1    

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
                tooManyEventsOrLumis.append(workflow)
                
        elif 'InputDataset' in jsn['Task1']:
            inputLumis=dbsTest.getInputLumis(jsn)

            closeOut = True
            tooMany = False
            for dataset in outputDatasets:
                print "    checking dataset  " + dataset
                # we cannot calculate completion of ALCARECO samples
                if 'ALCARECO' in dataset:
                    continue
                outputLumis=0
                tries=0
                while outputLumis==0 and tries < 3:
                    outputLumis=dbsTest.getOutputLumis(dataset)
                    if outputLumis==0:
                        print "0 output lumis"
                        time.sleep(50)
                    tries=tries+1    

                if options.verbose:        
                    print "        input lumis:  " + str(inputLumis)
                    print "        output lumis: " + str(outputLumis)
                
                if outputLumis == inputLumis:
                    pass
                elif outputLumis < inputLumis :
                    closeOut = False
                elif outputLumis > inputLumis :
                    closeOut = False
                    tooMany = True
                    break

            if closeOut:
                closedOut.append(workflow)
            else:
                nonClosedOut.append(workflow)
            if tooMany:
                tooManyEventsOrLumis.append(workflow)
        else:
            print "neither RequestNumEvents nor InputDataset in Task1, exiting"
            sys.exit(1)
        

    print '-------------------------------------------------------------------------------------------------------------------------------------'
    print '| Request                                                                                   | Closed-out? | Current status          |'
    print '-------------------------------------------------------------------------------------------------------------------------------------'

    for workflow in closedOut:
        jsn = getWorkflowJson(url, workflow)
        if jsn['RequestStatus'] == 'completed':
            if not options.test:
                closeOutTaskChainWorkflows(url, workflow)
        else:
            pass
        status = jsn['RequestStatus']
        print "%90s\tYES\t\t%s" % (workflow, status)
    for workflow in nonClosedOut:
        jsn = getWorkflowJson(url, workflow)
        status = jsn['RequestStatus']
        print "%90s\tNO\t\t%s" % (workflow, status)

    print '-------------------------------------------------------------------------------------------------------------------------------------'

    for workflow in tooManyEventsOrLumis:
        print "WARNING: One of the datasets produced by the workflow "+workflow+" contains MORE events or lumis than expected. Rerun with the --verbose flag to get more information."
    f.close
    sys.exit(0)

if __name__ == "__main__":
    main()
