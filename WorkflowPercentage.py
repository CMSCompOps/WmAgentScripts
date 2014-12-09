#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import reqMgrClient, dbs3Client
from optparse import OptionParser
"""
    Calculates event progress percentage of a given workflow,
    taking into account the workflow type, and comparing
    input vs. output numbers of events.
    Should be used instead of dbsTest.py
   
"""

def percentageCompletion(url, workflow, verbose=False, checkLumis=False, checkFilter=False):
    """
    Calculates Percentage of completion for a given workflow
    taking a particular output dataset
    if checkLumis is enabled, we get lumis instead.
    """
    #input events/lumis
    try:
        if checkLumis:
            inputEvents = int(reqMgrClient.getInputLumis(url, workflow.name))
        else:
            inputEvents = int(reqMgrClient.getInputEvents(url, workflow.name))
    except:
        #no input dataset
        inputEvents = 0        
    
    #filter Efficiency (only for events)
    if checkFilter and not checkLumis:
        filterEff = reqMgrClient.getFilterEfficiency(url, workflow.name)
        if not filterEff:
            filterEff = 1.0    
    else:
        filterEff = 1.0
    #datasets
    outputDataSets = reqMgrClient.outputdatasetsWorkflow(url, workflow.name)
    for dataset in outputDataSets:
        #output events/lumis
        if checkLumis:
            outputEvents = reqMgrClient.getOutputLumis(url, workflow.name, dataset)
        else:
            outputEvents = reqMgrClient.getOutputEvents(url, workflow.name, dataset)
        if not outputEvents:
            outputEvents = 0
        #calculate percentage
        if not inputEvents:
            perc = 0
        else:
            perc = 100.0*outputEvents/float(inputEvents)/filterEff
        #print results
        if verbose:
            print dataset
            print "Input %s: %d"%("lumis" if checkLumis else "events", int(inputEvents))
            print ("Output %s: %d (%s%%)"%("lumis" if checkLumis else "events", int(outputEvents), perc)+
                   ('(filter=%s)'%filterEff if checkFilter and not checkLumis else ''))
        else:
            print dataset, "%s%%"%perc

def percentageCompletion2StepMC(url, workflow, verbose=False, checkLumis=False):
    """
    Calculates percentage completion for a MonteCarlo
    with GEN and GEN-SIM output
    pdmvserv_SMP-Summer14Test2wmGENSIM-00002_00002_v0__140831_173202_4712
    """
    #input events/lumis
    try:
        if checkLumis:
            inputEvents = int(reqMgrClient.getInputLumis(url, workflow.name))
        else:
            inputEvents = int(reqMgrClient.getInputEvents(url, workflow.name))
    except:
        #no input dataset
        inputEvents = 0
    
    #filter Efficiency (only for events)
    if not checkLumis:
        filterEff = reqMgrClient.getFilterEfficiency(url, workflow.name)
        if not filterEff:
            filterEff = 1.0    
    else:
        filterEff = 1.0

    outputDataSets = reqMgrClient.outputdatasetsWorkflow(url, workflow.name)
    #set the GEN first
    if re.match('.*/GEN$', outputDataSets[1]):
        outputDataSets = [outputDataSets[1],outputDataSets[0]]
    #output events/lumis
    if checkLumis:
        outputEvents = [reqMgrClient.getOutputLumis(url, workflow.name, outputDataSets[0]),
                        reqMgrClient.getOutputLumis(url, workflow.name, outputDataSets[1])]
    else:
        outputEvents = [reqMgrClient.getOutputEvents(url, workflow.name, outputDataSets[0]),
                        reqMgrClient.getOutputEvents(url, workflow.name, outputDataSets[1])]
    if not inputEvents:
        perc = [100.0,100.0*outputEvents[1]/outputEvents[0]]
    else:
        perc = [100.0*outputEvents[0]/float(inputEvents),
                100.0*outputEvents[1]/float(inputEvents)/filterEff]
    #print results
    if verbose:
        print "Input %s: %d"%("lumis" if checkLumis else "events", int(inputEvents))
        print outputDataSets[0]
        print "Output %s: %d (%s%%)"%("lumis" if checkLumis else "events", int(outputEvents[0]), perc[0])
        print outputDataSets[1]
        print ("Output %s: %d (%s%%)"%("lumis" if checkLumis else "events", int(outputEvents[1]), perc[1])+
              ('(filter=%s)'%filterEff if not checkLumis else ''))
    else:
        print outputDataSets[0], "%s%%"%perc[0]
        print outputDataSets[1], "%s%%"%perc[1]

def percentageCompletionTaskChain(url, workflow, verbose=False, checkLumis=False):
    """
    Calculates a Percentage completion for a taskchain.
    Taking step/filter efficiency into account.
    pdmvserv_task_SUS-Summer12WMLHE-00004__v1_T_141003_120119_9755
    """
    if not checkLumis:
<<<<<<< HEAD
        inputEvents = reqMgrClient.getInputEvents(url, workflow)
    else:
        inputEvents = 0

    outputDataSets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
    if verbose:
        print "Input %s:"%("lumis" if checkLumis else "events"), int(inputEvents)
=======
        inputEvents = reqMgrClient.getInputEvents(url, workflow.name)
    else:
        inputEvents = 0

    outputDataSets = reqMgrClient.outputdatasetsWorkflow(url, workflow.name)
    
    print "Input events:", int(inputEvents)
>>>>>>> 09c7b7bd559866f2773f5a15718ebdc24b4dcb4d
    i = 1
    #if subtype doesn't come with the request, we decide based on dataset names
    fromGen = False
    if not re.match('.*/GEN$', outputDataSets[0]):
        fromGen = False
    elif (re.match('.*/GEN$', outputDataSets[0])
        and re.match('.*/GEN-SIM$', outputDataSets[1])):
        fromGen = True

    #task-chain 1 (without filterEff)
    if not fromGen:
        for dataset in outputDataSets:
<<<<<<< HEAD
                if not checkLumis:
                    outputEvents = reqMgrClient.getOutputEvents(url, workflow, dataset)
                else:
                    outputEvents = reqMgrClient.getOutputLumis(url, workflow, dataset)
                percentage = 100.0*outputEvents/float(inputEvents) if inputEvents > 0 else 0.0
                if verbose:
                    print dataset
                    print "Output events:", int(outputEvents), "(%.2f%%)"%percentage
                else:
                    print dataset, "%s%%"%percentage
=======
            if not checkLumis:
                outputEvents = reqMgrClient.getOutputEvents(url, workflow.name, dataset)
            else:
                outputEvents = reqMgrClient.getOutputLumis(url, workflow.name, dataset)
            percentage = 100.0*outputEvents/float(inputEvents) if inputEvents > 0 else 0.0
            if verbose:
                print dataset
                print "Output events:", int(outputEvents), "(%.2f%%)"%percentage
            else:
                print dataset, "%s%%"%percentage
>>>>>>> 09c7b7bd559866f2773f5a15718ebdc24b4dcb4d
    #task-chain 2 GEN, GEN-SIM, GEN-SIM-RAW, AODSIM, DQM
    else:
        i = 1
        for dataset in outputDataSets:
            if verbose:
                print dataset
            if not checkLumis:
<<<<<<< HEAD
                outputEvents = reqMgrClient.getOutputEvents(url, workflow, dataset)
            else:
                outputEvents = reqMgrClient.getOutputLumis(url, workflow, dataset)
            #GEN and GEN-SIM
            if 1<= i <= 2 and not checkLumis: 
                filterEff = reqMgrClient.getFilterEfficiency(url, workflow, 'Task%d'%i)
=======
                outputEvents = reqMgrClient.getOutputEvents(url, workflow.name, dataset)
            else:
                outputEvents = reqMgrClient.getOutputLumis(url, workflow.name, dataset)
            #GEN and GEN-SIM
            if 1<= i <= 2 and not checkLumis: 
                filterEff = reqMgrClient.getFilterEfficiency(url, workflow.name, 'Task%d'%i)
>>>>>>> 09c7b7bd559866f2773f5a15718ebdc24b4dcb4d
                #decrease filter eff
                inputEvents *= filterEff
                percentage = 100.0*outputEvents/float(inputEvents) if inputEvents > 0 else 0.0
                if verbose:
<<<<<<< HEAD
                    print "Output %s:"%("lumis" if checkLumis else "events"), int(outputEvents), "(%.2f%%)"%percentage, '(filter=%s)'%filterEff
=======
                    print "Output %s:"%("lumis" if checkLumis else "events"), int(outputEvents), "(%.2f%%)"%
>>>>>>> 09c7b7bd559866f2773f5a15718ebdc24b4dcb4d
            #GEN dataset with lumis
            elif i == 1 and checkLumis:
                if verbose:
                    print "Output %s:"%("lumis" if checkLumis else "events"), int(outputEvents)
                #we check the rest of datasets against the lumis on the GEN dataset
                inputEvents = outputEvents
            #Digi datasets
            else:
                percentage = 100.0*outputEvents/float(inputEvents) if inputEvents > 0 else 0.0
                if verbose:
<<<<<<< HEAD
                    print "Output %s:"%("lumis" if checkLumis else "events"), int(outputEvents), "(%.2f%%)"%percentage
=======
                    print "Output %s:"%("lumis" if checkLumis else "events"), int(outputEvents), "(%.2f%%)"%
>>>>>>> 09c7b7bd559866f2773f5a15718ebdc24b4dcb4d
            if not verbose:
                print dataset, "%s%%"%percentage
            i += 1

url = 'cmsweb.cern.ch'

def main():
    usage = "usage: %prog [options] workflow"
    parser = OptionParser(usage=usage)
    parser.add_option("-v","--verbose",action="store_true", dest="verbose", default=False,
                        help="Show detailed info")
    parser.add_option("-l","--lumis",action="store_true", dest="checkLumis", default=False,
                        help="Show lumis instead of events")
    parser.add_option("-f","--file", dest="fileName", default=None,
                        help="Input file")
    (options, args) = parser.parse_args()
    if len(args) != 1 and options.fileName is None:
        parser.error("Provide the workflow name or a file")
        sys.exit(1)
    if options.fileName is None:
        workflows = [args[0]]
    else:
        workflows = [l.strip() for l in open(options.fileName) if l.strip()]

    for wf in workflows:
        print wf
        workflow = reqMgrClient.Workflow(wf, url)        
        #by tyoe
        if workflow.type != 'TaskChain':
            #two step monte carlos (GEN and GEN-SIM)
            if workflow.type == 'MonteCarlo' and len(workflow.outputDatasets) == 2:
                percentageCompletion2StepMC(url, workflow, options.verbose, options.checkLumis)
            elif workflow.type == 'MonteCarloFromGEN':
                percentageCompletion(url, workflow, options.verbose, options.checkLumis, checkFilter=True)
            else:
                percentageCompletion(url, workflow, options.verbose, options.checkLumis)
        else:
            percentageCompletionTaskChain(url, workflow, options.verbose, options.checkLumis)

if __name__ == "__main__":
    main()

