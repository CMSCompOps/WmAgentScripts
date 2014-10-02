#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import reqMgrClient, dbs3Client

"""
    Calculates event progress percentage of a given workflow,
    taking into account the workflow type, and comparing
    input vs. output numbers of events.
    Should be used instead of dbsTest.py
   
"""



def percentageCompletion(url, workflow, outputDataset, verbose=False, checkLumis=False):
    """
    Calculates Percentage of completion for a given workflow
    taking a particular output dataset
    if checkLumis is enabled, we get lumis instead.
    """
    if checkLumis:
        try:
            inputEvents = reqMgrClient.getInputLumis(url, workflow)
        except:
            #no input dataset
            inputEvents = 0        
        outputEvents = reqMgrClient.getOutputLumis(url, workflow, outputDataset)
    else:
        inputEvents = reqMgrClient.getInputEvents(url, workflow)
        outputEvents = reqMgrClient.getOutputEvents(url, workflow, outputDataset)

    if not outputEvents:
        outputEvents = 0  
    if not inputEvents:
        inputEvents = 0
    try:
        inputEvents = int(inputEvents)
    except:
        inputEvents = 0
    if verbose:
        print outputDataset
        print "Input", "lumis:" if checkLumis else "events:", int(inputEvents)
        print "Output", "lumis:" if checkLumis else "events:", int(outputEvents)

    percentage = 100.0*outputEvents/float(inputEvents) if inputEvents > 0 else 0.0
    return percentage

    
def main():
    args=sys.argv[1:]
    if not 1 <= len(args) <= 2:
        print "usage:WorkflowPercentage.py workflowname [-l]"
        sys.exit(0)
    workflow=args[0]
    checkLumis = False
    if len(args) == 2 and args[1] == '-l':
        checkLumis = True
    url='cmsweb.cern.ch'

    #retrieve the output datasets
    outputDataSets=reqMgrClient.outputdatasetsWorkflow(url, workflow)    

    for dataset in outputDataSets:
        perc = percentageCompletion(url, workflow, dataset, verbose=True, checkLumis=checkLumis)
        print dataset,"match:",perc,"%"

    sys.exit(0);

if __name__ == "__main__":
    main()

