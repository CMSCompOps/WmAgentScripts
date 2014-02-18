#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import reqMgrClient

"""
    Calculates event progress percentage of a given workflow,
    taking into account the workflow type, and comparing
    input vs. output numbers of events.
    Should be used instead of dbsTest.py
   
"""



def percentageCompletion(url, workflow, outputDataset, verbose=False):
    """
    Calculates Percentage of completion for a given workflow
    taking a particular output dataset
    """
   
    inputEvents = reqMgrClient.getInputEvents(url, workflow)
    outputEvents = reqMgrClient.getOutputEvents(url, workflow, outputDataset)
    if inputEvents==0 or not inputEvents:
	    return 0
    if not outputEvents:
        outputEvents = 0  
    if verbose:
        print outputDataset
        print "Input events:", int(inputEvents)
        print "Output events:", int(outputEvents)

    percentage=100.0*outputEvents/float(inputEvents)
    return percentage

    
def main():
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:WorkflowPercentage.py workflowname"
        sys.exit(0)
    workflow=args[0]

    url='cmsweb.cern.ch'

    #retrieve the output datasets
    outputDataSets=reqMgrClient.outputdatasetsWorkflow(url, workflow)    

    for dataset in outputDataSets:
        perc = percentageCompletion(url, workflow, dataset, verbose=True)
        print dataset,"match:",perc,"%"

    sys.exit(0);

if __name__ == "__main__":
    main()

