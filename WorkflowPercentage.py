#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest, duplicateEventsGen
from xml.dom.minidom import getDOMImplementation
import reqMgrClient





def percentageCompletion(url, workflow, outputDataset, verbose=False):
    """
    Calculates Percentage of completion for a given workflow
    taking a particular output dataset
    """
   
    inputEvents = reqMgrClient.getInputEvents(url, workflow)
    outputEvents = reqMgrClient.getOutputEvents(url, workflow, outputDataset)
    if inputEvents==0:
	    return 0    
    if verbose:
        print outputDataset
        print "Input events:", inputEvents
        print "Output events:", outputEvents
    if not inputEvents:
        inputEvents = 0
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
    outputDataSets=phedexSubscription.outputdatasetsWorkflow(url, workflow)    

    for dataset in outputDataSets:
        perc = percentageCompletion(url, workflow, dataset, verbose=True)
        print dataset,"match:",perc,"%"

    sys.exit(0);

if __name__ == "__main__":
    main()

