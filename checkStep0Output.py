#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription, dbsTest, duplicateEventsGen, closeOutWorkflows

def main():
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:checkStep0Output workflowname"
        sys.exit(0)
    workflow=args[0]
    url='cmsweb.cern.ch'
    dataset=phedexSubscription.outputdatasetsWorkflow(url, workflow)[0]
    correctLumi=dbsTest.checkCorrectLumisEventGEN(dataset)
    if correctLumi:
	print "The workflow is correct"
    else:
	print "The output Dataset has at least one lumi with more than 300 events, please check it."
    sys.exit(0);

if __name__ == "__main__":
    main()
