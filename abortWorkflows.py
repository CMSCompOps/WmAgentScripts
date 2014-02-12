#!/usr/bin/env python
"""
Abort a given list of workflows
This can be used when input workflows are in status: acquired or running open/closed
    input arg: Text file with list of workflows.
"""

import urllib2,urllib, httplib, sys, re, os
try:
    import json
except:
    import simplejson as json
import reqMgrClient

def main():
    """
    Read the text file, for each workflow try:
    Abort it.
    """
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:abortWorkflows file.txt"
        sys.exit(0)
    url='cmsweb.cern.ch'
    filename=args[0]
    workflows = [wf.strip() for wf in open(filename).readlines() if wf.strip()]
    for workflow in workflows:
        print "Aborting workflow: " + workflow
        reqMgrClient.abortWorkflow(url, workflow)
        print "Aborted"
    sys.exit(0);

if __name__ == "__main__":
    main()
