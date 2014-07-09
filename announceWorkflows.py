#!/usr/bin/env python
"""
Announc a given list of workflows. It will search for any Resubmission requests 
for which the given request is a parent and announce them too.
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
    Announce it.
    """
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:announceWorkflows file.txt"
        sys.exit(0)
    url='cmsweb.cern.ch'
    filename=args[0]
    workflows = [wf.strip() for wf in open(filename).readlines() if wf.strip()]
    for workflow in workflows:
        print "Announcing workflow: " + workflow +". Look for resubmissions and announce them too"
        result=reqMgrClient.announceWorkflowCascade(url, workflow)
        if result==None or result == 'None':
          print "Announced"
        else:
          print "ERROR NOT ANNOUNCED"
          print result
          
    sys.exit(0);

if __name__ == "__main__":
    main()
