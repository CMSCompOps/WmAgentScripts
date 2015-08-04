#!/usr/bin/env python
"""
    Force-complete a list of workflows.
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
    force-complete it.
    """
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:forceCompleteWorkflows file.txt"
        sys.exit(0)
    url='cmsweb.cern.ch'
    filename=args[0]
    workflows = [wf.strip() for wf in open(filename).readlines() if wf.strip()]
    for workflow in workflows:
        print "Force-Completing workflow: " + workflow
        result = reqMgrClient.forceCompleteWorkflow(url, workflow)
        if result == None or result == 'None':
            print "Error"
        else :
            print "force-complete"
          
    sys.exit(0);

if __name__ == "__main__":
    main()
