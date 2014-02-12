#!/usr/bin/env python
"""
Abort a given list of workflows and then clone them.
This can be used when input workflows are in status: acquired (or running open/closed)
Please use rejectAndClone for workflows in status assigened or assignment-approved
The cloned requests have a newly generated RequestName, 
new timestamp, RequestDate, however -everything- else is copied from the original request.
    input arg: Text file with list of workflows.

NOTE: ProcessingVersion will NOT be increased with this script! 
You can use this to abort running-open/closed request but you have to manually increse 
ProcessingVersion before assign. 
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
    First abort it, then clone it.
    """
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:abortAndClone file.txt"
        sys.exit(0)
    url='cmsweb.cern.ch'
    filename=args[0]
    workflows = [wf.strip() for wf in open(filename).readlines() if wf.strip()]
    for workflow in workflows:
        print "Aborting workflow: " + workflow
        reqMgrClient.abortWorkflow(url, workflow)
        print "Aborted. Now cloning workflow..."
        data = reqMgrClient.cloneWorkflow(url, workflow)
        response_json = json.loads(data)
        clone = response_json.values()[0]['RequestName']
        print "Cloned workflow: " + clone
    sys.exit(0);

if __name__ == "__main__":
    main()
