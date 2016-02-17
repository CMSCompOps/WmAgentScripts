#!/usr/bin/env python
"""
Reject a given list of workflows and then clone them.
This can be used when input workflows are in status: assigened or assignment-approved
Please use abortAndClone for workflows in status acquired or running open/closed
The cloned requests have a newly generated RequestName, 
new timestamp, RequestDate, however -everything- else is copied from the original request.
    input arg: Text file with list of workflows.
"""

import urllib2,urllib, httplib, sys, re, os
try:
    import json
except:
    import simplejson as json
import resubmit, reqMgrClient

def main():
    """
    Read the text file, for each workflow try:
    First reject it, then clone it.
    """
#     args=sys.argv[1:]
#     if not len(args)==1:
#         print "usage:rejectAndClone file.txt"
#         sys.exit(0)
#     url='cmsweb.cern.ch'
#     filename=args[0]
#     workflows = [wf.strip() for wf in open(filename).readlines() if wf.strip()]
    workflows = ["sryu_B2G-RunIIFall15DR76-Backfill-00733_00334_v0__160204_071600_4367"]
    url='cmsweb.cern.ch'
    for workflow in workflows:
        print "Rejecting workflow: " + workflow
        reqMgrClient.rejectWorkflow(url, workflow)
        print "Rejected. Now cloning workflow..."
        clone = resubmit.cloneWorkflow(workflow, "sryu", "DATAOPS")
        print "Cloned workflow: ",   clone
        print "Cloned workflow: " + clone
    sys.exit(0);

if __name__ == "__main__":
    main()
