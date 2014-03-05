#!/usr/bin/env python
"""
Abort a given list of workflows and then clone them, also
INVALIDATES original dataset, since the clone will have increased
version number
This can be used when input workflows are in status: acquired (or running open/closed)
Please use rejectAndClone for workflows in status assigened or assignment-approved
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
import RelVal.setDatasetStatusDBS3 as dbs3

dbs3_url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSWriter'
url = 'cmsweb.cern.ch'
def main():
    """
    Read the text file, for each workflow try:
    First abort it, then clone it.
    """
    args=sys.argv[1:]
    if not len(args)==3:
        print "usage:abortAndClone file.txt user group"
        sys.exit(0)
    filename = args[0]
    user = args[1]
    group = args[2]

    #reading workflow list
    workflows = [wf.strip() for wf in open(filename).readlines() if wf.strip()]
    for workflow in workflows:
        #abort workflow
        print "Aborting workflow: " + workflow
        reqMgrClient.abortWorkflow(url, workflow)
        print "Aborted. Now cloning workflow..."
            
        #invalidates datasets
        print "Invalidating datasets"
        
        datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        for dataset in datasets:
            print dataset
            dbs3.setStatusDBS3(dbs3_url, dataset, 'INVALID', None)

        #clone workflow
        clone = resubmit.cloneWorkflow(workflow, user, group)
        print "Cloned workflow: ",   clone
    sys.exit(0);

if __name__ == "__main__":
    main()
