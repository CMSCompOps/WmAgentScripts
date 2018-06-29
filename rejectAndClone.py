#!/usr/bin/env python
"""
    Reject a given list of workflows and then clone them. also
    INVALIDATES original dataset, since the clone will have increased
    version number
    This can be used when input workflows are in status: assigened or assignment-approved
    Please use abortAndClone for workflows in status acquired or running open/closed
    The cloned requests have a newly generated RequestName, 
    new timestamp, however -everything- else is copied from the original request.
        input arg: Text file with list of workflows.
"""

import sys
import os
import pwd
import resubmit, reqMgrClient
import dbs3Client as dbs3
from optparse import OptionParser


url = 'cmsweb.cern.ch'
def main():
    """
    Read the text file, for each workflow try:
    First abort it, then clone it.
    """
    usage = "\n       python %prog [options] [WORKFLOW_NAME] [USER GROUP]\n"\
            "WORKFLOW_NAME: if the list file is provided this should be empty\n"\
            "USER: the user for creating the clone, if empty it will\n"\
            "      use the OS user running the script\n"\
            "GROUP: the group for creating the clone, if empty it will\n"\
            "      use 'DATAOPS' by default"

    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', help='Text file of workflows to Reject and Clone', dest='file')
    (options, args) = parser.parse_args()
    
    # Check the arguments, get info from them
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
        if len(args) == 2:
            user = args[0]
            group = args[1]
        elif len(args) == 0:
            #get os username by default
            uinfo = pwd.getpwuid(os.getuid())
            user = uinfo.pw_name
            #group by default DATAOPS
            group = 'DATAOPS'
    else:
        if len(args) == 3:
            user = args[1]
            group = args[2]
        elif len(args) == 1:
            #get os username by default
            uinfo = pwd.getpwuid(os.getuid())
            user = uinfo.pw_name
            #group by default DATAOPS
            group = 'DATAOPS'
        else:
            parser.error("Provide the workflow of a file of workflows")
            sys.exit(1)
        #name of workflow
        wfs = [args[0]]

    for wf in wfs:
        #abort workflow
        print "Rejecting workflow: " + wf
        reqMgrClient.rejectWorkflow(url, wf)
        #invalidates datasets
        print "Invalidating datasets"
        datasets = reqMgrClient.outputdatasetsWorkflow(url, wf)
        for ds in datasets:
            print ds
            dbs3.setDatasetStatus(ds, 'INVALID', files=True)

        #clone workflow
        clone = resubmit.cloneWorkflow(wf, user, group)
    sys.exit(0);

if __name__ == "__main__":
    main()
