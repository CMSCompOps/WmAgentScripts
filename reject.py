#!/usr/bin/env python
"""
    The script rejects or aborts a workflow or a set of worflows according to their status.
    The workflow can be cloned if the -c option is given
"""

import sys
import os
import pwd
import resubmit, reqMgrClient
import dbs3Client as dbs3
from optparse import OptionParser

url = 'cmsweb.cern.ch'


def main():
    usage = "\n       python %prog [options] [WORKFLOW_NAME] [USER GROUP]\n" \
            "WORKFLOW_NAME: if the list file is provided this should be empty\n" \
            "USER: the user for creating the clone, if empty it will\n" \
            "      use the OS user running the script\n" \
            "GROUP: the group for creating the clone, if empty it will\n" \
            "      use 'DATAOPS' by default"

    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', help='Text file of workflows to Reject and Clone', dest='file')
    parser.add_option('-c', '--clone', help='The worflows need to be cloned: values:true or false', dest='clone')
    (options, args) = parser.parse_args()

    # Check the arguments, get info from them
    print options.file
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
        if len(args) == 2:
            user = args[0]
            group = args[1]
        elif len(args) == 0:
            # get os username by default
            uinfo = pwd.getpwuid(os.getuid())
            user = uinfo.pw_name
            # group by default DATAOPS
            group = 'DATAOPS'
    else:
        if len(args) == 3:
            user = args[1]
            group = args[2]
        elif len(args) == 1:
            # get os username by default
            uinfo = pwd.getpwuid(os.getuid())
            user = uinfo.pw_name
            # group by default DATAOPS
            group = 'DATAOPS'
        else:
            parser.error("Provide the workflow of a file of workflows")
            sys.exit(1)
        # name of workflow
        wfs = [args[0]]

    for wf in wfs:
        workflow = reqMgrClient.Workflow(wf)
        if workflow.status == 'assigned' or workflow.status == 'assignment-approved':
            print "Rejecting workflow: " + wf
            reqMgrClient.rejectWorkflow(url, wf)
        elif workflow.status == 'acquired' or workflow.status == 'running-open' or workflow.status == 'running-close':
            print "Aborting workflow: " + wf
            reqMgrClient.abortWorkflow(url, wf)
        else:
            print "The workflow cannot be rejected or aborted."

        # invalidates datasets
        print "Invalidating datasets"
        datasets = reqMgrClient.outputdatasetsWorkflow(url, wf)
        for dataset in datasets:
            print dataset
            dbs3.setDatasetStatus(dataset, 'INVALID', files=True)

        # clone workflow
        if options.clone:
            print "Clonning workflow: "+ wf
            cloned = resubmit.cloneWorkflow(wf, user, group)
    sys.exit(0);

if __name__ == "__main__":
    main()
