#!/usr/bin/env python
"""
    __author__ = "Paola Rozo"
    __version__ = "0.2"
    __maintainer__ = "Paola Rozo"
    __email__ = "katherine.rozo@cern.ch"

    The script rejects or aborts a workflow or a set of workflows according to their status.
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
    usage = "\n       python %prog [options] [WORKFLOW_NAME]\n" \
            "WORKFLOW_NAME: if the list file is provided this should be empty\n"

    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', help='Text file of workflows to Reject and Clone', dest='file')
    parser.add_option('-c', '--clone', help='Are the workflows going to be cloned? The default value is False',action="store_true", dest='clone', default=False)
    parser.add_option('-i', '--invalidate', help='Invalidate datasets? The default value is False',action="store_true", dest='invalidate', default=False)
    parser.add_option("-u", "--user", dest="user",help="The user for creating the clone, if empty it will use the OS user running the script")
    parser.add_option("-g", "--group", dest="group", default='DATAOPS',help="The group for creating the clone, if empty it will, use 'DATAOPS' by default")
    parser.add_option("-m", "--memory", dest="memory", help="Set max memory for the clone. At assignment, this will be used to calculate maxRSS = memory*1024")

    (options, args) = parser.parse_args()

    # Check the arguments, get info from them
    if options.file:
        try:
            workflows = [l.strip() for l in open(options.file) if l.strip()]
        except:
            parser.error("Provide a valid file of workflows")
            sys.exit(1)
    elif len(args) >0:
        # name of workflow
        workflows = [args[0]]
    else:
        parser.error("Provide the workflow of a file of workflows")
        sys.exit(1)
 
    if not options.user:
        # get os username by default
        uinfo = pwd.getpwuid(os.getuid())
        user = uinfo.pw_name
    else:
        user = options.user

    for workflow in workflows:
        try:
            workflowInfo = reqMgrClient.Workflow(workflow)
        except:
            print("The workflow name: "+ workflow+" is  not valid.")
            continue
        # invalidates workflow
        print("Invalidating the workflow: "+ workflow)
        reqMgrClient.invalidateWorkflow(url,workflow,workflowInfo.status)

        # invalidates datasets
        if options.invalidate:
            print("Invalidating datasets")
            datasets = reqMgrClient.outputdatasetsWorkflow(url, workflow)
            for dataset in datasets:
                print(dataset)
                dbs3.setDatasetStatus(dataset, 'INVALID', files=True)

        # clones workflow
        if options.clone:
            print("Cloning workflow: "+ workflow)
            if options.memory:
                mem = float(options.memory)
            else:
                mem = workflowInfo.info["Memory"]
            cloned = resubmit.cloneWorkflow(workflow, user, options.group, memory=mem)
    sys.exit(0)


if __name__ == "__main__":
    main()
