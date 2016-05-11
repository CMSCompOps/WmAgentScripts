#!/usr/bin/env python
"""
    __author__ = "Paola Rozo"
<<<<<<< 77af5a2f6c38937fe6412a64d5203d1052f5f53b
    __version__ = "0.2"
    __maintainer__ = "Paola Rozo"
    __email__ = "katherine.rozo@cern.ch"

    The script rejects or aborts a workflow or a set of workflows according to their status.
=======
    __version__ = "0.1"
    __maintainer__ = "Paola Rozo"
    __email__ = "katherine.rozo@cern.ch"
    __status__ = "Testing"

    The script rejects or aborts a workflow or a set of worflows according to their status.
>>>>>>> Merging the rejecting scripts(rejectWorkflows.py,abortWorkflows.py,rejectAndClone.py and abortAndClone.p) and the resubmiting ones (resubmit.py and extendWorflow.py)
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
<<<<<<< 77af5a2f6c38937fe6412a64d5203d1052f5f53b
    usage = "\n       python %prog [options] [WORKFLOW_NAME]\n" \
            "WORKFLOW_NAME: if the list file is provided this should be empty\n"

    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', help='Text file of workflows to Reject and Clone', dest='file')
    parser.add_option('-c', '--clone', help='Are the workflows going to be cloned? The default value is False',action="store_true", dest='clone', default=False)
    parser.add_option('-i', '--invalidate', help='Invalidate datasets? The default value is False',action="store_true", dest='invalidate', default=False)
    parser.add_option("-u", "--user", dest="user",help="The user for creating the clone, if empty it will use the OS user running the script")
    parser.add_option("-g", "--group", dest="group", default='DATAOPS',help="The group for creating the clone, if empty it will, use 'DATAOPS' by default")
=======
    usage = "\n       python %prog [options] [WORKFLOW_NAME] [USER GROUP]\n" \
            "WORKFLOW_NAME: if the list file is provided this should be empty\n" \
            "USER: the user for creating the clone, if empty it will\n" \
            "      use the OS user running the script\n" \
            "GROUP: the group for creating the clone, if empty it will\n" \
            "      use 'DATAOPS' by default"

    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', help='Text file of workflows to Reject and Clone', dest='file')
    parser.add_option('-c', '--clone', help='The worflows need to be cloned: values:true or false', dest='clone')
>>>>>>> Merging the rejecting scripts(rejectWorkflows.py,abortWorkflows.py,rejectAndClone.py and abortAndClone.p) and the resubmiting ones (resubmit.py and extendWorflow.py)
    (options, args) = parser.parse_args()

    # Check the arguments, get info from them
    if options.file:
<<<<<<< 77af5a2f6c38937fe6412a64d5203d1052f5f53b
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
            cloned = resubmit.cloneWorkflow(workflow, user, options.group)
=======
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
        # invalidates workflow
        workflow = reqMgrClient.Workflow(wf)
        reqMgrClient.invalidateWorkflow(url,wf,workflow.status)

        # invalidates datasets
        print "Invalidating datasets"
        datasets = reqMgrClient.outputdatasetsWorkflow(url, wf)
        for dataset in datasets:
            print dataset
            dbs3.setDatasetStatus(dataset, 'INVALID', files=True)

        # clone workflow
        if options.clone:
            print("Clonning workflow: "+ wf)
            cloned = resubmit.cloneWorkflow(wf, user, group)
<<<<<<< 279458288b7170f541f07d7fb5622ff5ab50411c
>>>>>>> Merging the rejecting scripts(rejectWorkflows.py,abortWorkflows.py,rejectAndClone.py and abortAndClone.p) and the resubmiting ones (resubmit.py and extendWorflow.py)
=======
        reqMgrClient.invalidateWorkflow()
>>>>>>> Changes were made to invalidate the workflows.
    sys.exit(0);

if __name__ == "__main__":
    main()
