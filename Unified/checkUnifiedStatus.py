#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, reqmgr_url
import sys
import argparse
import getpass
username = getpass.getuser()

parser = argparse.ArgumentParser()
parser.add_argument('-w', type=str, action='store', help='The workflow name to check the status of')
parser.add_argument('-f', type=str, action='store', help='A file containing the list of workflows to check')
options = parser.parse_args()
url = reqmgr_url

if __name__ == "__main__":

    workflow = options.w
    filename = options.f

    if not workflow and not filename:
        print "Either a workflow name of a file name should be passed"

    else:
        if workflow:
            pass
        elif filename:
            wfs = [l.strip() for l in open(options.f) if l.strip()]

            for wf in wfs:
                workflow = session.query(Workflow).filter(Workflow.name.contains(wf)).all()[0]
                
                print str(wf) + " : " + workflow.status

        else:
            pass
