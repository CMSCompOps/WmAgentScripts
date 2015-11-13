#!/usr/bin/env python
"""
Announc a given list of workflows. It will search for any Resubmission requests 
for which the given request is a parent and announce them too.
    input arg: Text file with list of workflows.
"""

import sys
import optparse
import reqMgrClient
url='cmsweb.cern.ch'

def main():
    
    parser = optparse.OptionParser()
    parser.add_option('-f', '--file', help='Text file', dest='file')
    (options, args) = parser.parse_args()
    
    if options.file:
        workflows = [wf.strip() for wf in open(options.file) if wf.strip()]
    elif len(args) >= 1:
        workflows = args
    else:
        parser.error("Provide the workflow names or a text file")
        sys.exit(0)
    
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
