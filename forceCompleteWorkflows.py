#!/usr/bin/env python
"""
    Force-complete a list of workflows.
"""
import sys
import reqMgrClient
import optparse
url = 'cmsweb.cern.ch'

def main():
    """
    Read the text file, for each workflow try:
    force-complete it.
    """
    parser = optparse.OptionParser("python %prog [WF1 WF2 ... | -f FILE]")
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
        print "Force-Completing workflow: " + workflow
        result = reqMgrClient.forceCompleteWorkflow(url, workflow)
        if result == None or result == 'None':
            print "Error"
        else:
            print "force-complete"

    sys.exit(0)

if __name__ == "__main__":
    main()
