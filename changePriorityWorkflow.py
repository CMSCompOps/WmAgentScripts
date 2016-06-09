#!/usr/bin/env python
"""
 Changes priority of a workflow or a list of workflows
"""
import sys
import optparse
import reqMgrClient

url = 'cmsweb.cern.ch'

def main():
    parser = optparse.OptionParser("Usage %prog [WF1 WF2 ... | -f FILE] PRIO")
    parser.add_option('-f', '--file', help='Text file',
                      dest='file'); options, args = parser.parse_args()

    if options.file:
        wfs = [l.strip() for l in open(options.file).readlines() if l.strip()]
    elif len(args) >= 2:
        # get workflow and priority
        wfs = args[:-1]
    else:
        parser.error("Provide workflow names and priority")
        sys.exit(0)
        
    priority = args[-1]
    # repeat for everyone
    for wf in wfs:
        reqMgrClient.changePriorityWorkflow(url, wf, priority)
    

if __name__ == "__main__":
    main()
