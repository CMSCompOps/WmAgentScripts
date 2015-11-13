#!/usr/bin/env python
""" 
Reject a given list of workflows.
This can be used when input workflows are in status: assigened or assignment-approved
    input arg: Text file with list of workflows.
"""

import  sys
from optparse import OptionParser
import reqMgrClient
import dbs3Client as dbs3

def main():
    url='cmsweb.cern.ch'
    
    #Create option parser
    usage = "\n       python %prog [-f FILE_NAME | WORKFLOW_NAME ...]\n"
    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', help='Text file with a list of workflows', dest='file')
    parser.add_option('-i', '--invalidate', action='store_true', default=False,
                      help='Also invalidate output datasets on DBS', dest='invalidate')
    (options, args) = parser.parse_args()
    
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    elif args:
        wfs = args
    else:
        parser.error("Provide the workflow of a file of workflows")
        sys.exit(1)
    
    for wf in wfs:
        print "Rejecting workflow: " + wf
        reqMgrClient.rejectWorkflow(url, wf)
        print "Rejected"
        if options.invalidate:
            print "Invalidating datasets"
            datasets = reqMgrClient.outputdatasetsWorkflow(url, wf)
            for ds in datasets:
                print ds
                dbs3.setDatasetStatus(ds, 'INVALID', files=True)

        
if __name__ == "__main__":
    main()
