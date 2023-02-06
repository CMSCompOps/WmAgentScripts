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
import logging

def rejectWorkflow(wf, url, invalidate):
    import reqMgrClient
    import dbs3Client as dbs3
    import logging
    
    logging.info("Rejecting workflow: " + wf)
    reqMgrClient.rejectWorkflow(url, wf)
    logging.info("Rejected")
    if invalidate:
        logging.info("Invalidating datasets")
        datasets = reqMgrClient.outputdatasetsWorkflow(url, wf)
        for ds in datasets:
            logging.info(ds)
            dbs3.setDatasetStatus(ds, 'INVALID', files=True)

def main():
    url='cmsweb.cern.ch'
    
    #Create option parser
    usage = "\n       python %prog [-f FILE_NAME | WORKFLOW_NAME ...]\n"
    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', help='Text file with a list of workflows', dest='file')
    parser.add_option('-a', '--afile', help='Text file with output of autoACDC', dest='afile')
    parser.add_option('-i', '--invalidate', action='store_true', default=False,
                      help='Also invalidate output datasets on DBS', dest='invalidate')
    (options, args) = parser.parse_args()
    
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    elif options.afile:
        ins = [l.strip() for l in open(options.afile) if l.strip()]

        # select only ones w/ errors
        wfs = [w.split(", ")[-2] for w in ins if len(w.split(", ")) > 2]
    elif args:
        wfs = args
    else:
        parser.error("Provide the workflow of a file of workflows")
        sys.exit(1)
    
    for wf in wfs:
        rejectWorkflow(wf, url, options.invalidate)

if __name__ == "__main__":
    main()