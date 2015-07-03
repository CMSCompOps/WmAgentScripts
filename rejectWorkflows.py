#!/usr/bin/env python
""" 
Reject a given list of workflows.
This can be used when input workflows are in status: assigened or assignment-approved
    input arg: Text file with list of workflows.
"""

import urllib2,urllib, httplib, sys, re, os
from optparse import OptionParser
try:
    import json
except:
    import simplejson as json
import reqMgrClient

def main():
    url='cmsweb.cern.ch'
    
    #Create option parser
    usage = "\n       python %prog [-f FILE_NAME | WORKFLOW_NAME ...]\n"
    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', help='Text file with a list of workflows', dest='file')
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
        
if __name__ == "__main__":
    main()
