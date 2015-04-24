import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import reqMgrClient as rqmgr
import phedexClient as phd
import dbs3Client as dbs
from optparse import OptionParser
"""
    Basic script for deleting output from old workflows
    It only creates deletion requests if the dataset
    is either INVALID or DEPRECATED
"""

url = 'cmsweb.cern.ch'
url_tb =  'cmsweb-testbed.cern.ch'




def makeDeletionRequests(url, datasets):
    """
    make a single deletion request per bunch of datasets
    """
    
    size = 10
    while datasets:
        ds = datasets[:size]
        datasets = datasets[size:]
        #get the sites
        sites = set()
        #add all sites for all datasets
        dsToDelete = []
        for d in ds:
            t = dbs.getDatasetStatus(d)
            if t != 'INVALID' and t != 'DEPRECATED':
                print d, 'is', t
                continue
            sites2 = phd.getBlockReplicaSites(d, onlycomplete=False)
            for s in sites2:
                if "Buffer" in s or "Export" in s:
                    continue
                sites.add(s)
            dsToDelete.append(ds)
        #create a single request
        print "About to create a deletion request for"
        print dsToDelete
        print sites
        r = phd.makeDeletionRequest(url, list(sites), dsToDelete, "Invalid data, can be deleted")
        print r


def main():
    usage = "usage: %prog [options] workflow"
    parser = OptionParser(usage=usage)
    parser.add_option("-f","--file", dest="fileName", default=None,
                        help="Input file")
    (options, args) = parser.parse_args()

    if len(args) != 1 and options.fileName is None:
        parser.error("Provide the workflow name or a file")
        sys.exit(1)
    if options.fileName is None:
        workflows = [args[0]]
    else:
        workflows = [l.strip() for l in open(options.fileName) if l.strip()]
    
    datasets = []
    i = 0
    for wf in workflows:
        ds = rqmgr.outputdatasetsWorkflow(url, wf)
        datasets += ds
    
    makeDeletionRequests(url, datasets)


if __name__ == "__main__":
    main()

