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




def makeDeletionRequests(url, allDatasets, verbose=False):
    """
    make a single deletion request per bunch of datasets
    Filtering only the INVALID or DEPRECATED ones
    """
    
    size = 20
    deletionRequests = []
    #delete duplicates
    allDatasets = list(set(allDatasets))
    while allDatasets:
        datasets = allDatasets[:size]
        allDatasets = allDatasets[size:]
        
        #get the sites
        sites = set()
        #add all sites for all datasets
        dsToDelete = set()
        for ds in datasets:
            t = dbs.getDatasetStatus(ds)
            if verbose:
                print ds, 'is', t
            #filter by status
            if t == 'INVALID' or t == 'DEPRECATED':
                dsToDelete.add(ds)
            sites2 = phd.getBlockReplicaSites(ds, onlycomplete=False)
            for s in sites2:
                #ignore buffers
                if "Buffer" in s or "Export" in s:
                    continue
                sites.add(s)
            if verbose:
                print "available in", sites

        #create a single request
        if dsToDelete and sites:
            print "About to create a deletion request for"
            print '\n'.join(dsToDelete)
            print "To this sites:"
            print '\n'.join(sites)
            r = phd.makeDeletionRequest(url, list(sites), dsToDelete, "Invalid data, can be deleted")
            if ("phedex" in r and 
                    "request_created" in r["phedex"] and
                    "id" in r["phedex"]["request_created"]):
                reqid = r["phedex"]["request_created"]["id"]
                deletionRequests.append(reqid)
                if verbose:
                    print "Request created:", reqid
            else:
                print r
    return deletionRequests

def main():
    usage = "usage: %prog [options] workflow"
    parser = OptionParser(usage=usage)
    parser.add_option("-f","--file", dest="fileName", default=None,
                        help="Input file")
    parser.add_option("-v","--verbose",action="store_true", dest="verbose", default=False,
                        help="Show detailed info")
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

    print "Getting output from workflows"
    for wf in workflows:
        if options.verbose:
            print wf
        try:
            ds = rqmgr.outputdatasetsWorkflow(url, wf)
            datasets += ds
        except:
            print wf, "skipped"
    reqs = makeDeletionRequests(url, datasets, options.verbose)
    print "Deletion request made:"
    print '\n'.join(reqs)

if __name__ == "__main__":
    main()

