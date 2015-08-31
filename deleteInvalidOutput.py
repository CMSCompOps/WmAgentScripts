import sys
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

def makeDeletionRequests(url, allDatasets, verbose=False, test=False):
    """
    make a single deletion request per bunch of datasets
    Filtering only the INVALID or DEPRECATED ones
    """
    
    #delete duplicates
    datasets = list(set(allDatasets))
    
    #group datasets by sites
    requests = {}
    for ds in datasets:
        try:
            t = dbs.getDatasetStatus(ds)
            if verbose:
                print ds, 'is', t
            #filter by status
            if t != 'INVALID' and t != 'DEPRECATED':
                continue
            sites = phd.getBlockReplicaSites(ds, onlycomplete=False)
            for s in sites:
                #ignore buffers
                if "Buffer" in s or "Export" in s:
                    continue
                if s not in requests:
                    requests[s] = []
                requests[s].append(ds)
            if verbose:
                print "available in", sites
        except Exception as e:
            print ds,e
    

    deletionRequests = []
    #for each site
    for s in sorted(requests.keys()):
        datasets = requests[s]
        print "site", s
        print "datasets to delete"
        print '\n'.join(datasets)
        if not test:
            r = phd.makeDeletionRequest(url, [s], datasets, "Invalid data, can be deleted")
            if ("phedex" in r and 
                    "request_created" in r["phedex"]):
                reqid = r["phedex"]["request_created"][0]["id"]
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
    parser.add_option("--test",action="store_true", dest="test", default=False,
                        help="Only test and console output (doesn't make the actual calls)")
    (options, args) = parser.parse_args()

    if len(args) != 1 and options.fileName is None:
        parser.error("Provide the workflow name or a file")
        sys.exit(1)
    if options.fileName is None:
        workflows = [args[0]]
    else:
        workflows = [l.strip() for l in open(options.fileName) if l.strip()]
    
    datasets = []

    print "Getting output from workflows"
    for wf in workflows:
        if options.verbose:
            print wf
        try:
            ds = rqmgr.outputdatasetsWorkflow(url, wf)
            datasets += ds
        except:
            print wf, "skipped"
    reqs = makeDeletionRequests(url, datasets, options.verbose, options.test)
    print "Deletion request made:"
    print '\n'.join(reqs)

if __name__ == "__main__":
    main()

