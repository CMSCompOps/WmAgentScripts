#
# Gets the Input location of a given workflow, list
# of workflows or dataset. Useful for discarding stuck
# workflows due to unsubscribed or unstaged data.
# Author: Julian Badillo
# before using: - Load WmAgentScripts
#               - create your voms proxy
# Usage: python getInputLocation.py [WFNAME | -f FILE | -d DATASET]
#

import reqMgrClient
import phedexClient
import dbs3Client as dbsClient
import optparse
url = 'cmsweb.cern.ch'


def formatSize(size):
    """
    To human-readable format
    """
    if size >= 10**12:
        return "%.2f" % (float(size) / 10**12) + "T"
    if size >= 10**9:
        return "%.2f" % (float(size) / 10**9) + "G"
    if size >= 10**6:
        return "%.2f" % (float(size) / 10**12) + "M"
    if size >= 10**3:
        return "%.2f" % (float(size) / 10**12) + "K"
    return str(size) + " Bytes"


def printDsLocation(ds, clean=False, anyb=False):
    """
    Only printing
    """
    onlycomplete = not anyb
    sites = sorted(phedexClient.getBlockReplicaSites(ds, onlycomplete))
    print ds
    if onlycomplete:
        print "block replicas (only complete):"
    else:
        print "All block replicas"
    print ','.join(sites)

    # print subscriptions only when asked for full block
    if onlycomplete:
        sites = sorted(phedexClient.getSubscriptionSites(ds))
        print "subscriptions:"
        print ','.join(sites)

    # print in the clean ready-to-use format
    if clean:
        sites2 = []
        for s in sites:
            if '_MSS' in s or '_Export' in s or '_Buffer' in s:
                continue
            s = s.replace('_Disk', '')
            sites2.append(s)
        print ','.join(sites2)

    # and the size
    size = dbsClient.getDatasetSize(ds)
    print formatSize(size)

def getInputDataset(workflow):
    if 'InputDataset' in workflow.info:
        return workflow.info['InputDataset']
    elif workflow.type == 'TaskChain':
        task1 = workflow.info['Task1']
        if 'InputDataset' in task1:
            return task1['InputDataset']
    return None

def main():
    usage = 'python %prog [OPTIONS] [WORKFLOW]'
    parser = optparse.OptionParser(usage=usage)
    parser.add_option(
        '-f', '--file', help='Text file with several workflows', dest='file')
    parser.add_option(
        '-a', '--any', help='Any block replica', dest='anyb', action='store_true')
    parser.add_option(
        '-d', '--dataset', help='A single dataset', dest='dataset', action='store_true')
    parser.add_option('-p', '--pileup', action="store_true",
                      help='Look also for pileup location', dest='pileup')
    parser.add_option('-c', '--clean', help='Print ready to use site list',
                      dest='clean', action="store_true", default=False)
    (options, args) = parser.parse_args()

    # if file
    if options.file:
        ls = [l.strip() for l in open(options.file) if l.strip()]
    elif len(args) == 1:
        ls = [args[0]]
    else:
        parser.error("Provide the workflow of a file of workflows")

    for x in ls:
        # if dataset given
        if options.dataset:
            printDsLocation(x, options.clean, options.anyb)
        else:
            print x
            workflow = reqMgrClient.Workflow(x)
             
            ds = getInputDataset(workflow)
            if not ds:
                print x, "Has no input dataset"
                continue
            
            printDsLocation(ds, options.clean, options.anyb)
            # pile ups
            if options.dataset and 'MCPileup' in workflow.info:
                pu = workflow.info['MCPileup']
                print "Pile up:"
                printDsLocation(pu, options.clean, options.anyb)

if __name__ == '__main__':
    main()
