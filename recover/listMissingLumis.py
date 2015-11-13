#!/usr/bin/env python

import logging
import sys
from pprint import pprint
from recoverMissingLumis import getRequestInformationAndWorkload
from recoverMissingLumis import getFiles
from recoverMissingLumis import buildDatasetTree, buildDifferenceMap
from optparse import OptionParser

def getMissingLumiList(workload, requestInfo, dbsUrl):
    """
    _defineRequests_

    This is the ultimate function,
    it will create JSONs for the appropiate resubmission requests
    that can be feed into the reqmgr.py script and it will assemble
    acdc records that can be uploaded to the database.
    """
    # First retrieve the run and block lists and load
    # the information of all datasets
    logging.debug("Original request info:\n%s", requestInfo)
    topTask = workload.getTopLevelTask()[0]
    runWhitelist = topTask.inputRunWhitelist()
    runBlacklist = topTask.inputRunBlacklist()
    blockWhitelist = topTask.inputBlockWhitelist()
    blockBlacklist = topTask.inputBlockBlacklist()
    inputDataset = workload.listInputDatasets()[0]
    #outputModules = getOutputModules(workload)
    

    datasetInformation = {}
    logging.info("Loading DBS information for the datasets...")
    datasetInformation[inputDataset] = getFiles(inputDataset, runBlacklist, runWhitelist, blockBlacklist, blockWhitelist, dbsUrl)
    for dataset in workload.listOutputDatasets():
        datasetInformation[dataset] = getFiles(dataset, runBlacklist, runWhitelist, blockBlacklist, blockWhitelist, dbsUrl)
    logging.info("Finished loading DBS information for the datasets...")

    # Now get the information about the datasets and tasks
    nodes, edges = buildDatasetTree(workload)
    logging.info("Dataset tree built...")
    for k,v in nodes.items():
        logging.debug("%s : %s" % (k,v))
    for k,v in edges.items():
        logging.debug("%s : %s" % (k,v))
    # Load the difference information between input and outputs
    differenceInformation = buildDifferenceMap(workload, datasetInformation)
    logging.info("Difference map processed...")
    logging.debug("%s" % str(differenceInformation))

    logging.info("Now definining the required requests...")
    # First generate requests for the datasets with children, that way we can
    # shoot the requests with skims in single requests
    for dataset in differenceInformation.keys():
        if dataset not in nodes:
            continue
        datasetsToRecover = [dataset]
        diffedLumis = differenceInformation[dataset]
        outputModulesToRecover = edges[(inputDataset,dataset)]['outMod']
        intersectionDiff = {}
        for childDataset in nodes[dataset]:
            childDiffLumis = differenceInformation[childDataset]
            matchAvailable = False
            for run in diffedLumis:
                if run in childDiffLumis:
                    for lumi in diffedLumis[run]:
                        if lumi in childDiffLumis[run]:
                            matchAvailable = True
                            break
            if matchAvailable:
                outputModulesToRecover.extend(edges[(dataset, childDataset)]['outMod'])
                datasetsToRecover.append(childDataset)
            for run in diffedLumis:
                if run in childDiffLumis:
                    if run not in intersectionDiff:
                        intersectionDiff[run] = set()
                        intersectionDiff[run] = diffedLumis[run] & childDiffLumis[run]
                    else:
                        intersectionDiff[run] &= diffedLumis[run] & childDiffLumis[run]
                else:
                    intersectionDiff[run] = set()
        for run in intersectionDiff:
            if not intersectionDiff[run]:
                del intersectionDiff[run]
        if not intersectionDiff:
            # Can't create request for this dataset + children
            continue
        for run in intersectionDiff:
            for childDataset in nodes[dataset]:
                childDiffLumis = differenceInformation[childDataset]
                if run in childDiffLumis:
                    childDiffLumis[run] -= intersectionDiff[run]
                    if not childDiffLumis[run]:
                        del childDiffLumis[run]
            diffedLumis[run] -= intersectionDiff[run]
            if not diffedLumis[run]:
                del diffedLumis[run]
        if not diffedLumis:
            del differenceInformation[dataset]
        for childDataset in nodes[dataset]:
            if not differenceInformation[childDataset]:
                del differenceInformation[childDataset]

    # Now go through all the output datasets, creating a single request for
    # each
    return differenceInformation

def main():
    """
    _main_

    Define the CLI options and perform the main script function
    """
    myOptParser = OptionParser()
    myOptParser.add_option("-r", "--requestName", dest = "requestName",
                           help = "Name of the request to recover")
    myOptParser.add_option("-d", "--dbsUrl", dest = "dbsUrl",
                           default = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader",
                           help = "URL for the DBS instance to get the dataset info (both DBS2 and DBS3 work)")
    myOptParser.add_option("-a", "--acdcUrl", dest = "acdcUrl",
                           default = "https://cmsweb.cern.ch/couchdb",
                           help = "URL for the ACDC database where the records can be uploaded")
    myOptParser.add_option("-s", "--acdcServer", dest = "acdcServer",
                           default = "acdcserver",
                           help = "Database name for the ACDC records")
    myOptParser.add_option("-u", "--reqMgrUrl", dest = "reqMgrUrl",
                           default = "https://cmsweb.cern.ch/reqmgr/reqMgr",
                           help = "URL to the request manager API")
    myOptParser.add_option("-v", "--verbose", dest = "verbose",
                           default = False, action = "store_true",
                           help = "Increase the level of verbosity for debug")
    val, _ = myOptParser.parse_args()

    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    if val.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    # First load the request
    if val.requestName is None:
        raise RuntimeError("Request name must be specified.")
    workload, requestInfo = getRequestInformationAndWorkload(val.requestName, val.reqMgrUrl)

    # calculate lumis
    diffInfo = getMissingLumiList(workload, requestInfo, val.dbsUrl)
    for dataset, diffedLumis in diffInfo.items():
        print "For dataset:"
        print dataset
        print len(diffedLumis), "missing lumis"
        pprint(diffedLumis)
    

if __name__ == "__main__":
    sys.exit(main())
