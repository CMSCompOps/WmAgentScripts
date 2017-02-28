#!/usr/bin/env python

from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.Database.CMSCouch import Database
from WMCore.GroupUser.User import makeUser
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from pprint import pprint

import json
import logging
import pickle
import sys
import uuid

from optparse import OptionParser

jsonBlob = { 
            "createRequest":
            {
             "RequestString": "RequestString-OVERRIDE-ME",
             "RequestPriority": 9e5,
             "TimePerEvent": 10,
             "PrepID": None,
             "Campaign": None,
             "OriginalRequestName": "",
             "InitialTaskPath" : "",
             "ACDCServer" : "",
             "ACDCDatabase" : "",
             "RequestType": "Resubmission",
             "Requestor": "",
             "Group": "",
             #"Memory": 2300,
             "SizePerEvent": 200,
             "CollectionName" : "",
             "IgnoredOutputModules" : []
            },
            "changeSplitting":
            {
             "DataProcessing" :
             {
              "SplittingAlgo" : "LumiBased",
              "lumis_per_job" : 1,
              "halt_job_on_file_boundaries" : "True"
             }
            },
            "assignRequest":
            {
             "SiteWhitelist": "SiteWhitelist-OVERRIDE-ME",
             "MergedLFNBase": "/store/data",
             "UnmergedLFNBase": "/store/unmerged",
             "AcquisitionEra": "AcquisitionEra-OVERRIDE-ME",
             "ProcessingVersion": 1,
             "MaxRSS": 2411724,
             "MaxVSize": 20411724,
             "Dashboard": "reprocessing",
             "Team": "Team--OVERRIDE-ME",
             "OpenRunningTimeout": 0
            }
}

def buildDatasetTree(workload):
    """
    _buildDatasetTree_

    Build a tree with named edges that represents the connections between
    datasets in the workload. Assuming a standard ReReco with two skims, the
    graph would look like this:

    root = 'RAW'
    graph = {'RAW' : ['RECO', 'AOD', 'DQM'],
             'RECO' : ['USERSkim1', 'UserSkim2']}
    edges = {('RAW', 'RECO') : {'task' : '/request/DataProcessing', 'outMod' : ['RECOoutput']},
             ('RAW', 'AOD') : {'task' : '/request/DataProcessing', 'outMod' : ['AODoutput']},
             ('RAW', 'DQM') : {'task' : '/request/DataProcessing', 'outMod' : ['DQMoutput']},
             ('RECO', 'USERSkim1') : {'task' : '/request/DataProcessing/Merge/Skim1',
                                      'outMod' : ['Skim1']}
             ('RECO', 'USERSkim2') : {'task' : '/request/DataProcessing/Merge/Skim2',
                                      'outMod' : ['Skim2']}}

    Only non-transient outputs are valid nodes in the graph, and the name on
    the edge represents the task and list of output modules
    to be reproduced to get between input and output.
    """
    root = workload.listInputDatasets()[0]
    graph, edges = traverseWorkloadForDatasets(workload)
    graph, edges = reduceGraph(graph, root, edges)
    return graph, edges

def reduceGraph(graph, root, edges):
    """
    _reduceGraph_

    Take a graph and edge definition, starting from the root
    remove the transient nodes and ammend the connections
    between the persistent nodes. This is returned in a new graph
    and edges object.
    """
    newGraph = {}
    newEdges = {}
    if root not in graph:
        return newGraph, newEdges
    for child in graph[root]:
        tempGraph, tempEdges = reduceGraph(graph, child, edges)
        newGraph.update(tempGraph)
        newEdges.update(tempEdges)
        if child.endswith('transient'):
            if root not in newGraph:
                newGraph[root] = []
            newGraph[root].extend(newGraph[child])
            nodes = newGraph[child]
            for node in nodes:
                newEdges[(root, node)] = {'task' : edges[(root, child)]['task'],
                                          'outMod' : []}
                newEdges[(root,node)]['outMod'].extend(edges[(root, child)]['outMod'])
                newEdges[(root,node)]['outMod'].extend(newEdges[(child, node)]['outMod'])
                del newEdges[(child,node)]
            del newGraph[child]
        else:
            if root not in newGraph:
                newGraph[root] = []
            newGraph[root].append(child)
            newEdges[(root,child)] = edges[(root, child)]
            if newEdges[(root,child)]['outMod'] == ['Merged']:
                newEdges[(root,child)]['outMod'] = []
    return newGraph, newEdges

def traverseWorkloadForDatasets(workload, initialTask = None,
                                roots = None):
    """
    _traverseWorkloadForDatasets_

    Go through the workload recursively and build a graph that links
    the different tasks with their outputs in a graph-like structure.
    """
    graph = {}
    edges = {}

    if initialTask:
        taskIterator = initialTask.childTaskIterator()
    else:
        taskIterator = workload.taskIterator()

    for task in taskIterator:
        taskChildren = {}
        inputRef = task.inputReference()
        for stepName in task.listAllStepNames():
            stepHelper = task.getStepHelper(stepName)
            if not getattr(stepHelper.data.output, "keep", True):
                continue

            if stepHelper.stepType() == "CMSSW" or \
                   stepHelper.stepType() == "MulticoreCMSSW":
                for outputModuleName in stepHelper.listOutputModules():
                    outputModule = stepHelper.getOutputModule(outputModuleName)
                    transient = getattr(outputModule, "transient", False)
                    outputDataset = "/%s/%s/%s" % (outputModule.primaryDataset,
                                                   outputModule.processedDataset,
                                                   outputModule.dataTier)
                    if transient or task.taskType() != "Merge":
                        outputDataset += "transient"
                    if roots is None:
                        root = task.getInputDatasetPath()
                    else:
                        root = roots[inputRef.outputModule]
                    if root not in graph:
                        graph[root] = []
                    graph[root].append(outputDataset)
                    edges[(root, outputDataset)] = {'task' : task.getPathName(),
                                                    'outMod' : [outputModuleName]}
                    taskChildren[outputModuleName] = outputDataset
        otherGraph, otherEdges = traverseWorkloadForDatasets(workload,
                                                             task,
                                                             taskChildren)
        graph.update(otherGraph)
        edges.update(otherEdges)
    return graph, edges

def getOutputModules(workload, initialTask = None):
    """
    _getOutputModules_

    Retrieve the list of all output modules in the workload.
    """
    outputModules = []
    if initialTask:
        taskIterator = initialTask.childTaskIterator()
    else:
        taskIterator = workload.taskIterator()

    for task in taskIterator:
        for stepName in task.listAllStepNames():
            stepHelper = task.getStepHelper(stepName)
            if not getattr(stepHelper.data.output, "keep", True):
                continue
            if stepHelper.stepType() == "CMSSW" or \
                   stepHelper.stepType() == "MulticoreCMSSW":
                for outputModuleName in stepHelper.listOutputModules():
                    if task.taskType() != "Merge":
                        outputModules.append(outputModuleName)
        otherOutputModules = getOutputModules(workload,
                                              task)
        outputModules.extend(otherOutputModules)
    return outputModules

def getFiles(datasetName, runBlacklist, runWhitelist, blockBlacklist,
             blockWhitelist, dbsUrl, fakeLocation=False):
    """
    _getFiles_

    Get the full information of a dataset including files, blocks, runs and lumis.
    Filter it using run and block white/black lists.

    It can receive and optional DBSUrl.
    """
    dbsReader = DBSReader(endpoint = dbsUrl)
    phedexReader = PhEDEx()
    siteDB = SiteDBJSON()

    files = {}
    outputDatasetParts = datasetName.split("/")
    print "dataset",datasetName,"parts",outputDatasetParts   
    try:
        #retrieve list of blocks from dataset
        blockNames = dbsReader.listFileBlocks(datasetName)
    except:
        raise RuntimeError("Dataset %s doesn't exist in given DBS instance" % datasetName)
    
    has_parent = False
    try:
        parents = dbsReader.listDatasetParents( datasetName )
        if parents: has_parent=True
    except:
        print "Dataset with no parent"
        pass

    #traverse each block
    for blockName in blockNames:
        #deal with white and black list.
        if blockBlacklist and blockName in blockBlacklist:
            continue
        if blockWhitelist and blockName not in blockWhitelist:
            continue
        
        #existing blocks in phedex
        replicaInfo = phedexReader.getReplicaInfoForBlocks(block = blockName,
                                                           subscribed = 'y')
        blockFiles = dbsReader.listFilesInBlock(blockName, lumis=True)
        #has_parent = dbsReader.listBlockParents(blockName)
        if has_parent:
            try:
                blockFileParents = dbsReader.listFilesInBlockWithParents(blockName)
            except:
                print blockName,"does not appear to have a parent, even though it should. Very suspicious"
                blockFileParents = dbsReader.listFilesInBlock(blockName)
        else:
            blockFileParents = dbsReader.listFilesInBlock(blockName)

        blockLocations = set()
        #load block locations
        if len(replicaInfo["phedex"]["block"]) > 0:
            for replica in replicaInfo["phedex"]["block"][0]["replica"]:
                node = replica["node"]
                cmsSites = siteDB.PNNtoPSN(node)
                blockLocations.add(node.replace("_MSS","_Disk"))
                if type(cmsSites) != list:
                    cmsSites = [cmsSites]
                # for cmsName in cmsSites:
                #     se = siteDB.cmsNametoSE(cmsName)
                #     blockLocations.update(se)
                #     logging.debug("cmsName %s mapped to se %s", cmsName, se)
                logging.info("PhEDEx node %s, cmsSites %s, blockLocations %s", node, cmsSites, blockLocations)

        # We cannot upload docs without location, so force it in case it's empty
        if not blockLocations:
            if fakeLocation:
                logging.info("\t\t %s\tno location", blockName)
                blockLocations.update([u'cmssrmdisk.fnal.gov', u'srm-eoscms.cern.ch'])
            elif not has_parent: ## this should be the source
                logging.info("Blockname: %s\tno location, ABORT", blockName)
                sys.exit(1)
        
        logging.info("Blockname: %s\tLocations: %s", blockName, blockLocations)
 
        #for each file on the block
        for blockFile in blockFiles:
            parentLFNs = []
            #populate parent information
            if blockFileParents and "ParentList" in blockFileParents[0]:
                for fileParent in blockFileParents[0]["ParentList"]:
                    parentLFNs.append(fileParent["LogicalFileName"])
            ## remove when https://github.com/dmwm/WMCore/issues/7128 gets fixed
            #elif not 'RAW' in blockName:
            #    print "no parent info"
            runInfo = {}
            #Lumis not included in file
            for lumiSection in blockFile["LumiList"]:
                if runBlacklist and lumiSection["RunNumber"] in runBlacklist:
                    continue
                if runWhitelist and lumiSection["RunNumber"] not in runWhitelist:
                    continue

                if lumiSection["RunNumber"] not in runInfo.keys():
                    runInfo[lumiSection["RunNumber"]] = []

                runInfo[lumiSection["RunNumber"]].append(lumiSection["LumiSectionNumber"])
            if len(runInfo.keys()) > 0:
                files[blockFile["LogicalFileName"]] = {"runs": runInfo,
                                                       "events": blockFile["NumberOfEvents"],
                                                       "size": blockFile["FileSize"],
                                                       "locations": list(blockLocations),
                                                       "parents": parentLFNs}
    return files

def diffDatasets(inputDataset, outputDataset):
    """
    _diffDatasets_

    Compare two datasets in terms of lumi section content.
    Return a dictionary containing the run/lumis that are in the input
    but not in the output.
    """
    
    inputRunInfo = {}
    #make a set of input run-lumis.
    for inputLFN in inputDataset:
        for inputRun in inputDataset[inputLFN]["runs"]:
            if inputRun not in inputRunInfo:
                inputRunInfo[inputRun] = set()  
            #if type is list or [list]
            if type(inputDataset[inputLFN]["runs"][inputRun][0]) is list:
                inputRunInfo[inputRun].update(inputDataset[inputLFN]["runs"][inputRun][0])
            elif type(inputDataset[inputLFN]["runs"][inputRun]) is list:
                inputRunInfo[inputRun].update(inputDataset[inputLFN]["runs"][inputRun])
            else:
                raise RuntimeError("Don't know what this is:"+str( inputDataset[inputLFN]["runs"][inputRun]))

    outputRunInfo = {}
    #make a set of input run-lumis. 
    for outputLFN in outputDataset:
        for outputRun in outputDataset[outputLFN]["runs"]:
            if outputRun not in outputRunInfo:
                outputRunInfo[outputRun] = set()
            #if type is list or [list]
            if type(outputDataset[outputLFN]["runs"][outputRun][0]) is list:
                outputRunInfo[outputRun].update(outputDataset[outputLFN]["runs"][outputRun][0])
            elif type(outputDataset[outputLFN]["runs"][outputRun]) is list:
                outputRunInfo[outputRun].update(outputDataset[outputLFN]["runs"][outputRun])
            else:
                raise RuntimeError("Don't know what this is:"+str( outputDataset[outputLFN]["runs"][outputRun]))
    
    diffRunInfo = {}
    #make set difference for each run
    for inputRun in inputRunInfo:
        if inputRun not in outputRunInfo:
            diffRunInfo[inputRun] = inputRunInfo[inputRun]
        else:
            diffLumis = inputRunInfo[inputRun] - outputRunInfo[inputRun]
            if diffLumis:
                diffRunInfo[inputRun] = diffLumis
    
    return diffRunInfo

def buildDifferenceMap(workload, datasetInformation):
    """
    _buildDifferenceMap_

    Check the output datasets and find which lumis are differing from the input
    so we may construct the appriopiate requests. It returns
    a dictionary structure with the output datasets as the key
    and the difference in lumis.
    """
    differences = {}
    inputDataset = workload.listInputDatasets()[0]
    logging.info("InputDataset : %s", inputDataset)
    logging.info("OutputDataset: %s", workload.listOutputDatasets())
    for dataset in workload.listOutputDatasets():
        difference = diffDatasets(datasetInformation[inputDataset], datasetInformation[dataset])
        if difference:
            differences[dataset] = difference
    return differences

def getRequestInformationAndWorkload(requestName, reqmgrUrl, centralRequestDBURL):
    """
    _getRequestInformationAndWorkload_

    Retrieve the request information for assignment
    and the full pickled workload.
    """
    wfDBReader = RequestDBReader(centralRequestDBURL, couchapp = "ReqMgr")
    result = wfDBReader.getRequestByNames(requestName,True)
    workloadDB = Database(result[requestName]['CouchWorkloadDBName'], result[requestName]['CouchURL'])
    workloadPickle = workloadDB.getAttachment(requestName, 'spec')
    spec = pickle.loads(workloadPickle)
    workload = WMWorkloadHelper(spec)
    return workload, result[requestName]

def defineRequests(workload, requestInfo,
                   acdcCouchUrl, acdcCouchDb,
                   requestor, group,
                   dbsUrl,
                   fakeLocation,
                   datasetInformation = None):
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
    outputModules = getOutputModules(workload)
    if datasetInformation is None:
        datasetInformation = {}
        logging.info("Loading DBS information for the datasets...")
        datasetInformation[inputDataset] = getFiles(inputDataset, runBlacklist, runWhitelist,
                                                    blockBlacklist, blockWhitelist, dbsUrl, fakeLocation=fakeLocation)
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
    # Define an object that will hold the potential requests
    requests = []
    logging.info("Now definining the required requests...")
    # First generate requests for the datasets with children, that way we can
    # shoot the requests with skims in single requests
    for dataset in differenceInformation.keys():
        if dataset not in nodes:
            continue
        datasetsToRecover = [dataset]
        diffedLumis = differenceInformation[dataset]
        taskToRecover = edges[(inputDataset, dataset)]['task']
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

        requestObject = {'task' : taskToRecover,
                         'input' : inputDataset,
                         'lumis' : intersectionDiff,
                         'outMod' : outputModulesToRecover,
                         'outputs' : datasetsToRecover}
        requests.append(requestObject)
    # Now go through all the output datasets, creating a single request for
    # each
    for dataset in differenceInformation:
        datasetsToRecover = [dataset]
        diffedLumis = differenceInformation[dataset]
        if (inputDataset, dataset) in edges:
            taskToRecover = edges[(inputDataset, dataset)]['task']
            outputModulesToRecover = edges[(inputDataset,dataset)]['outMod']
            parentDataset = inputDataset
        else:
            for parentDataset in nodes:
                if dataset in nodes[parentDataset]:
                    taskToRecover = edges[(parentDataset, dataset)]['task']
                    outputModulesToRecover = edges[(parentDataset,dataset)]['outMod']
                    break
        requestObject = {'task' : taskToRecover,
                         'input' : parentDataset,
                         'lumis' : diffedLumis,
                         'outMod' : outputModulesToRecover,
                         'outputs' : datasetsToRecover}
        requests.append(requestObject)

    logging.info("About to upload ACDC records to: %s/%s" % (acdcCouchUrl, acdcCouchDb))
    pprint(requests)
    # With the request objects we need to build ACDC records and
    # request JSONs
    for idx, requestObject in enumerate(requests):
        collectionName = '%s_%s' % (workload.name(), str(uuid.uuid1()))
        filesetName = requestObject['task']
        collection = CouchCollection(**{"url" : acdcCouchUrl,
                                      "database" : acdcCouchDb,
                                      "name" : collectionName})
        owner = makeUser(workload.getOwner()['group'], workload.getOwner()['name'], acdcCouchUrl, acdcCouchDb)
        collection.setOwner(owner)
        files = 0
        lumis = 0
        for lfn in datasetInformation[requestObject['input']]:
            fileInfo = datasetInformation[requestObject['input']][lfn]
            fileRuns = {}
            for run in fileInfo['runs']:
                if run in requestObject['lumis']:
                    for lumi in fileInfo['runs'][run][0]:
                        if lumi in requestObject['lumis'][run]:
                            if run not in fileRuns:
                                fileRuns[run] = []
                            fileRuns[run].append(lumi)
                            lumis += 1
            if fileRuns:
                files += 1
                fileset = CouchFileset(**{"url" : acdcCouchUrl,
                                        "database" : acdcCouchDb,
                                        "name" : filesetName})
                fileset.setCollection(collection)
                acdcRuns = []
                for run in fileRuns:
                    runObject = {}
                    runObject['run_number'] = int(run)
                    runObject['lumis'] = fileRuns[run]
                    acdcRuns.append(runObject)
                acdcFile = {"lfn" : lfn,
                            "first_event" : 0,
                            "last_event" : 0,
                            "checksums" : {},
                            "size" : fileInfo["size"],
                            "events" : fileInfo["events"],
                            "merged" : 1,
                            "parents" : fileInfo["parents"],
                            "locations" : fileInfo["locations"],
                            "runs" : acdcRuns
                            }
                fileset.makeFilelist({lfn : acdcFile})

        # Put the creation parameters
        creationDict = jsonBlob["createRequest"]
        creationDict["OriginalRequestName"] = str(workload.name())
        creationDict["InitialTaskPath"] = requestObject['task']
        creationDict["CollectionName"] = collectionName
        creationDict["IgnoredOutputModules"] = list(set(outputModules) - set(requestObject['outMod']))
        creationDict["ACDCServer"] = acdcCouchUrl
        creationDict["ACDCDatabase"] = acdcCouchDb
        creationDict["RequestString"] = "recovery-%d-%s" % (idx, workload.name()[:-18])
        creationDict["Requestor"] = requestor
        creationDict["Group"] = group
        creationDict["TimePerEvent"] = requestInfo['TimePerEvent']
        creationDict["Memory"] = requestInfo['Memory']
        creationDict["SizePerEvent"] = requestInfo['SizePerEvent']
        creationDict["PrepID"] = requestInfo.get('PrepID')
        creationDict["Campaign"] = requestInfo.get('Campaign')

        # Assign parameters
        assignDict = jsonBlob["assignRequest"]
        team = requestInfo['Teams'][0]
        processingString = requestInfo['ProcessingString']
        processingVersion = requestInfo['ProcessingVersion']
        acqEra = requestInfo['AcquisitionEra']
        mergedLFNBase = requestInfo['MergedLFNBase']
        unmergedLFNBase = requestInfo['UnmergedLFNBase']
        #processingString = workload.getProcessingString()
        #processingVersion = workload.getProcessingVersion()
        #acqEra = workload.getAcquisitionEra()
        #mergedLFNBase = workload.getMergedLFNBase()
        #unmergedLFNBase = workload.getUnmergedLFNBase()
        topTask = workload.getTopLevelTask()[0]
        siteWhitelist = topTask.siteWhitelist()
        assignDict["SiteWhitelist"] = siteWhitelist
        assignDict["MergedLFNBase"] = mergedLFNBase
        assignDict["UnmergedLFNBase"] = unmergedLFNBase
        assignDict["AcquisitionEra"] = acqEra
        assignDict["Team"] = team
        try:
            int(processingVersion)
            assignDict["ProcessingVersion"] = int(processingVersion)
            if processingString is not None and processingString != 'None':
                assignDict["ProcessingString"] = processingString
        except Exception:
            tokens = processingVersion.split('-')
            assignDict["ProcessingVersion"] = int(tokens[-1][1:])
            assignDict["ProcessingString"] = ('-').join(tokens[:-1])

        fileHandle = open('%s.json' % creationDict["RequestString"], 'w')
        json.dump(jsonBlob, fileHandle)
        fileHandle.close()
        logging.info("Created JSON %s for recovery of %s" % ('%s.json' % creationDict["RequestString"],
                                                             requestObject['outputs']))
        logging.info("This will recover %d lumis in %d files" % (lumis, files))

def main():
    """
    _main_

    Define the CLI options and perform the main script function
    """
    myOptParser = OptionParser()
    myOptParser.add_option("-r", "--requestName", dest = "requestName",
                           help = "Name of the request to recover")
    myOptParser.add_option("-q", "--requestor", dest = "requestor",
                           help = "Requestor for the Resubmission requests")
    myOptParser.add_option("-g", "--group", dest = "group",
                           help = "Group for the Resubmission requests")
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
                           default = "https://cmsweb.cern.ch/reqmgr2/data",
                           help = "URL to the request manager API")
    myOptParser.add_option("-c", "--centralRequestDBURL", dest = "centralRequestDBURL",
                           default = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache",
                           help = "URL to the centralRequestDBURL")
    myOptParser.add_option("-j", "--json", dest = "jsonFile",
                           help = "For testing only: JSON file with the dataset information already loaded")
    myOptParser.add_option("-v", "--verbose", dest = "verbose",
                           default = False, action = "store_true",
                           help = "Increase the level of verbosity for debug")
    myOptParser.add_option("-f", "--fake", dest = "fake",
                           default = False, action = "store_true",
                           help = "In case there is no block location, forces it to CERN and FNAL")
    val, _ = myOptParser.parse_args()

    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    if val.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    # First load the request
    if val.requestName is None:
        raise RuntimeError("Request name must be specified.")
    workload, requestInfo = getRequestInformationAndWorkload(val.requestName, val.reqMgrUrl,val.centralRequestDBURL)
    # If testing, then load the pre-fetched dataset info
    datasetInformation = None
    if val.jsonFile is not None:
        fileHandle = open(val.jsonFile, 'r')
        datasetInformation = json.load(fileHandle)
        fileHandle.close()
    if val.requestor is None or val.group is None:
        raise RuntimeError("Requestor and group must be specified.")
    # Define the requests
    defineRequests(workload, requestInfo, val.acdcUrl, val.acdcServer, val.requestor,
                   val.group, val.dbsUrl, val.fake, datasetInformation)

if __name__ == "__main__":
    sys.exit(main())
