#!/usr/bin/env python

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset
import WMCore.ACDC.CollectionTypes as CollectionTypes
from WMCore.GroupUser.User import makeUser

from ReqMgrCmdLine import ReqMgrInterface

import sys
import pickle
import time
import os

def buildTaskMap(wmWorkloadHelper, initialTask = None):
    """
    _buildTaskMap_

    Recurse through all of the tasks in the spec and build dictionary that maps
    task names to output datasets and output modules.  Note that this will ignore
    merge tasks.  
    """
    taskMap = {}

    if initialTask:
        taskIterator = initialTask.childTaskIterator()
    else:
        taskIterator = wmWorkloadHelper.taskIterator()

    for task in taskIterator:
        if task.taskType() != "Merge":
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)
            
                if not getattr(stepHelper.data.output, "keep", True):
                    continue
            
                if stepHelper.stepType() == "CMSSW" or \
                       stepHelper.stepType() == "MulticoreCMSSW":
                    for outputModuleName in stepHelper.listOutputModules():
                        outputModule = stepHelper.getOutputModule(outputModuleName)
                        outputDataset = "/%s/%s/%s" % (outputModule.primaryDataset,
                                                       outputModule.processedDataset,
                                                   outputModule.dataTier)
                        if task.getPathName() not in taskMap.keys():
                            taskMap[task.getPathName()] = {}
                        if outputDataset not in taskMap[task.getPathName()]:
                            taskMap[task.getPathName()][outputModuleName] = outputDataset

        moreTasks = buildTaskMap(wmWorkloadHelper, task)
        for anotherTask in moreTasks.keys():
            if anotherTask not in taskMap.keys():
                taskMap[anotherTask] = {}
            for outputModuleName in moreTasks[anotherTask].keys():
                if outputModuleName not in taskMap[anotherTask]:
                    taskMap[anotherTask][outputModuleName] = moreTasks[anotherTask][outputModuleName]
                    
    return taskMap

def findInputDatasetForTask(wmWorkloadHelper, taskName):
    """
    _findInputDatasetForTask_

    Find the input dataset for the given task.  This returns a dict with the
    following keys:
      name
      runBlacklist
      runWhitelist
      blockBlacklist
      blockWhitelist
    """
    output = {"name": None, "runBlacklist": [], "runWhitelist": [],
              "blockBlacklist": [], "blockWhitelist": []}
    
    task = wmWorkloadHelper.getTaskByPath(taskName)
    inputRef = task.inputReference()

    if not (hasattr(inputRef, "inputStep") and hasattr(inputRef, "outputModule")):
        output["name"] = "/%s/%s/%s" % (inputRef.dataset.primary,
                                        inputRef.dataset.processed,
                                        inputRef.dataset.tier)
        output["runBlacklist"] = inputRef.dataset.runs.blacklist
        output["runWhitelist"] = inputRef.dataset.runs.whitelist
        output["blockBlacklist"] = inputRef.dataset.blocks.blacklist
        output["blockWhitelist"] = inputRef.dataset.blocks.whitelist
        return output

    inputDatasetName = None
    inputStep = inputRef.inputStep
    outputModule = inputRef.outputModule
    while True:
        inputTaskName = "/".join(inputStep.split("/")[:-1])
        inputTask = wmWorkloadHelper.getTaskByPath(inputTaskName)

        keptOutput = False
        for stepName in inputTask.listAllStepNames():
            stepHelper = inputTask.getStepHelper(stepName)
            if not getattr(stepHelper.data.output, "keep", True):
                continue

            if stepHelper.stepType() == "CMSSW" or \
                   stepHelper.stepType() == "MulticoreCMSSW":
                if outputModule in stepHelper.listOutputModules():
                    outputModuleSect = stepHelper.getOutputModule(outputModule)
                    inputDatasetName = "/%s/%s/%s" % (outputModuleSect.primaryDataset,
                                                      outputModuleSect.processedDataset,
                                                      outputModuleSect.dataTier)                    
                    break

        if inputDatasetName == None:
            parentInputRef = inputTask.inputReference()
            inputStep = parentInputRef.inputStep
            outputModule = parentInputRef.outputModule
        else:
            output["name"] = inputDatasetName
            return output

    return None

def getFiles(datasetName, runBlacklist, runWhitelist, blockBlacklist,
             blockWhitelist):
    """
    _getFiles_

    """
    dbsReader = DBSReader("https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet")
    phedexReader = PhEDEx()

    files = {}
    outputDatasetParts = datasetName.split("/")
    datasets = dbsReader.matchProcessedDatasets(outputDatasetParts[1],
                                                outputDatasetParts[3],
                                                outputDatasetParts[2])

    if len(datasets) == 0:
        return runInfo

    totalLumis = 0
    blockNames = dbsReader.listFileBlocks(datasetName)
    count = 0
    for blockName in blockNames:
        count += 1
        print "  %s" % blockName
        if blockName in blockBlacklist:
            continue
        if len(blockWhitelist) > 0 and blockName not in blockWhitelist:
            continue

        replicaInfo = phedexReader.getReplicaInfoForBlocks(block = blockName)
        block = dbsReader.listFilesInBlockWithParents(blockName)
        blockLocations = []
        if len(replicaInfo["phedex"]["block"]) > 0:
            for replica in replicaInfo["phedex"]["block"][0]["replica"]:
                if replica["se"] != None and replica["se"] not in blockLocations:
                    blockLocations.append(replica["se"])
        for blockFile in block:
            parentLFNs = []
            for fileParent in blockFile["ParentList"]:
                parentLFNs.append(fileParent["LogicalFileName"])
            runInfo = {}
            for lumiSection in blockFile["LumiList"]:
                if lumiSection["RunNumber"] in runBlacklist:
                    continue
                if len(runWhitelist) > 0 and lumiSection["RunNumber"] not in runWhitelist:
                    continue

                if lumiSection["RunNumber"] not in runInfo.keys():
                    runInfo[lumiSection["RunNumber"]] = []

                runInfo[lumiSection["RunNumber"]].append(lumiSection["LumiSectionNumber"])
                totalLumis += 1
            if len(runInfo.keys()) > 0:
                files[blockFile["LogicalFileName"]] = {"runs": runInfo, "events": blockFile["NumberOfEvents"],
                                                       "size": blockFile["FileSize"], "locations": blockLocations,
                                                       "parents": parentLFNs}
        #if count > 30:
        #    break
    print "Total lumis: %d" % totalLumis             
    return files

def diffDatasets(inputDataset, outputDataset):
    """
    _diffDatasets_

    """
    diffResult = {}
    outputRunInfo = {}
    inputRunInfo = {}
    diffRunInfo = {}
    duplicateLumiInfo = {}
    outputExtra = {}

    numOutputLumis = 0
    numInputLumis = 0
    numDiffLumis = 0

    totalLumis = 0
    for inputLFN in inputDataset.keys():
        for inputRun in inputDataset[inputLFN]["runs"].keys():
            if inputRun not in inputRunInfo.keys():
                inputRunInfo[inputRun] = []
            for inputLumi in inputDataset[inputLFN]["runs"][inputRun]:
                totalLumis += 1
                inputRunInfo[inputRun].append(inputLumi)

    print "Scanning the input dataset for duplicate lumis which will be excluded:"
    for inputRun in inputRunInfo.keys():
        uniqueLumis = set(inputRunInfo[inputRun])
        dupLumis = inputRunInfo[inputRun][:]        
        if len(inputRunInfo[inputRun]) != len(uniqueLumis):
            for uniqueLumi in uniqueLumis:
                dupLumis.remove(uniqueLumi)
                
            print "  %s:" % (inputRun)
            print "    %s" % dupLumis
            duplicateLumiInfo[inputRun] = dupLumis[:]
        else:
            duplicateLumiInfo[inputRun] = []

    outputLumis = 0
    for outputLFN in outputDataset.keys():
        for outputRun in outputDataset[outputLFN]["runs"].keys():
            if outputRun not in outputRunInfo.keys():
                outputRunInfo[outputRun] = []
            for outputLumi in outputDataset[outputLFN]["runs"][outputRun]:
                if outputLumi not in duplicateLumiInfo[outputRun]:
                    outputRunInfo[outputRun].append(outputLumi)
                    numOutputLumis += 1

    totalLumis = 0
    for inputLFN in inputDataset.keys():
        for inputRun in inputDataset[inputLFN]["runs"].keys():
            for inputLumi in inputDataset[inputLFN]["runs"][inputRun]:
                totalLumis += 1

    print "\nScanning for lumis in the output dataset that are not in the input:"
    for outputRun in outputRunInfo.keys():
        if outputRun not in outputExtra.keys():
            outputExtra[outputRun] = []
        for outputLumi in outputRunInfo[outputRun]:
            if outputLumi in inputRunInfo[outputRun]:
                continue
            if outputLumi in duplicateLumiInfo[outputRun]:
                continue
            outputExtra[outputRun].append(outputLumi)
            numOutputLumis -= 1

        if len(outputExtra[outputRun]) > 0:
            print "  %s:\n    %s" % (outputRun, outputExtra[outputRun])            

    skipped = 0
    for inputLFN in inputDataset.keys():
        diffedFile = {}
        diffedFile["last_event"] = 0
        diffedFile["first_event"] = 0
        diffedFile["parents"] = inputDataset[inputLFN]["parents"]
        diffedFile["checksums"] = {}
        diffedFile["merged"] = 0
        diffedFile["events"] = inputDataset[inputLFN]["events"]
        diffedFile["size"] = inputDataset[inputLFN]["size"]
        diffedFile["runs"] = []
        diffedFile["locations"] = inputDataset[inputLFN]["locations"]
        diffedFile["lfn"] = inputLFN
        for inputRun in inputDataset[inputLFN]["runs"].keys():
            for inputLumi in inputDataset[inputLFN]["runs"][inputRun]:
                if inputLumi in duplicateLumiInfo[inputRun]:
                    skipped += 1
                    continue
                numInputLumis += 1
                if inputRun not in outputRunInfo or inputLumi not in outputRunInfo[inputRun]:
                    if inputRun not in diffRunInfo.keys():
                        diffRunInfo[inputRun] = []
                    diffRunInfo[inputRun].append(inputLumi)
                    numDiffLumis += 1
                    for acdcRun in diffedFile["runs"]:
                        if acdcRun["run_number"] == inputRun:
                            acdcRun["lumis"].append(inputLumi)
                            break
                    else:
                        diffedFile["runs"].append({"run_number": inputRun, "lumis": [inputLumi]})

        if len(diffedFile["runs"]) > 0:
            diffResult[inputLFN] = diffedFile

    print "Input lumis: %s" % numInputLumis
    print "Output lumis: %s" % numOutputLumis
    print "Diff lumis: %s" % numDiffLumis
    print "Skipped: %s" % skipped

    if numInputLumis - numOutputLumis == numDiffLumis:
        print "  Numbers check out."
    else:
        print "  SOMETHING IS WRONG."
        sys.exit(-1)

    return diffResult

def getCouchUrl():
    """
    _getCouchUrl_

    """
    if "WMAGENT_SECRETS_LOCATION" not in os.environ.keys():
        print "WMAGENT_SECRETS_LOCATION needs to be set."
        sys.exit(-1)

    couchUser = None
    couchPass = None
    couchHost = None
    couchPort = None
    secretsHandle = open(os.environ["WMAGENT_SECRETS_LOCATION"])    
    for line in secretsHandle.readlines():
        (key, value) = line.split("=")
        value = value.rstrip("\n")
        if key == "COUCH_USER":
            couchUser = value
        elif key == "COUCH_PASS":
            couchPass = value
        elif key == "COUCH_HOST":
            couchHost = value
        elif key == "COUCH_PORT":
            couchPort = value

    return "http://%s:%s@%s:%s" % (couchUser, couchPass, couchHost, couchPort)        

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage:"
        print "recover.py <request name> <dataset name>"
        sys.exit(-1)

    requestName = sys.argv[1]
    outputDataset = sys.argv[2]

    reqInt = ReqMgrInterface("https://cmsweb.cern.ch", verbose = True)

    print "Retrieving spec for %s..." % requestName
    spec = pickle.loads(reqInt.retrieveSpec(requestName))
    wlHelper = WMWorkloadHelper(spec)

    print "Looking for task and output module names..."
    if outputDataset not in wlHelper.listOutputDatasets():
        print "Output dataset was not produced by this request."
        print "Output datasets in this request:"
        for outputDataset in wlHelper.listOutputDatasets():
            print "  %s" % outputDataset
        sys.exit(-1)

    taskMap = buildTaskMap(wlHelper)
    inputTaskName = None
    inputOutputModuleName = None
    for taskName in taskMap.keys():
        for outputModuleName in taskMap[taskName].keys():
            if taskMap[taskName][outputModuleName] == outputDataset:
                inputTaskName = taskName
                inputOutputModuleName = outputModuleName

    taskOutputModules = []
    for outputModuleName in taskMap[inputTaskName].keys():
        if outputModuleName != inputOutputModuleName:
            taskOutputModules.append(outputModuleName)

    if inputTaskName == None:            
        print "Unable to find task that produced dataset."
        print "Task map: %s" % taskMap
        sys.exit(-1)

    print "  Task name: %s" % inputTaskName
    print "  Output module name: %s" % inputOutputModuleName

    input = findInputDatasetForTask(wlHelper, inputTaskName)
    print "  Input dataset: %s" % input["name"]
    print "    Run whitelist: %s" % input["runWhitelist"]
    print "    Run blacklist: %s" % input["runBlacklist"]
    print "    Block whitelist: %s" % input["blockWhitelist"]
    print "    Block blacklist: %s" % input["blockBlacklist"]

    print "Querying DBS for blocks then files..." 
    inputFiles = getFiles(input["name"], input["runBlacklist"], input["runWhitelist"],
                          input["blockBlacklist"], input["blockWhitelist"])
    outputFiles = getFiles(outputDataset, [], [], [], [])

    print "Found %d input files, %d output files." % (len(inputFiles), len(outputFiles))
    diffFiles = diffDatasets(inputFiles, outputFiles)

    altCollectionName = "%s-recover-%s" % (str(int(time.time())), requestName)
    collection = CouchCollection(database = "acdcserver", url = "https://cmsweb-testbed.cern.ch/couchdb",
                                 name = altCollectionName,
                                 type = CollectionTypes.DataCollection)
    requestor = wlHelper.getOwner()["name"]
    group = wlHelper.getOwner()["group"]
    owner = makeUser(group, requestor, "https://cmsweb-testbed.cern.ch/couchdb",
                     "acdcserver")
    collection.setOwner(owner)
    fileset = CouchFileset(database = "acdcserver", url = "https://cmsweb-testbed.cern.ch/couchdb",
                           name = inputTaskName)
    collection.addFileset(fileset)
    fileset.makeFilelist(diffFiles)

    print "Original Request Name: %s" % (requestName)
    print "Initial Task Path: %s" % (inputTaskName)
    print "ACDC Server URL: %s" % ("https://cmsweb-testbed.cern.ch/couchdb")
    print "ACDC Database Name: acdcserver"
    print "Ignored Output Modules: %s" % (taskOutputModules)
    print "Alternative Collection Name: %s" % (altCollectionName)

    # Submit an ACDC request for the recovery
