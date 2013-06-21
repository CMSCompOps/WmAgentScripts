#!/usr/bin/env python
import urllib, httplib, sys, re, os
import json
import optparse
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

def retrieveSchema(workflowName):
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    helper.load(specURL)
    schema = {}
    for (key, value) in helper.data.request.schema.dictionary_whole_tree_().iteritems():
        if key == 'ProdConfigCacheID':
            schema['ConfigCacheID'] = value
        elif key == 'ProcConfigCacheID':
            schema['ConfigCacheID'] = value
        elif key=='RequestSizeEvents':
            schema['RequestNumEvents'] = value
        elif value != None:
            schema[key] = value
    return schema

def printBase(workflow, request):
    #print request
    print "'RequestName':", request['RequestName']
    print "'RequestType':", request['RequestType']
    print "'Campaign':", request['Campaign']
    print "'RequestPriority':", request['RequestPriority']
    print "'CMSSWVersion':", request['CMSSWVersion']
    print "'ScramArch':", request['ScramArch']
    print "'GlobalTag':", request['GlobalTag']
    print "'SkimConfigs':", request['SkimConfigs']
#    print "'Memory':", request['Memory']
#    print "'SizePerEvent':", request['SizePerEvent']
#    print "'TimePerEvent':", request['TimePerEvent']

def printReReco(workflow, request):
    printException(request, 'EnableHarvesting')
    printException(request, 'IncludeParents')
    printException(request, 'Scenario')
    print "'InputDataset':", request['InputDataset']
    print "'BlockWhitelist':", request['BlockWhitelist']
    print "'BlockBlacklist':", request['BlockBlacklist']
    print "'RunWhitelist':", request['RunWhitelist']
    print "'RunBlacklist':", request['RunBlacklist']
    print ""

def printTaskChain(workflow, request):
    printException(request, 'MCPileup')
    printException(request, 'EnableHarvesting')
    printException(request, 'EnableDQMHarvest')
    printException(request, 'ProcScenario')
    printException(request, 'DQMUploadUrl')
    printException(request, 'DQMConfigCacheID')
    printException(request, 'AcquisitionEra')
#    print "'TaskChain':", request['TaskChain']
    x = 1
    while x <= request['TaskChain']:
        task = 'Task'+str(x)
        print task
        printTaskException(request, task, 'MCPileup')
        printTaskException(request, task, 'TaskName')
        printTaskException(request, task, 'InputDataset')
        printTaskException(request, task, 'PrimaryDataset')
        printTaskException(request, task, 'RequestNumEvents')
        printTaskException(request, task, 'GlobalTag')
        printTaskException(request, task, 'AcquisitionEra')
        printTaskException(request, task, 'RunWhitelist')
        printTaskException(request, task, 'SplittingArguments')
        printTaskException(request, task, 'InputTask')
        x += 1
#        print "'':", request['']
    print ""

def printException(request, keyDic):
    try:
        result = str(request[keyDic])
        print "'"+keyDic+"': "+result
    except KeyError:
        pass
        #print ""

def printTaskException(request, task, keyDic):
    try:
        result = request[task][keyDic] 
        print "  '%s': %r" % (keyDic, result) 
    except KeyError:
        pass
        #print ""

def printAll(workflow):
    request = retrieveSchema(workflow)
    for key,value in request.items():
        if key=='Group' or key=='Requestor' or key=='timeStamp' or key=='dashboardActivity' or key=='CouchDBName' or key=='unmergedLFNBase' or key=='RequestString' or key=='RequestorDN' or key=='RequestDate':
            continue
        if type(value) is dict:
            print "'%s':" % key
            for key2, value2 in request[key].items():
                print "  '%s': %r" % (key2, value2)
        else:
            print "'%s': %r" % (key, value)
    print ""

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-c', '--complete', action="store_true", help='Use it to print the whole request dictionary',dest='complete')
    (options,args) = parser.parse_args()

    if len(sys.argv) < 2:
        print "Usage:"
        print "python printRequest.py [-c|--complete] <workflow_Name>"
        sys.exit(0)

    if options.complete:
        workflow=(sys.argv[2])
        printAll(workflow)
        sys.exit(0)

    ### Getting the original dictionary
    workflow=(sys.argv[1])
    request = retrieveSchema(workflow)
    if request['RequestType'] == 'ReReco':
        printBase(workflow, request)
        printReReco(workflow, request)
    elif request['RequestType'] == 'TaskChain':
        printBase(workflow, request)
        printTaskChain(workflow, request)
    else:
        print "Invalid request type:", request['RequestType']
        sys.exit(0)

    sys.exit(0);
