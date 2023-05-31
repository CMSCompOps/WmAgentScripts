#!/usr/bin/env python

"""
    __modified__ = "Paola Rozo"
    __version__ = "1.1"
    __maintainer__ = "Paola Rozo"
    __email__ = "katherine.rozo@cern.ch"
    __status__ = "Testing"


    This script clones or extends a given workflow
    Usage:
        python resubmit.py [options] WORKFLOW_NAME
    Options:
        -a --action, decides to clone or extend a workflow
        -b --backfill, creates a clone
        -v --verbose, prints schemas and responses

    This script depends on WMCore code, so WMAgent environment,libraries and voms proxy need to be loaded before running it.
"""
import os
import pwd
import re
import sys
from optparse import OptionParser
from pprint import pprint

import dbs3Client

import reqMgrClient

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

DELTA_EVENTS = 1000
DELTA_LUMIS = 200

def modifySchema(cache, workflow, user, group, events, firstLumiNum, backfill=False, memory=None, timeperevent=None, filterEff=None, taskNumber=None, taskMem=None, taskMulticore=None, taskNumEvents=None, scramArch=None, runNumber=None, firstEvent=None, firstLumi=None, extend = None, blockBlacklist=None, dontIncrementPV=None):
    """
    Adapts schema to right parameters.
    If the original workflow points to DBS2, DBS3 URL is fixed instead.
    if backfill is True, modifies RequestString, ProcessingString, AcquisitionEra
    and Campaign to say Backfill, and restarts requestDate.
    """
    result = reqMgrClient.purgeClonedSchema( cache )

    ## then further drop nested arguments
    taskParamBlacklist = [ 'EventsPerJob' ] 
    for i in range(1,100):
        t='Task%s'%i
        if not t in result: break
        for p in taskParamBlacklist:
            if p in result[t]:
                result[t].pop( p )
    if memory:
        result['Memory'] = memory
    if timeperevent:
        result['TimePerEvent'] = timeperevent
    if user:
        result['Requestor'] = user
    if group:
        result['Group'] = group
    # and required for cross-cloning between prod and testbed
    if result.get('CouchURL'):
        result['ConfigCacheUrl'] = result.pop('CouchURL')

    #if we are extending the workflow
    if events:
        # extend workflow so it will safely start outside of the boundary
        RequestNumEvents = int(result['RequestNumEvents'])
        FirstEvent = int(result['FirstEvent'])

        # FirstEvent_NEW > FirstEvent + RequestNumEvents
        # the fist event needs to be oustide the range
        result['FirstEvent'] = FirstEvent + RequestNumEvents + DELTA_EVENTS

        # FirstLumi_NEW > FirstLumi + RequestNumEvents/events_per_job/filterEff
        # same for the first lumi, needs to be after the last lumi

        result['FirstLumi'] = firstLumi + DELTA_LUMIS
        # only the desired events
        result['RequestNumEvents'] = events

        # prepend EXT_ to recognize as extension
        result["RequestString"] = 'EXT_' + result["RequestString"]
    else:
        if not dontIncrementPV:
            try:
                result["ProcessingVersion"] += 1
            except ValueError:
                result["ProcessingVersion"] = 2

    # modify for backfill
    if backfill:
        # Modify ProcessingString, AcquisitionEra, Campaign and Request string (if they don't
        # have the word 'backfill' in it
        result["ProcessingString"] = "BACKFILL"
        if isinstance(result["AcquisitionEra"],dict):
            for eraName in result["AcquisitionEra"]:
                if "backfill" not in result["AcquisitionEra"][eraName].lower():
                    result["AcquisitionEra"][eraName] = result["AcquisitionEra"][eraName] + "Backfill"
        else:
            if "backfill" not in result["AcquisitionEra"].lower():
                result["AcquisitionEra"] = result["AcquisitionEra"] + "Backfill"
        if "backfill" not in result["Campaign"].lower():
            result["Campaign"] = result["Campaign"] + "-Backfill"
        if "backfill" not in result["RequestString"].lower():
            # Word backfill in the middle of the request strin
            parts = result["RequestString"].split('-')
            result["RequestString"] = '-'.join(parts[:2] + ["Backfill"] + parts[2:])
        if "PrepID" in result:
            # delete entry
            del result["PrepID"]
        if result["RequestType"] == 'TaskChain':
            for taskNumber in range(1, result['TaskChain'] + 1):
                taskName = 'Task%s' % taskNumber
                if 'ProcessingString' in result[taskName]:
                    result[taskName]['ProcessingString'] = "BACKFILL"
                if 'AcquisitionEra' in result[taskName]:
                    result[taskName]['AcquisitionEra'] += "Backfill"

    # DataProcessing requests don't support RequestNumEvents argument anymore
    if 'InputDataset' in result and result['InputDataset']:
        result.pop('RequestNumEvents', None)
    # Dirty check in TaskChain and StepChains
    if 'Task1' in result and 'InputDataset' in result['Task1'] and result['Task1']['InputDataset']:
        result['Task1'].pop('RequestNumEvents', None)
    if 'Step1' in result and 'InputDataset' in result['Step1'] and result['Step1']['InputDataset']:
        result['Step1'].pop('RequestNumEvents', None)


    if extend:

        dataset = reqMgrClient.outputdatasetsWorkflow(url, workflow).pop()
        lastLumi = dbs3Client.getMaxLumi(dataset)
        firstlumi = int(lastLumi)
        result['FirstLumi'] = firstlumi + DELTA_LUMIS

        if "RequestNumEvents" in result:
            RequestNumEvents = int(result['RequestNumEvents'])
        elif "RequestNumEvents" in result["Task1"]:
            RequestNumEvents = int(result["Task1"]['RequestNumEvents'])
        else:
            print("Can't find RequestNumEvents, exiting")
            sys.exit(1)
        FirstEvent = int(result['FirstEvent'])
        result['FirstEvent'] = FirstEvent + RequestNumEvents + DELTA_EVENTS


    if firstEvent and firstLumi:
        result["FirstEvent"] = firstEvent
        result["FirstLumi"] = firstLumi

    if taskNumber:
        # Check request type and determine the key (task or step name) in which we'll do modifications
        if "RequestType" in result:
            if result["RequestType"] == "StepChain":
                key = "Step%s" % str(taskNumber)
                nTasks = result["StepChain"]
            else:
                key = "Task%s" % str(taskNumber)
                nTasks = result["TaskChain"]
        else:
            raise Exception("The Request Type is not identified")


        # Check if that key exists
        if key not in result:
            raise Exception("There is no task/step called %s" % key)

        if taskMem:
            result[key]["Memory"] = int(taskMem)
            if nTasks == 1:
                result["Memory"] = int(taskMem)

        if taskMulticore:
            result[key]["Multicore"] = int(taskMulticore)
            if nTasks == 1:
                result["Memory"] = int(taskMem)
        if taskNumEvents:
            result[key]["RequestNumEvents"] = int(taskNumEvents)

        # ScramArch update
        if scramArch:
            # Question: does the global scramArch have any use for taskchains and stepchains
            if result["RequestType"] == "TaskChain":
                result["Task" + str(taskNumber)]["ScramArch"] = scramArch
            elif result["RequestType"] == "StepChain":
                result["Step" + str(taskNumber)]["ScramArch"] = scramArch
            else:
                print("You're trying to change the scramArch in task level while the request isn't a task or stepchain")
                sys.exit()
    elif scramArch:
        requestType = result["RequestType"]
        prefix = requestType[:4]
        nTasks = result[requestType]
        for i in range(nTasks):
            result[prefix + str(i + 1)]["ScramArch"] = scramArch
    else:
        pass

    if runNumber:
        result["RunNumber"] = runNumber

    if blockBlacklist:
        result['BlockBlacklist'] = blockBlacklist

    return result

def cloneWorkflow(workflow, user, group, verbose=True, backfill=False, testbed=False, memory=None, timeperevent=None, bwl=None, filterEff=None, taskNumber=None, taskMem=None, taskMulticore=None, taskNumEvents=None, scramArch=None, runNumber=None, firstEvent=None, firstLumi=None, extend=None, blockBlacklist=None, dontIncrementPV=None):
    """
    clones a workflow
    """
    # Adapt schema and add original request to it
    cache = reqMgrClient.getWorkflowInfo(url, workflow)

    schema = modifySchema(cache, workflow, user, group, None, None, backfill, memory, timeperevent, filterEff, taskNumber, taskMem, taskMulticore, taskNumEvents, scramArch, runNumber, firstEvent, firstLumi, extend, blockBlacklist, dontIncrementPV)

    if verbose:
        pprint(schema)
    
    if bwl:
        if 'Task1' in schema:
            schema['Task1']['BlockWhitelist'] = bwl.split(',')
        else:
            schema['BlockWhitelist'] = bwl.split(',')
    ## only once

    print('Submitting workflow')
    # Submit cloned workflow to ReqMgr
    if testbed:
        newWorkflow = reqMgrClient.submitWorkflow(url_tb, schema)
    else:
        newWorkflow = reqMgrClient.submitWorkflow(url, schema)
    if verbose:
        print("RESPONSE", newWorkflow)

    # find the workflow name in response
    if newWorkflow:
        print('Cloned workflow: ' + newWorkflow)
        if verbose:
            print(newWorkflow)
            print('Approving request response:')

        # Move the request to Assignment-approved
        if testbed:
            data = reqMgrClient.setWorkflowApproved(url_tb, newWorkflow)
        else:
            data = reqMgrClient.setWorkflowApproved(url, newWorkflow)
        if verbose:
            print(data)
        # return the name of new workflow
        return newWorkflow
    else:
        if verbose:
            print(newWorkflow)
        else:
            print("Couldn't clone the workflow.")
        return None

def getMissingEvents(workflow):
    """
    Gets the missing events for the workflow
    """
    inputEvents = reqMgrClient.getInputEvents(url, workflow)
    dataset = reqMgrClient.outputdatasetsWorkflow(url, workflow).pop()
    outputEvents = reqMgrClient.getOutputEvents(url, workflow, dataset)
    return int(inputEvents) - int(outputEvents)

def extendWorkflow(workflow, user, group, verbose=False, events=None, firstlumi=None):

    if events is None:
        events = getMissingEvents(workflow)
    events = int(events)

    if firstlumi is None:
        #get the last lumi of the dataset
        dataset = reqMgrClient.outputdatasetsWorkflow(url, workflow).pop()

        lastLumi = dbs3Client.getMaxLumi(dataset)
        firstlumi = lastLumi
    firstlumi = int(firstlumi)

    # Get info about the workflow to be cloned
    cache = reqMgrClient.getWorkflowInfo(url, workflow)

    schema = modifySchema(cache, workflow, user, group, events, firstlumi, None)
    if verbose:
        pprint(schema)
    print('Submitting workflow')
    # Submit cloned workflow to ReqMgr
    response = reqMgrClient.submitWorkflow(url,schema)
    if verbose:
        print("RESPONSE", response)

    #find the workflow name in response
    m = re.search("details\/(.*)\'",response)
    if m:
        newWorkflow = m.group(1)
        print('Cloned workflow: '+newWorkflow)
        print('Extended with', events, 'events')
        print(response)

        # Move the request to Assignment-approved
        print('Approve request response:')
        data = reqMgrClient.setWorkflowApproved(url, newWorkflow)
        print(data)
    else:
        print(response)


"""
__Main__
"""
url = 'cmsweb.cern.ch'
url_tb = 'cmsweb-testbed.cern.ch'
reqmgrCouchURL = "https://" + url + "/couchdb/reqmgr_workload_cache"

 
def main():

    # Create option parser
    usage = "\n       python %prog [options] [WORKFLOW_NAME]"\
            "WORKFLOW_NAME: if the list file is provided this should be empty\n"

    parser = OptionParser(usage=usage)
    parser.add_option("-a", "--action", dest="action", default='clone',
                      help="There are two options clone (clone) or extend a worflow (extend) .")
    parser.add_option("-u", "--user", dest="user",
                      help="User we are going to use", default=None)
    parser.add_option("-g", "--group", dest="group", default='DATAOPS',
                      help="Group to send the workflows.")
    parser.add_option("-b", "--backfill", action="store_true", dest="backfill", default=False,
                      help="Creates a clone for backfill test purposes.")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Prints all query information.")
    parser.add_option('-f', '--file', help='Text file with a list of workflows', dest='file')
    parser.add_option('--bwl', help='The block white list to be used', dest='bwl',default=None)
    #Extend workflow options
    parser.add_option('-e', '--events', help='# of events to add', dest='events')
    parser.add_option('-l', '--firstlumi', help='# of the first lumi', dest='firstlumi')
    parser.add_option("-m", "--memory", dest="memory", help="Set max memory for the event. At assignment, this will be used to calculate maxRSS = memory*1024")
    parser.add_option("--TimePerEvent", help="Set the TimePerEvent on the clone")
    parser.add_option("--testbed", action="store_true", dest="testbed", default=False,
                      help="Clone to testbed reqmgr insted of production")
    parser.add_option('--filterEff', help='filter efficiency of given task/step', dest='filterEff', default=None)
    parser.add_option('--taskNumber', help='taskNumber for which to change filterEff or memory or multicore', dest='taskNumber', default=None)
    parser.add_option('--taskMem', help='memory to change in task level', dest='taskMem', default=None)
    parser.add_option('--taskMulticore', help='multicore to change in task level', dest='taskMulticore', default=None)
    parser.add_option('--taskNumEvents', help='RequestNumEvents to change in task level', dest='taskNumEvents', default=None)
    parser.add_option('--scramArch', help='Add ScramArch on top of the existing one', dest='scramArch', default=None)
    parser.add_option('--runNumber', help='runNumber', dest='runNumber', default=1)
    parser.add_option('--firstEvent', help='firstEvent', dest='firstEvent', default=1)
    parser.add_option('--firstLumi', help='firstLumi', dest='firstLumi', default=1)
    parser.add_option('--extend', action="store_true", help='extend', dest='extend', default=False)
    parser.add_option('--blockBlacklist', help='blockBlacklist', dest='blockBlacklist', default=None)
    parser.add_option("--dontIncrementPV", action="store_true", dest="dontIncrementPV", default=False,
                      help="If True, increments PV when necessary. Else keeps it the same")
    (options, args) = parser.parse_args()

    # Check the arguments, get info from them
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    elif len(args) > 0:
        # name of workflow
        wfs = [args[0]]
    else:
        parser.error("Provide the workflow of a file of workflows")
        sys.exit(1)

    if not options.user:
        # get os username by default
        uinfo = pwd.getpwuid(os.getuid())
        user = uinfo.pw_name
    else:
        user = options.user

    if options.blockBlacklist:
        blockBlacklist = [block.strip() for block in open(options.blockBlacklist) if block.strip()]

    if options.action == 'clone':
        for wf in wfs:
            memory = None
            timeperevent = None
            workflow = reqMgrClient.Workflow(wf)
            if options.memory:
                memory = float(options.memory)
            if options.TimePerEvent:
                timeperevent = float(options.TimePerEvent)
            cloneWorkflow(
                wf,
                user,
                options.group,
                options.verbose,
                options.backfill,
                options.testbed,
                memory,
                timeperevent,
                bwl=options.bwl,
                filterEff=options.filterEff,
                taskNumber=options.taskNumber,
                taskMem=options.taskMem,
                taskMulticore=options.taskMulticore,
                taskNumEvents=options.taskNumEvents,
                scramArch=options.scramArch,
                runNumber=options.runNumber,
                firstEvent=options.firstEvent,
                firstLumi=options.firstLumi,
                extend=options.extend,
                blockBlacklist=blockBlacklist,
                dontIncrementPV=options.dontIncrementPV
            )
    elif options.action == 'extend':
        for wf in wfs:
            extendWorkflow(wf, user, options.group, options.verbose, options.events, options.firstlumi)

    sys.exit(0)


if __name__ == "__main__":
    main()
