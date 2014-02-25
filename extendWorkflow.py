#!/usr/bin/env python

"""
This script should be used only for MonteCarlo from scratch.
It creates a resubmition of a MonteCarlo workflow that begins
when the original worklfow ended.
"""
import os
import sys
import urllib
import httplib
import re
import json
import changePriorityWorkflow
import reqMgrClient, dbs3Client
import math
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

DELTA_EVENTS = 1000
DELTA_LUMIS = 200

def retrieveSchema(workflowName):
    """
    Creates the cloned specs for the original request
    Updates parameters
    """
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    helper.load(specURL)
    return helper
    

def modifySchema(helper, workflow, user, group, events):
    """
    Adapts schema to right parameters
    """
    result = {}
    # Add AcquisitionEra, ProcessingString and ProcessingVersion
    result["ProcessingString"] = helper.getProcessingString()
    result["ProcessingVersion"] = helper.getProcessingVersion()
    result["AcquisitionEra"] = helper.getAcquisitionEra()
    
    for key, value in helper.data.request.schema.dictionary_().items():
        #previous versions of tags
        if key == 'ProcConfigCacheID':
            result['ConfigCacheID'] = value
        elif key == 'RequestSizeEvents':
            result['RequestSizeEvents'] = value
        #requestor info
        elif key == 'Requestor':
            result['Requestor'] = user
        elif key == 'Group':
            result['Group'] = group
        #if emtpy
        elif key in ["RunWhitelist", "RunBlacklist", "BlockWhitelist", "BlockBlacklist"] and not value:
            result[key]=[]
        #skip empty entries
        elif not value:
            continue
        elif value != None:
            result[key] = value

    #extend workflow so it will safely start outside of the boundary
    RequestNumEvents = int(result['RequestNumEvents'])
    FirstEvent = int(result['FirstEvent'])
    FirstLumi = int(result['FirstLumi'])
    EventsPerLumi = int(result['EventsPerLumi'])
    FilterEfficiency = float(result['FilterEfficiency'])

    #FirstEvent_NEW > FirstEvent + RequestNumEvents
    #the fist event needs to be oustide the range
    result['FirstEvent'] = FirstEvent + RequestNumEvents + DELTA_EVENTS

    #FirstLumi_NEW > FirstLumi + RequestNumEvents/events_per_job/filterEff
    # same for the first lumi, needs to be after the last lumi
    """
    result['FirstLumi'] = int(FirstLumi
                            + math.ceil( RequestNumEvents / float(EventsPerLumi) / FilterEfficiency )
                            + DELTA_LUMIS / FilterEfficiency )
    """
    #get the last lumi of the dataset
    dataset = reqMgrClient.outputdatasetsWorkflow(url, workflow).pop()
    LastLumi = dbs3Client.getMaxLumi(dataset)

    result['FirstLumi'] = LastLumi + DELTA_LUMIS
    #only the desired events    
    result['RequestNumEvents'] = events

    if 'LumisPerJob' not in result and result['RequestType']=='MonteCarlo':
        #seek for lumis per job on helper
        splitting = helper.listJobSplittingParametersByTask()
        lumisPerJob = 300
        for k, v in splitting.items():
            if k.endswith('/Production'):
                if 'lumis_per_job' in v:
                    lumisPerJob = v['lumis_per_job']
        result['LumisPerJob'] = lumisPerJob

    #TODO do this always?
    if 'EventsPerJob' not in result and result['RequestType']=='MonteCarlo':
        #seek for events per job on helper
        splitting = helper.listJobSplittingParametersByTask()
        eventsPerJob = 120000
        for k, v in splitting.items():
            if k.endswith('/Production'):
                if 'events_per_job' in v:
                    eventsPerJob = v['events_per_job']
        result['EventsPerJob'] = eventsPerJob
   
    if 'MergedLFNBase' not in result:
        result['MergedLFNBase'] = helper.getMergedLFNBase()
    return result



def getMissingEvents(workflow):
    """
    Gets the missing events for the workflow
    """
    inputEvents = reqMgrClient.getInputEvents(url, workflow)
    dataset = reqMgrClient.outputdatasetsWorkflow(url, workflow).pop()
    outputEvents = reqMgrClient.getOutputEvents(url, workflow, dataset)
    return int(inputEvents) - int(outputEvents)

"""
__Main__
"""
url = 'cmsweb.cern.ch'

def main():
    # Check the arguements, get info from them
    if len(sys.argv) < 4:
        print "Usage:"
        print "  ./resubmit WORKFLOW_NAME USER GROUP [EVENTS]"
        sys.exit(0)
    workflow = sys.argv[1]
    user = sys.argv[2]
    group = sys.argv[3]
    if len(sys.argv) > 4:
        events = int(sys.argv[4])
    else:
        events = getMissingEvents(workflow)

    # Get info about the workflow to be cloned
    helper = retrieveSchema(workflow)
    schema = modifySchema(helper, workflow, user, group, events)

    print 'Submitting workflow'
    # Sumbit cloned workflow to ReqMgr
    response = reqMgrClient.submitWorkflow(url,schema)
    #find the workflow name in response
    m = re.search("details\/(.*)\'",response)
    if m:
        newWorkflow = m.group(1)
        print 'Cloned workflow: '+newWorkflow
        print 'Extended with', events, 'events'
        print response
        
        # Move the request to Assignment-approved
        print 'Approve request response:'
        data = reqMgrClient.setWorkflowApproved(url, newWorkflow)
        print data
    else:
        print response
    sys.exit(0)


if __name__ == "__main__":
    main()

