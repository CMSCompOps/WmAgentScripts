#!/usr/bin/env python
"""
This script should be used only for MonteCarlo from scratch.
It creates a resubmition of a MonteCarlo workflow that begins
when the original worklfow ended.
"""
import sys
import re
import reqMgrClient
import dbs3Client
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from optparse import OptionParser
from pprint import pprint
import pwd
import os

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

DELTA_EVENTS = 1000
DELTA_LUMIS = 200

def modifySchema(helper, workflow, user, group, events, firstLumi):
    """
    Adapts schema to right parameters
    """
    result = {}
    #pprint.pprint(helper.data.request.schema.dictionary_())
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
        #replace old DBS2 URL
        elif value == "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet":
            result[key] = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        #copy the right LFN base
        elif key == 'MergedLFNBase':
            result['MergedLFNBase'] = helper.getMergedLFNBase()
        #TODO deleting timeout so they will move to running-close as soon as they can
        #elif key == 'OpenRunningTimeout':
            #delete entry
        #    continue
        #skip empty entries
        elif value != None:
            result[key] = value
        elif not value:
            continue
    # Clean requestor  DN?
    if 'RequestorDN' in result:
        del result['RequestorDN']
        
    #extend workflow so it will safely start outside of the boundary
    RequestNumEvents = int(result['RequestNumEvents'])
    FirstEvent = int(result['FirstEvent'])

    #FirstEvent_NEW > FirstEvent + RequestNumEvents
    #the fist event needs to be oustide the range
    result['FirstEvent'] = FirstEvent + RequestNumEvents + DELTA_EVENTS

    #FirstLumi_NEW > FirstLumi + RequestNumEvents/events_per_job/filterEff
    # same for the first lumi, needs to be after the last lumi  

    result['FirstLumi'] = firstLumi + DELTA_LUMIS
    #only the desired events    
    result['RequestNumEvents'] = events
    
    #prepend EXT_ to recognize as extension
    result["RequestString"] = 'EXT_'+result["RequestString"]

    #check MonteCarlo
    if result['RequestType']=='MonteCarlo':
        #check assigning parameters
        #seek for events per job on helper
        try:
            splitting = helper.listJobSplittingParametersByTask()
        except AttributeError:
            splitting = {}
        
        eventsPerJob = 120000
        eventsPerLumi = 100000
        for k, v in splitting.items():
            print k,":",v
            if k.endswith('/Production'):
                if 'events_per_job' in v:
                    eventsPerJob = v['events_per_job']
                elif 'events_per_lumi' in v:
                    eventsPerLumi = v['events_per_lumi']
        # result['EventsPerJob'] = eventsPerJob
        # result['EventsPerLumi'] = eventsPerLumi

    #Merged LFN   
    if 'MergedLFNBase' not in result:
        result['MergedLFNBase'] = helper.getMergedLFNBase()
    
    #update information from reqMgr    
    # Add AcquisitionEra, ProcessingString and ProcessingVersion
    result["ProcessingString"] = helper.getProcessingString()
    result["AcquisitionEra"] = helper.getAcquisitionEra()
    #try to parse processing version as an integer, if don't, assign 1
    try:
        result["ProcessingVersion"] = int(helper.getProcessingVersion())
    except ValueError:
        result["ProcessingVersion"] = 1

    return result

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
    helper = reqMgrClient.retrieveSchema(workflow)
    schema = modifySchema(helper, workflow, user, group, events, firstlumi)
    schema['OriginalRequestName'] = workflow
    if verbose:
        pprint(schema)
    print 'Submitting workflow'
    # Sumbit cloned workflow to ReqMgr
    response = reqMgrClient.submitWorkflow(url,schema)
    if verbose:
        print "RESPONSE", response
    
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
    pass
"""
__Main__
"""
url = 'cmsweb.cern.ch'

def main():
    # Check the arguements, get info from them
    
    # Create option parser
    usage = "\n       python %prog [options] [WORKFLOW_NAME] [USER GROUP]\n"\
            "WORKFLOW_NAME: if the list file is provided this should be empty\n"\
            "USER: the user for creating the clone, if empty it will\n"\
            "      use the OS user running the script\n"\
            "GROUP: the group for creating the clone, if empty it will\n"\
            "      use 'DATAOPS' by default"

    parser = OptionParser(usage=usage)
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Prints all query information.")
    parser.add_option('-f', '--file', help='Text file with a list of workflows', dest='file')
    parser.add_option('-e', '--events', help='# of events to add', dest='events')
    parser.add_option('-l', '--firstlumi', help='# of the first lumi', dest='firstlumi')
    (options, args) = parser.parse_args()


    # Check the arguments, get info from them
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
        if len(args) == 2:
            user = args[0]
            group = args[1]
        elif len(args) == 0:
            # get os username by default
            uinfo = pwd.getpwuid(os.getuid())
            user = uinfo.pw_name
            # group by default DATAOPS
            group = 'DATAOPS'
        else:
            parser.error("Provide the workflow of a file of workflows")
            sys.exit(1)    
    else:
        if len(args) == 3:
            user = args[1]
            group = args[2]
        elif len(args) == 1:
            # get os username by default
            uinfo = pwd.getpwuid(os.getuid())
            user = uinfo.pw_name
            # group by default DATAOPS
            group = 'DATAOPS'
        else:
            parser.error("Provide the workflow of a file of workflows")
            sys.exit(1)
        # name of workflow
        wfs = [args[0]]
    
    for wf in wfs:
        extendWorkflow(wf, user, group, options.verbose, options.events, options.firstlumi)
    
    sys.exit(0)


if __name__ == "__main__":
    main()

