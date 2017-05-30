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

try:
    import reqMgrClient
except:
    print "WMCore libraries not loaded, run the following command:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(0)

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

DELTA_EVENTS = 1000
DELTA_LUMIS = 200

def modifySchema(cache, workflow, user, group, events, firstLumi, backfill=False, memory=None, timeperevent=None):
    """
    Adapts schema to right parameters.
    If the original workflow points to DBS2, DBS3 URL is fixed instead.
    if backfill is True, modifies RequestString, ProcessingString, AcquisitionEra
    and Campaign to say Backfill, and restarts requestDate.
    """
    # list of keys to be removed from the new request schema
    paramBlacklist = ['BlockCloseMaxEvents', 'BlockCloseMaxFiles', 'BlockCloseMaxSize', 'BlockCloseMaxWaitTime',
                      'CouchWorkloadDBName', 'CustodialGroup', 'CustodialSubType', 'Dashboard',
                      'GracePeriod', 'HardTimeout', 'InitialPriority', 'inputMode', 'MaxMergeEvents', 'MaxMergeSize',
                      'MaxRSS', 'MaxVSize', 'MinMergeSize', 'NonCustodialGroup','NonCustodialSubType',
                      'OutputDatasets', 'ReqMgr2Only', 'RequestDate' 'RequestorDN', 'RequestName', 'RequestStatus',
                      'RequestTransition', 'RequestWorkflow', 'SiteWhitelist', 'SoftTimeout', 'SoftwareVersions',
                      'SubscriptionPriority', 'Team', 'timeStamp', 'TrustSitelists', 'TrustPUSitelists',
                      'TotalEstimatedJobs', 'TotalInputEvents', 'TotalInputLumis', 'TotalInputFiles', 'DN',
                      'AutoApproveSubscriptionSites', 'NonCustodialSites', 'CustodialSites', 'Teams',
                      'OutputModulesLFNBases', 'AllowOpportunistic', 'InputDatasets', 'DeleteFromSource', '_id']

    result = {}
    for k, v in cache.iteritems():
        if k in paramBlacklist or v in ([], {}, None, ''):
            continue
        else:
            result[k] = v

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
        try:
            #result["ProcessingVersion"] = int(helper.getProcessingVersion()) + 1
            result["ProcessingVersion"] += 1
        except ValueError:
            result["ProcessingVersion"] = 2

    # modify for backfill
    if backfill:
        # Modify ProcessingString, AcquisitionEra, Campaign and Request string (if they don't
        # have the word 'backfill' in it
        result["ProcessingString"] = "BACKFILL"
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

    return result

def cloneWorkflow(workflow, user, group, verbose=True, backfill=False, testbed=False, memory=None, timeperevent=None, bwl=None):
    """
    clones a workflow
    """
    # Adapt schema and add original request to it
    cache = reqMgrClient.getWorkflowInfo(url, workflow)

    schema = modifySchema(cache, workflow, user, group, None, None, backfill, memory, timeperevent)

    if verbose:
        pprint(schema)
    
    if bwl:
        if 'Task1' in schema:
            schema['Task1']['BlockWhitelist'] = bwl.split(',')
        else:
            schema['BlockWhitelist'] = bwl.split(',')
    ## only once
    ####schema['CMSSWVersion'] = 'CMSSW_8_0_16'

    print 'Submitting workflow'
    # Submit cloned workflow to ReqMgr
    if testbed:
        newWorkflow = reqMgrClient.submitWorkflow(url_tb, schema)
    else:
        newWorkflow = reqMgrClient.submitWorkflow(url, schema)
    if verbose:
        print "RESPONSE", newWorkflow

    # find the workflow name in response
    if newWorkflow:
        print 'Cloned workflow: ' + newWorkflow
        if verbose:
            print newWorkflow
            print 'Approving request response:'
        # TODO only for debug
        #response = reqMgrClient.setWorkflowSplitting(url, schema)
        # print "RESPONSE", response
        #schema['requestName'] = requestName
        #schema['splittingTask'] = '/%s/%s' % (requestName, taskName)
        #schema['splittingAlgo'] = splittingAlgo

        # Move the request to Assignment-approved
        if testbed:
            data = reqMgrClient.setWorkflowApproved(url_tb, newWorkflow)
        else:
            data = reqMgrClient.setWorkflowApproved(url, newWorkflow)
        if verbose:
            print data
        # return the name of new workflow
        return newWorkflow
    else:
        if verbose:
            print newWorkflow
        else:
            print "Couldn't clone the workflow."
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
    print 'Submitting workflow'
    # Submit cloned workflow to ReqMgr
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
                wf, user, options.group, options.verbose, options.backfill, options.testbed, memory,timeperevent,bwl=options.bwl)
    elif options.action == 'extend':
        for wf in wfs:
            extendWorkflow(wf, user, options.group, options.verbose, options.events, options.firstlumi)

    sys.exit(0)


if __name__ == "__main__":
    main()
