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
        -b --backfill, creates a clone
        -v --verbose, prints schemas and responses

    This script depends on WMCore code, so WMAgent environment,libraries and voms proxy need to be loaded before running it.
"""
import os
import datetime
import pwd
import sys
import re
from optparse import OptionParser
from pprint import pprint
try:
    import reqMgrClient
    from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
    from WMCore.Wrappers import JsonWrapper
except:
    print "WMCore libraries not loaded, run the following command:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(0)

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

DELTA_EVENTS = 1000
DELTA_LUMIS = 200


def modifySchema(helper, workflow, user, group, cache, events, firstLumi, backfill=False):
    """
    Adapts schema to right parameters.
    If the original workflow points to DBS2, DBS3 URL is fixed instead.
    if backfill is True, modifies RequestString, ProcessingString, AcquisitionEra
    and Campaign to say Backfill, and restarts requestDate.
    """
    result = {}
    for (key, value) in helper.data.request.schema.dictionary_whole_tree_().items():
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
        #if empty
        elif key in ["RunWhitelist", "RunBlacklist", "BlockWhitelist", "BlockBlacklist"] and not value:
            result[key] = []
        #replace old DBS2 URL
        elif value == "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet":
            result[key] = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        #copy the right LFN base
        elif key == 'MergedLFNBase':
            result['MergedLFNBase'] = helper.getMergedLFNBase()
        # skip empty entries
        elif value != None:
            result[key] = value
        elif not value:
            continue
    # Clean requestor  DN?
    if 'RequestorDN' in result:
        del result['RequestorDN']
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
        # Update the request priority
        if cache and 'RequestPriority' in cache:
            result['RequestPriority'] = cache['RequestPriority']
    # check MonteCarlo
    if result['RequestType'] == 'MonteCarlo':
        # check assigning parameters
        # seek for events per job on helper
        try:
            splitting = helper.listJobSplittingParametersByTask()
        except AttributeError:
            splitting = {}

        eventsPerJob = 120000
        eventsPerLumi = 100000
        for k, v in splitting.items():
            # print k,":",v
            if k.endswith('/Production'):
                if 'events_per_job' in v:
                    eventsPerJob = v['events_per_job']
                elif 'events_per_lumi' in v:
                    eventsPerLumi = v['events_per_lumi']
        # result['EventsPerJob'] = eventsPerJob
        # result['EventsPerLumi'] = eventsPerLumi
    # check MonteCarloFromGen
    elif result['RequestType'] == 'MonteCarloFromGEN':
        # seek for lumis per job on helper
        splitting = helper.listJobSplittingParametersByTask()
        lumisPerJob = 300
        for k, v in splitting.items():
            if k.endswith('/Production'):
                if 'lumis_per_job' in v:
                    lumisPerJob = v['lumis_per_job']
        result['LumisPerJob'] = lumisPerJob
        # Algorithm = lumi based?
        result["SplittingAlgo"] = "LumiBased"
    elif result['RequestType'] == "TaskChain":
        # Now changing the parameters according to HG1309
        x = 1
        # on every task
        while x <= result['TaskChain']:
            task = 'Task' + str(x)
            for (key, value) in result[task].iteritems():
                if key == "SplittingAlgorithm":
                    result[task]['SplittingAlgo'] = value
                    del result[task]['SplittingAlgorithm']
                elif key == "SplittingArguments":
                    for (k2, v2) in result[task][key].iteritems():
                        if k2 == "lumis_per_job":
                            result[task]["LumisPerJob"] = v2
                        elif k2 == "events_per_job":
                            result[task]["EventsPerJob"] = v2
                        del result[task]['SplittingArguments']
            x += 1

    # Merged LFN
    if 'MergedLFNBase' not in result:
        result['MergedLFNBase'] = helper.getMergedLFNBase()

    # update information from reqMgr
    # Add AcquisitionEra, ProcessingString and increase ProcessingVersion by 1
    result["ProcessingString"] = helper.getProcessingString()
    result["AcquisitionEra"] = helper.getAcquisitionEra()
    # try to parse processing version as an integer, if don't, assign 1 or 2 given the case.
    if events:
        try:
            result["ProcessingVersion"] = int(helper.getProcessingVersion())
        except ValueError:
            result["ProcessingVersion"] = 1
    else:
        try:
            result["ProcessingVersion"] = int(helper.getProcessingVersion()) + 1
        except ValueError:
            result["ProcessingVersion"] = 2

    # modify for backfill
    if backfill:
        # Modify ProcessingString, AcquisitionEra, Campaign and Request string (if they don't
        # have the word 'backfill' in it
        result["ProcessingString"] = "BACKFILL"
        if "backfill" not in result["AcquisitionEra"].lower():
            result["AcquisitionEra"] = helper.getAcquisitionEra() + "Backfill"
        if "backfill" not in result["Campaign"].lower():
            result["Campaign"] = result["Campaign"] + "-Backfill"
        if "backfill" not in result["RequestString"].lower():
            # Word backfill in the middle of the request strin
            parts = result["RequestString"].split('-')
            result[
                "RequestString"] = '-'.join(parts[:2] + ["Backfill"] + parts[2:])
        if "PrepID" in result:
            # delete entry
            del result["PrepID"]
        # reset the request date
        now = datetime.datetime.utcnow()
        result["RequestDate"] = [
            now.year, now.month, now.day, now.hour, now.minute]

    #result['Memory'] = 3000

    return result


def cloneWorkflow(workflow, user, group, verbose=True, backfill=False, testbed=False, bwl=None):
    """
    clones a workflow
    """
    # Get info about the workflow to be cloned
    helper = reqMgrClient.retrieveSchema(workflow, reqmgrCouchURL)
    # Adapt schema and add original request to it
    try:
        cache = reqMgrClient.getWorkloadCache(url, workflow)
    except:
        cache = None
        
    schema = modifySchema(helper, workflow, user, group, cache, None, None, backfill)

    schema['OriginalRequestName'] = workflow
    if verbose:
        pprint(schema)
    
    if bwl:
        if 'Task1' in schema:
            schema['Task1']['BlockWhitelist'] = bwl.split(',')
        else:
            schema['BlockWhitelist'] = bwl.split(',')
    print 'Submitting workflow'
    # Sumbit cloned workflow to ReqMgr
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
    helper = reqMgrClient.retrieveSchema(workflow)
    schema = modifySchema(helper, workflow, user, group, None, events, firstlumi, None)
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
reqmgrCouchURL = "https://" + url + "/couchdb/reqmgr_workload_cache"


def main():

    # Create option parser
    usage = "\n       python %prog [options] [WORKFLOW_NAME]"\
            "WORKFLOW_NAME: if the list file is provided this should be empty\n"

    parser = OptionParser(usage=usage)
    parser.add_option("-a", "--action", dest="action", default='clone',
                      help="There are two options clone (clone) or extend a worflow (extend) .")
    parser.add_option("-u", "--user", dest="user",
<<<<<<< HEAD
                      help="User we are going to use")
=======
                      help="User we are going to use", default=None)
>>>>>>> 538aa78a05b835f8784558ace207ac365f478c19
    parser.add_option("-g", "--group", dest="group", default='DATAOPS',
                      help="Group we are going to use.")
    parser.add_option("-b", "--backfill", action="store_true", dest="backfill", default=False,
                      help="Creates a clone for backfill test purposes.")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Prints all query information.")
    parser.add_option('-f', '--file', help='Text file with a list of workflows', dest='file')
    parser.add_option('--bwl', help='The block white list to be used', dest='bwl',default=None)
    #Extend workflow options
    parser.add_option('-e', '--events', help='# of events to add', dest='events')
    parser.add_option('-l', '--firstlumi', help='# of the first lumi', dest='firstlumi')

    parser.add_option("--testbed", action="store_true", dest="testbed", default=False,
                      help="Clone to testbed reqmgr insted of production")
    (options, args) = parser.parse_args()

    # Check the arguments, get info from them
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    else:
        # name of workflow
        wfs = [args[0]]
<<<<<<< HEAD
    if not user:
        # get os username by default
        uinfo = pwd.getpwuid(os.getuid())
        user = uinfo.pw_name
=======
    if not options.user:
        # get os username by default
        uinfo = pwd.getpwuid(os.getuid())
        options.user = uinfo.pw_name
>>>>>>> 538aa78a05b835f8784558ace207ac365f478c19

    if action == 'clone':
        for wf in wfs:
            cloneWorkflow(
<<<<<<< HEAD
                wf, user, group, options.verbose, options.backfill, options.testbed, bwl=options.bwl)
    elif action == 'extend':
        for wf in wfs:
            extendWorkflow(wf, user, group, options.verbose, options.events, options.firstlumi)
=======
                wf, options.user, options.group, options.verbose, options.backfill, options.testbed, bwl=options.bwl)
    elif action == 'extend':
        for wf in wfs:
            extendWorkflow(wf, options.user, options.group, options.verbose, options.events, options.firstlumi)
>>>>>>> 538aa78a05b835f8784558ace207ac365f478c19

    sys.exit(0)


if __name__ == "__main__":
    main()
