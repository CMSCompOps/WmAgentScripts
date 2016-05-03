#!/usr/bin/env python

"""
    This script clones a given workflow
    Usage:
        python resubmit.py [options] WORKFLOW_NAME [USER GROUP]
    Options:
        -b --backfill, creates a clone
        -v --verbose, prints schemas and responses
    USER: the user for creating the clone, if empty it will
          use the OS user running the script
    GROUP: the group for creating the clone, if empty it will
          use 'DATAOPS' by default
    This script depends on WMCore code, so WMAgent environment
    ,libraries and voms proxy need to be loaded before running it.
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


def modifySchema(helper, user, group, cache, backfill=False):
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
        # requestor info
        elif key == 'Requestor':
            result['Requestor'] = user
        elif key == 'Group':
            result['Group'] = group
        # if emtpy
        elif key in ["RunWhitelist", "RunBlacklist", "BlockWhitelist", "BlockBlacklist"] and not value:
            result[key] = []
        # replace old DBS2 URL
        elif value == "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet":
            result[key] = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        # copy the right LFN base
        elif key == 'MergedLFNBase':
            result['MergedLFNBase'] = helper.getMergedLFNBase()
        # convert LumiList to dict
        # elif key == 'LumiList':
        #   result['LumiList'] = JsonWrapper.loads(value)
        #   result['LumiList'] = eval(value)

        # TODO deleting timeout so they will move to running-close as soon as they can
        # elif key == 'OpenRunningTimeout':
            # delete entry
        #    continue
        # skip empty entries
        elif value != None:
            result[key] = value
        elif not value:
            continue
    # Clean requestor  DN?
    if 'RequestorDN' in result:
        del result['RequestorDN']
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
    # try to parse processing version as an integer, if don't, assign 2.
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
        
    schema = modifySchema(helper, user, group, cache, backfill)
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

"""
__Main__
"""
url = 'cmsweb.cern.ch'
#url_tb = 'cmsweb-testbed.cern.ch'
#url = url_tb
reqmgrCouchURL = "https://" + url + "/couchdb/reqmgr_workload_cache"


def main():

    # Create option parser
    usage = "\n       python %prog [options] [WORKFLOW_NAME] [USER GROUP]\n"\
            "WORKFLOW_NAME: if the list file is provided this should be empty\n"\
            "USER: the user for creating the clone, if empty it will\n"\
            "      use the OS user running the script\n"\
            "GROUP: the group for creating the clone, if empty it will\n"\
            "      use 'DATAOPS' by default"

    parser = OptionParser(usage=usage)
    parser.add_option("-b", "--backfill", action="store_true", dest="backfill", default=False,
                      help="Creates a clone for backfill test purposes.")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Prints all query information.")
    parser.add_option('-f', '--file', help='Text file with a list of workflows', dest='file')
    parser.add_option('--bwl', help='The block white list to be used', dest='bwl',default=None)
    parser.add_option("--testbed", action="store_true", dest="testbed", default=False,
                      help="Clone to testbed reqmgr insted of production")
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
        cloneWorkflow(
            wf, user, group, options.verbose, options.backfill, options.testbed, bwl=options.bwl)

    sys.exit(0)


if __name__ == "__main__":
    main()
