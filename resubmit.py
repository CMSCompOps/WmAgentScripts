#!/usr/bin/env python

"""
This script clones a given workflow
*args must be: workflow_name user group
This script depends on WMCore code, so WMAgent environment
and libraries need to be loaded before running it.
"""
import pprint
import os, datetime
import sys
import urllib
import httplib
import re
import json
import changePriorityWorkflow
import reqMgrClient
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

def modifySchema(helper, user, group, backfill=False):
    """
    Adapts schema to right parameters.
    If the original workflow points to DBS2, DBS3 URL is fixed instead.
    if backfill is True, modifies RequestString, ProcessingString, AcquisitionEra
    and Campaign to say Backfill, and restarts requestDate.
    """
    result = {}
    for (key, value) in helper.data.request.schema.dictionary_().items():
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
        elif not value:
            continue
        elif value != None:
            result[key] = value
    #check MonteCarlo
    if result['RequestType']=='MonteCarlo':
        #check assigning parameters
        #seek for events per job on helper
        splitting = helper.listJobSplittingParametersByTask()
        eventsPerJob = 120000
        eventsPerLumi = 100000
        for k, v in splitting.items():
            print k,":",v
            if k.endswith('/Production'):
                if 'events_per_job' in v:
                    eventsPerJob = v['events_per_job']
                elif 'events_per_lumi' in v:
                    eventsPerLumi = v['events_per_lumi']
        result['EventsPerJob'] = eventsPerJob
        #result['EventsPerLumi'] = eventsPerLumi
    #check MonteCarloFromGen
    elif result['RequestType']=='MonteCarloFromGEN':
        #seek for lumis per job on helper
        splitting = helper.listJobSplittingParametersByTask()
        lumisPerJob = 300
        for k, v in splitting.items():
            if k.endswith('/Production'):
                if 'lumis_per_job' in v:
                    lumisPerJob = v['lumis_per_job']
        result['LumisPerJob'] = lumisPerJob
        #Algorithm = lumi based?
        result["SplittingAlgo"] = "LumiBased"
    #Merged LFN   
    if 'MergedLFNBase' not in result:
        result['MergedLFNBase'] = helper.getMergedLFNBase()
    
    #update information from reqMgr    
    # Add AcquisitionEra, ProcessingString and increase ProcessingVersion by 1
    result["ProcessingString"] = helper.getProcessingString()
    result["AcquisitionEra"] = helper.getAcquisitionEra()
    #try to parse processing version as an integer, if don't, assign 2.
    try:
        result["ProcessingVersion"] = int(helper.getProcessingVersion()) + 1
    except ValueError:
        result["ProcessingVersion"] = 2

    #modify for backfill
    if backfill:
        #Modify ProcessingString, AcquisitionEra, Campaign and Request string (if they don't
        #have the word 'backfill' in it
        result["RequestNumEvents"] = 10000000
        result["ProcessingString"] = "BACKFILL"
        if "backfill" not in result["AcquisitionEra"].lower():
            result["AcquisitionEra"] = helper.getAcquisitionEra()+"Backfill"
        if "backfill" not in result["Campaign"].lower():
            result["Campaign"] = result["Campaign"]+"-Backfill"
        if "backfill" not in result["RequestString"].lower():
            #Word backfill in the middle of the request strin
            parts = result["RequestString"].split('-')
            result["RequestString"] = '-'.join(parts[:2]+["Backfill"]+parts[2:])
        if "PrepID" in result:
            #delete entry
            del result["PrepID"]
        #reset the request date
        now = datetime.datetime.utcnow()
        result["RequestDate"] = [now.year, now.month, now.day, now.hour, now.minute]
    return result


def cloneWorkflow(workflow, user, group, verbose=False, backfill=False):
    """
    clones a workflow
    """
    # Get info about the workflow to be cloned
    helper = reqMgrClient.retrieveSchema(workflow)
    # get info from reqMgr
    schema = modifySchema(helper, user, group, backfill)

    print 'Submitting workflow'
    # Sumbit cloned workflow to ReqMgr
    response = reqMgrClient.submitWorkflow(url,schema)
    print "RESPONSE", response

    #find the workflow name in response
    m = re.search("details\/(.*)\'",response)
    if m:
        newWorkflow = m.group(1)
        if verbose:
            print 'Cloned workflow: '+newWorkflow
            print response    

            print 'Approve request response:'
        #TODO only for debug
        #response = reqMgrClient.setWorkflowSplitting(url, schema)
        #print "RESPONSE", response
        #schema['requestName'] = requestName
        #schema['splittingTask'] = '/%s/%s' % (requestName, taskName)
        #schema['splittingAlgo'] = splittingAlgo

        # Move the request to Assignment-approved
        data = reqMgrClient.setWorkflowApproved(url, newWorkflow)
        if verbose:
            print data
        #return the name of new workflow
        return newWorkflow
    else:
        if verbose:
            print response
        return None


"""
__Main__
"""
url = 'cmsweb.cern.ch'

def main():
    # Check the arguements, get info from them
    if len(sys.argv) != 4 and len(sys.argv) != 5:
        print "Usage:"
        print "  ./resubmit WORKFLOW_NAME USER GROUP [-b]"
        sys.exit(0)
    #backfill option
    backfill = (len(sys.argv) == 5 and sys.argv[4] == '-b')
    workflow = sys.argv[1]
    user = sys.argv[2]
    group = sys.argv[3]    
    #Show verbose
    cloneWorkflow(workflow, user, group, True, backfill)
    
    sys.exit(0)


if __name__ == "__main__":
    main()

