#!/usr/bin/env python

"""
This script clones a given workflow
*args must be: workflow_name user group
This script depends on WMCore code, so WMAgent environment
and libraries need to be loaded before running it.
"""
import os
import sys
import urllib
import httplib
import re
import json
import changePriorityWorkflow
import reqMgrClient
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

def retrieveSchema(workflowName):
    """
    Creates the cloned specs for the original request
    Updates parameters
    """
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    helper.load(specURL)
    return helper
    

def modifySchema(helper, user, group):
    """
    Adapts schema to right parameters.
    If the original workflow points to DBS2, DBS3 URL is fixed instead.
    """
    result = {}
    # Add AcquisitionEra, ProcessingString and increase ProcessingVersion by 1
    result["ProcessingString"] = helper.getProcessingString()
    result["ProcessingVersion"] = helper.getProcessingVersion() + 1
    result["AcquisitionEra"] = helper.getAcquisitionEra()
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
        #skip empty entries
        elif not value:
            continue
        elif value != None:
            result[key] = value
    
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


def cloneWorkflow(workflow, user, group, verbose=False):
    """
    clones a workflow
    """
    # Get info about the workflow to be cloned
    helper = retrieveSchema(workflow)
    schema = modifySchema(helper, user, group)

    print 'Submitting workflow'
    # Sumbit cloned workflow to ReqMgr
    response = reqMgrClient.submitWorkflow(url,schema)
    #find the workflow name in response
    m = re.search("details\/(.*)\'",response)
    if m:
        newWorkflow = m.group(1)
        if verbose:
            print 'Cloned workflow: '+newWorkflow
            print response    

            print 'Approve request response:'
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
    if len(sys.argv) != 4:
        print "Usage:"
        print "  ./resubmit WORKFLOW_NAME USER GROUP"
        sys.exit(0)
    workflow = sys.argv[1]
    user = sys.argv[2]
    group = sys.argv[3]    
    #Show verbose
    cloneWorkflow(workflow, user, group, True)
    
    sys.exit(0)


if __name__ == "__main__":
    main()

