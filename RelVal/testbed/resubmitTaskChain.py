#!/usr/bin/env python

import os
import sys
import urllib
import httplib
import re
import json
import pprint
from copy import deepcopy
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

#url = "cmsweb-testbed.cern.ch"
url = "cmsweb.cern.ch"
reqmgrCouchURL = "https://"+url+"/couchdb/reqmgr_workload_cache"

def approveRequest(url,workflow):
    params = {"requestName": workflow,
              "status": "assignment-approved"}

    encodedParams = urllib.urlencode(params)
    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #conn  =  httplib.HTTPConnection(url)
    conn.request("PUT",  "/reqmgr/reqMgr/request", encodedParams, headers)
    response = conn.getresponse()
    if response.status != 200:
        print 'could not approve request with following parameters:'
        for item in params.keys():
            print item + ": " + str(params[item])
        print 'Response from http call:'
        print 'Status:',response.status,'Reason:',response.reason
        print 'Explanation:'
        data = response.read()
        print data
        print "Exiting!"
        sys.exit(1)
    conn.close()
    print 'Cloned workflow:',workflow
    return



def retrieveSchema(workflowName):
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    helper.load(specURL)
    schema = {}
    for (key, value) in helper.data.request.schema.dictionary_whole_tree_().iteritems():
        if key == 'ProdConfigCacheID':
            schema['ConfigCacheID'] = value
        elif key=='ProcConfigCacheID':
            schema['ConfigCacheID'] = value
        elif key=='RequestSizeEvents':
            schema['RequestNumEvents'] = value
        elif key=='ProcessingString' and value == {}:
            continue
        elif key=='AcquisitionEra' and value == {}:
            continue
        elif key=='SkimConfigs' and not value:
            continue
        elif key=='SiteWhitelist' and not value:
            continue
        elif value != None:
            schema[key] = value
#    print "Retrieved schema:"
#    pprint.pprint(schema)
#    sys.exit(1)
    request = deepcopy(schema)

    ### Now changing the parameters according to HG1309
    x = 1
    while x <= schema['TaskChain']:
        task = 'Task'+str(x)
        for (key, value) in schema[task].iteritems():
            if key == "SplittingAlgorithm":
                request[task]['SplittingAlgo'] = value
                del request[task]['SplittingAlgorithm']
            elif key == "SplittingArguments":
                for (k2, v2) in schema[task][key].iteritems():
                    if k2 == "lumis_per_job":
                        request[task]["LumisPerJob"] = v2
                    elif k2 == "events_per_job":
                        request[task]["EventsPerJob"] = v2
                    del request[task]['SplittingArguments']
        x += 1

##### for SAM, HC and HCtest
#    request['RequestString'] = 'RVCMSSW_7_0_4ProdTTbar_HC'
    request['Requestor'] = 'jbadillo'
    request['Group'] = 'DATAOPS'
#    request['Task1']['RequestNumEvents'] = 2200000
#    request['Task1']['EventsPerLumi'] = 850
#    request['SubRequestType'] = 'FullChain'
#    request['Campaign'] = 'Agent0998_Validation'

#    request['ConfigCacheUrl'] = 'https://cmsweb.cern.ch/couchdb'
#    request['DbsUrl'] = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'
#    request['Task1']['RequestNumEvents'] = 5000
#    del request['CouchURL']
#    del request['CouchWorkloadDBName']


#    request['Task1']['KeepOutput'] = 'False'
#    request['Task2']['KeepOutput'] = 'False'
#    request['ConfigCacheUrl'] = 'https://cmsweb.cern.ch/couchdb'

#    request['Campaign'] = 'HG1406_SLC6_Validation'
#    request['DQMUploadUrl'] = 'https://cmsweb-testbed.cern.ch/dqm/dev'
#    request['DbsUrl'] = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'

#    del request['CouchDBName']
#    del request['CouchURL']
#    del request['CouchWorkloadDBName']
#    del request['RequestDate']
#    del request['RequestName']
#    del request['timeStamp']
#    del request['ConfigCacheURL']

#    request['mergedLFNBase'] = '/store/mc'
#    request['Task1']['RequestNumEvents'] = 500000
#    request['Task1']['AcquisitionEra'] = 'HC'
#    request['Task2']['AcquisitionEra'] = 'HC'
#    request['Task3']['AcquisitionEra'] = 'HC'

#    request['Memory'] = 2000
#    request['EventsPerLumi'] = 300
#    request['LheInputFiles'] = True
#    del request['Task1']['EventsPerLumi']
#    del request['Task1']['LheInputFiles']
#    request['RequestString'] = 'TEST_LSF_Meyrin_Alan_710pre5TTbar_13'
#    request['Task1']['InputDataset'] = '/QCD_HT-100To250_8TeV-madgraph/LHE-testAlan_Attempt3-v2/LHE'
#    del request['Task1']['LheInputFiles']
#    del request['Task1']['PrimaryDataset']
#    del request['Task1']['Seeding']
#    request['Task1']['RequestNumEvents'] = 15000
#    del request['Task3']
#    del request['Task4']
#    del request['DQMUploadUrl']
#    del request['DQMConfigCacheID']
#    request['TaskChain'] = 2
#    request['Task2']['LumisPerJob'] = 5
#    request['RequestPriority'] = 40000
#    request['Task1']['EventsPerJob'] = 30000
#    del request['Task3']
#    del request['RequestorDN']
#    del request['DQMUploadUrl']
#    request['Task1']['LumisPerJob'] = 1
#    request['DbsUrl'] = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
#    request['ConfigCacheURL'] = 'https://cmsweb.cern.ch/couchdb'
#    request['Task1']['ProcessingString'] = 'PU_START70_V4_FastSim_testing_a_very_very_long_processing_string_which_is_not_allowed_to_be_used_in_RequestManager_and_dbs3_and_whatever_else_one_could_ever_imagine_to_use_here_and_there'
#    request['Task2']['MCPileup'] = '/RelValMinBias/CMSSW_7_0_0_pre11-START70_V4_TEST_Agent0988_Validation-v6/GEN-SIM'
#    request['Task3']['MCPileup'] = '/RelValMinBias/CMSSW_7_0_0_pre11-START70_V4_TEST_Agent0988_Validation-v6/GEN-SIM'
#    schema['Task1']['BlockBlacklist'] = ['/DoubleMu/Run2011A-ZMu-08Nov2011-v1/RAW-RECO#93c53d22-25b2-11e1-8c62-003048f02c8a']
#    schema['Task1']['RunWhitelist'] = [208307]
#    del schema['Task2']['MCPileup']
#    request['Task1']['BlockWhitelist'] = ['/JetHT/Run2012B-v1/RAW#2b4465b4-a2cd-11e1-86c7-003048caaace']
#    request['DbsUrl'] = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
#    del request['Task1']['RunWhitelist']
    return request

def submitWorkflow(schema):
    for schemaListItem in ["RunWhitelist", "RunBlacklist", "BlockWhitelist", "BlockBlacklist"]:
        if schemaListItem in schema.keys():
            schema[schemaListItem] = str(schema[schemaListItem])
    jsonEncodedParams = {}
    for paramKey in schema.keys():
        jsonEncodedParams[paramKey] = json.dumps(schema[paramKey])

    encodedParams = urllib.urlencode(jsonEncodedParams, False)
           
    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
#    print "  submitting new workflow..."
    conn.request("POST",  "/reqmgr/create/makeSchema", encodedParams, headers)
    response = conn.getresponse()
    print response.status, response.reason
    data = response.read()
    print data
    details=re.search("details\/(.*)\'",data)
    return details.group(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage:"
        print " ./resubmitTaskChain.py WORKFLOW_NAME"
        sys.exit(0)

    schema = retrieveSchema(sys.argv[1])
    #print "\nNew schema:"
    #pprint.pprint(schema)
#    sys.exit(0)
    newWorkflow=submitWorkflow(schema)
    approveRequest(url,newWorkflow)
    sys.exit(0)
