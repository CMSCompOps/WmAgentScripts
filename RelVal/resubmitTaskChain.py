#!/usr/bin/env python

import os
import sys
import urllib
import httplib
import re
import json
from copy import deepcopy
#from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
import optparse

url = "cmsweb.cern.ch"
reqmgrCouchURL = "https://"+url+"/couchdb/reqmgr_workload_cache"
#reqmgrHostname = "vocms144"
#reqmgrPort = 8687

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
#    for (key, value) in helper.data.request.schema.dictionary_().iteritems():
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
        elif value != None:
            schema[key] = value
#    print "Retrieved schema:\n", schema   ### FOR DEBUG
    request = deepcopy(schema)
    request['Requestor'] = 'anlevin'
    request['Group'] = 'DATAOPS'
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

    #request['Memory'] = 3900
    #del request['SiteWhitelist']        
    #request['RequestString'] = 'TEST_ALAN_TEST'
    #request['Task1']['BlockWhitelist'] = ['/RelValQCD_Pt_80_170_BCtoE_8TeV/CMSSW_6_2_0_pre8-PRE_ST62_V8-v3/GEN-SIM#d99587e0-625e-11e3-ad0f-00221959e7c0','/RelValQCD_Pt_80_170_BCtoE_8TeV/CMSSW_6_2_0_pre8-PRE_ST62_V8-v3/GEN-SIM#8689de68-606d-11e3-ad0f-00221959e7c0']
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
    conn.request("POST",  "/reqmgr/create/makeSchema", encodedParams, headers)
    response = conn.getresponse()
    print response.status, response.reason
    data = response.read()
    print data
    details=re.search("details\/(.*)\'",data)
    return details.group(1)

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('--correct_env',action="store_true",dest='correct_env')
    (options,args) = parser.parse_args()

    command=""
    for arg in sys.argv:
        command=command+arg+" "

    if not options.correct_env:
         os.system("source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh; source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh; python2.6 "+command + "--correct_env")
         sys.exit(0)

    if len(args) != 1:
        print "Usage:"
        print " ./resubmitTaskChain.py WORKFLOW_NAME"
        sys.exit(0)

    from WMCore.WMSpec.WMWorkload import WMWorkloadHelper    

    schema = retrieveSchema(sys.argv[1])
#    print "\nNew schema:\n", schema     # FOR DEBUG
#    sys.exit(0)
    newWorkflow=submitWorkflow(schema)
    approveRequest(url,newWorkflow)
    sys.exit(0)
