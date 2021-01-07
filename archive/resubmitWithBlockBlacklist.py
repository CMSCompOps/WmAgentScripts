#!/usr/bin/env python

import os
import sys
import urllib
import httplib
import re

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

def makeNewBlockBlacklist(url, workflow):
        blockBlacklist = ''
        dataset=getInputDataset(url, workflow)
        blocks=os.popen("./dbssql --input='find site,block where block="+dataset+"*' | grep 'block' | colrm 1 13 | sed 's/..$//'").read()
        i=0
        for block in blocks.split('\n'):
           if "/" in block:
              if i>0:
                 blockBlacklist = blockBlacklist + ", "
              blockBlacklist = blockBlacklist + "'" + block + "'"
              i=i+1
        print 'Blacklist = ',blockBlacklist
        return blockBlacklist

def getInputDataset(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/view/showWorkload?requestName='+workflow)
        r2=conn.getresponse()
        workload=r2.read()
        list = workload.split('\n')
        dataset = ''

        for line in list:
           if 'request.schema.InputDataset' in line:
              dataset = line[line.find("'")+1:line.find("'",line.find("'")+1)]

        return dataset

def approveRequest(url,workflow):
    params = {"requestName": workflow,
              "status": "assignment-approved"}

    encodedParams = urllib.urlencode(params)
    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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



def retrieveSchema(workflowName,newBlockBlacklist):
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    helper.load(specURL)
    schema = {}
    for (key, value) in helper.data.request.schema.dictionary_().iteritems():
        #print key
        if key == 'ProdConfigCacheID':
            schema['ProdConfigCacheID'] = value
        elif value != None:
            schema[key] = value

    schema["BlockBlacklist"] = "[" + newBlockBlacklist + "]"

    return schema

def submitWorkflow(schema):
    for schemaListItem in ["RunWhitelist", "RunBlacklist", "BlockWhitelist",
                           "BlockBlacklist"]:
        if schemaListItem in schema.keys():
            schema[schemaListItem] = str(schema[schemaListItem])
            
    encodedParams = urllib.urlencode(schema, True)
    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection("cmsweb.cern.ch", cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("POST",  "/reqmgr/create/makeSchema", encodedParams, headers)
    response = conn.getresponse()
    data = response.read()
    details=re.search("details\/(.*)\'",data)
    return details.group(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage:"
        print "  ./resubmitWithBlockBlacklist.py WORKFLOW_NAME"
        sys.exit(0)

    url='cmsweb.cern.ch'
    newBlockBlackList = makeNewBlockBlacklist(url, sys.argv[1])

    print "Going to attempt to resubmit %s..." % sys.argv[1]
    schema = retrieveSchema(sys.argv[1], newBlockBlackList)
    newWorkflow=submitWorkflow(schema)
    approveRequest('cmsweb.cern.ch',newWorkflow)
    sys.exit(0)
