#!/usr/bin/env python

import os
import sys
import urllib
import httplib

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
#reqmgrHostname = "vocms144"
#reqmgrPort = 8687

def retrieveSchema(workflowName):
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    print "  retrieving original workflow...",
    helper.load(specURL)
    print "done."
    schema = {}
    for (key, value) in helper.data.request.schema.dictionary_().iteritems():
        #print key
        if key == 'ProdConfigCacheID':
            schema['ProdConfigCacheID'] = value
        elif value != None:
            schema[key] = value
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
    #conn  =  httplib.HTTPConnection("%s:%s" % (reqmgrHostname, reqmgrPort))
    print "  submitting new workflow..."
    conn.request("POST",  "/reqmgr/create/makeSchema", encodedParams, headers)
    response = conn.getresponse()
    print response.status, response.reason
    data = response.read()
    print data
    conn.close()
    return

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage:"
        print "  ./resubmit WORKFLOW_NAME"
        sys.exit(0)

    print "Going to attempt to resubmit %s..." % sys.argv[1]
    schema = retrieveSchema(sys.argv[1])
    submitWorkflow(schema)
    sys.exit(0)
