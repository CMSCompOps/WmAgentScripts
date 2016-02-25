import os
import httplib
from WMCore.Wrappers import JsonWrapper
from WMCore.Database.CMSCouch import Database

def getConfigDoc(url, cacheID, config=False):
    "Helper function to get configuration for given cacheID"
    conn = httplib.HTTPSConnection(url, 
            cert_file = os.getenv('X509_USER_CERT'),
            key_file = os.getenv('X509_USER_KEY'))
    if config:
        cacheID = cacheID + "/configFile" 
    conn.request("GET",'/couchdb/reqmgr_config_cache/'+cacheID)
    config = conn.getresponse().read()
    return config

url = "cmsweb.cern.ch"
configID = "a09fa7ea98f8c88bdae47b19ed049d44"
configID = "1ad063a0d73c1d81143b4182cbf84793"
configID = "47e7c86552e77dd5909308babf4ea377"
configID = "564b9ff125f090d3385206a0b460ed9b"
doc = JsonWrapper.loads(getConfigDoc(url, configID))

print doc
del doc["_rev"]
del doc['_attachments']

workloadString = getConfigDoc(url, configID, True)
print workloadString

testbed = "https://cmsweb-testbed.cern.ch/couchdb"
configdb = "reqmgr_config_cache"

couchdb = Database(configdb, testbed)

if not couchdb.documentExists(configID):
    reDoc = couchdb.putDocument(configID, doc)
    reDoc = JsonWrapper.loads(reDoc)
    rev = reDoc['rev']
else:
    reDoc = couchdb.document(configID)
    rev = reDoc['_rev']
        
result = couchdb.addAttachment(configID, rev, workloadString, 'configFile')
print result