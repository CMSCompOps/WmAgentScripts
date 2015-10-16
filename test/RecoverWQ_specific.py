import RecoverQ
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Database.CMSCouch import CouchConflictError
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from pprint import pprint

fileName = "RecoverQ.txt"
f = open(fileName, "a")

couchUrl = "https://cmsweb.cern.ch/couchdb"
#couchUrl = "https://cmsweb-testbed.cern.ch/couchdb"
#qUrl = "%s/workqueue" % couchUrl 
#queueParams = {'QueueURL': qUrl, "CouchUrl": couchUrl}
dbname = "workqueue"
couchdb = CouchServer(couchUrl).connectDatabase(dbname, False)
reqDB = RequestDBReader("%s/reqmgr_workload_cache" % couchUrl)

print len(RecoverQ.ra)
r = RecoverQ.ra
a = 0
updated = 0
conflicted = 0
eleStr = 'WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'
workflow = set()
for preDoc in r:
    a += 1
    #if preDoc['_id'] == 'fefbeb2820a4c204fd168237ad78053e':
    #   print a
    #if preDoc['_id'] == 'c679098eb14a6198908d15d18846313c':
    #    print a
    #if preDoc['_id'] == '94d51add28360f6a7055340c8f680b4f':
    #    print a
    #if a > 800:
    docid = preDoc["_id"]
    newDoc = {}
    newDoc[eleStr] = preDoc[eleStr]
    newDoc["thunker_encoded_json"] = preDoc["thunker_encoded_json"]
    newDoc["timestamp"] = preDoc["timestamp"]
    newDoc["type"] = preDoc["type"]
    newDoc["updatetime"] = preDoc["updatetime"]
    if newDoc[eleStr]["Status"] != "Available" and newDoc[eleStr]["Status"]  != "Acquired":
        #print docid
        #print newDoc[eleStr]["Status"]
        workflow.add(newDoc[eleStr]["ParentQueueId"])
#         try:
#             couchdb.putDocument(docid, newDoc)
#             print a
#             print docid
#             updated += 1
#         except CouchConflictError:
#             conflicted += 1
#             pass
#         except Exception, ex:
#             print a
#             raise ex

ids = list(workflow)
byStatus = {}
print len(ids)
for reqName, value in reqDB.getStatusAndTypeByRequest(ids).items():
    byStatus.setdefault(value[0], [])
    byStatus[value[0]].append(reqName)
    
pprint(byStatus)
print conflicted
print updated
print "done"
