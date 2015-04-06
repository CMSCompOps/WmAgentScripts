from WMCore.Database.CMSCouch import CouchServer
from pprint import pprint
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Wrappers import JsonWrapper

fileName = "RecoverQ.txt"
f = open(fileName, "a")

couchUrl = "https://cmsweb.cern.ch/couchdb"
#couchUrl = "https://cmsweb-testbed.cern.ch/couchdb"
#qUrl = "%s/workqueue" % couchUrl 
#queueParams = {'QueueURL': qUrl, "CouchUrl": couchUrl}
dbname = "workqueue"
couchdb = CouchServer(couchUrl).connectDatabase(dbname, False)
reqMgrUrl = "https://cmsweb.cern.ch/reqmgr2"
reqMgr = ReqMgr(reqMgrUrl)
reqDB = RequestDBReader("%s/reqmgr_workload_cache" % couchUrl)
ids = []
workflows = set()
items = []
for item in couchdb.changes(223000)['results']:
    if item.has_key('deleted') and item["deleted"]:
        
        try:
            preDoc = couchdb.getPrevisousRevision(item["id"])
        except Exception, ex:
            if item["id"].find("_") != -1:
                #pprint(reqMgr.getRequestByNames(item["id"]))
                #print "Error: %s" % item["id"]
                ids.append(item["id"])
        
        else:
            eleStr = 'WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'
            if preDoc.has_key(eleStr):
                lastStatus = preDoc[eleStr]['Status']
                #print lastStatus
                #workflows.add(preDoc[eleStr]['ParentQueueId'])
                if lastStatus not in ['Canceled', 'Done']:
                    items.append(preDoc)
                    docid = preDoc["_id"]
                    newDoc = {}
                    newDoc[eleStr] = preDoc[eleStr]
                    newDoc["thunker_encoded_json"] = preDoc["thunker_encoded_json"]
                    newDoc["timestamp"] = preDoc["timestamp"]
                    newDoc["type"] = preDoc["type"]
                    newDoc["updatetime"] = preDoc["updatetime"]
                    couchdb.putDocument(docid, newDoc)
                    print docid
print len(items)
JsonWrapper.dump(items, f)
f.close()
                    
                    
#                     print lastStatus
#                     print "xxx"
#                     print preDoc[eleStr]['ParentQueueId']
#                     print "ooo"

#pprint(workflows)
#pprint(reqMgr.getRequestByNames(ids))
# byStatus = {}
# for reqName, value in reqDB.getStatusAndTypeByRequest(ids).items():
#     byStatus.setdefault(value[0], [])
#     byStatus[value[0]].append(reqName)
    
#pprint(byStatus)
 
print "*********"
print "All Done"
#print couchdb.allDocs({}, ids)
#print couchdb.getRevision("6130293acd7029436f32a87dac9bf9fa")