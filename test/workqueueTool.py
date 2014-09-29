from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Database.CMSCouch import CouchServer
couchUrl = "https://cmsweb.cern.ch/couchdb"
#couchUrl = "https://cmsweb-testbed.cern.ch/couchdb"
qUrl = "%s/workqueue" % couchUrl 
queueParams = {'QueueURL': qUrl, "CouchUrl": couchUrl}

# gq = globalQueue(**queueParams)
# print "start"
# print gq.updateLocationInfo()
# print "done"

def deleteDocsByIDs(couchDB, ids):
    
    if len(ids) == 0:
        return None
    
    docs = couchDB.allDocs(keys=ids)['rows']
    for j in docs:
        doc = {}
        doc["_id"]  = j['id']
        doc["_rev"] = j['value']['rev']
        couchDB.queueDelete(doc)
    committed = couchDB.commit()
    return committed
     
dbnames = ["workqueue", "workqueue_inbox"]
# dbnames = ["workqueue"]
# couchdb = CouchServer(couchUrl).connectDatabase("workqueue", False)
# print deleteDocsByIDs(couchdb, ['616d274173d24d36058ab4f2469d0134'])
 
for dbname in dbnames:
    print dbname
    couchdb = CouchServer(couchUrl).connectDatabase(dbname, False)
    options = {}
    options["stale"] = "ok"
    options["key"] = "franzoni_RVCMSSW_7_2_0_pre4Pythia6_BuJpsiK_TuneZ2star_13_140815_114436_9710"
    options["reduce"] = False
    result = couchdb.loadView("WorkQueue", "elementsByWorkflow", options)
    ids = []
    for entry in result["rows"]:
        ids.append(entry["id"])
        print entry["id"]
    if ids:
        deleteDocsByIDs(couchdb, ids)
print "done"
