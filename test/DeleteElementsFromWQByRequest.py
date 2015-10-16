from WMCore.Database.CMSCouch import CouchServer
couchUrl = "https://cmsweb.cern.ch/couchdb"
#couchUrl = "https://cmsweb-testbed.cern.ch/couchdb"

def deleteDocsByIDs(couchDB, ids):
    
    if len(ids) == 0:
        return None
    
    docs = couchDB.allDocs(keys=ids)['rows']
    for j in docs:
        doc = {}
        doc["_id"]  = j['id']
        doc["_rev"] = j['value']['rev']
        couchDB.queueDelete(doc)
        print j['id']
    committed = couchDB.commit()
    print committed
    return committed
     
dbnames = ["workqueue", "workqueue_inbox"]
 
for dbname in dbnames:
    print dbname
    couchdb = CouchServer(couchUrl).connectDatabase(dbname, False)
    options = {}
#    options["stale"] = "ok"
    keys = ["pdmvserv_TOP-RunIISpring15DR74-00053_00058_v0__150514_003820_6493"]
    options["reduce"] = False
    result = couchdb.loadView("WorkQueue", "elementsByWorkflow", options, keys = keys)
    ids = []
    for entry in result["rows"]:
        ids.append(entry["id"])
    print len(ids)
    if ids:
        deleteDocsByIDs(couchdb, ids)
print "done"
