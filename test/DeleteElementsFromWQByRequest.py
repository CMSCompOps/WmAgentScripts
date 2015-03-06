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
    committed = couchDB.commit()
    return committed
     
dbnames = ["workqueue", "workqueue_inbox"]
 
for dbname in dbnames:
    print dbname
    couchdb = CouchServer(couchUrl).connectDatabase(dbname, False)
    options = {}
    options["stale"] = "ok"
    keys = ["alahiff_TAU-2019GEMUpg14DR-00029_00075_v0__150224_140549_6688"]
    options["reduce"] = False
    result = couchdb.loadView("WorkQueue", "elementsByWorkflow", options, keys = keys)
    ids = []
    for entry in result["rows"]:
        ids.append(entry["id"])
        print entry["id"]
    print len(ids)
    if ids:
        deleteDocsByIDs(couchdb, ids)
print "done"
