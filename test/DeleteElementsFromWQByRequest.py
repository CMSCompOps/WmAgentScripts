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
    options["key"] = "franzoni_2013APPJet_5323_141216_160701_6065"
    options["reduce"] = False
    result = couchdb.loadView("WorkQueue", "elementsByWorkflow", options)
    ids = []
    for entry in result["rows"]:
        ids.append(entry["id"])
        print entry["id"]
    print len(ids)
    if ids:
        deleteDocsByIDs(couchdb, ids)
print "done"
