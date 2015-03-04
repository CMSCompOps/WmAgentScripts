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
    keys = ["pdmvserv_EXO-RunIIWinter15GS-00260_00018_v0__150219_203137_8002",
            "pdmvserv_EXO-RunIIWinter15GS-00259_00018_v0__150219_203143_1934"
            "pdmvserv_EXO-RunIIWinter15GS-00257_00018_v0__150219_203127_3408",
            "pdmvserv_EXO-RunIIWinter15GS-00267_00018_v0__150219_203151_2405",
            "pdmvserv_BTV-2019GEMUpg14DR-00013_00075_v0__150220_161306_6017",
            "pdmvserv_EXO-RunIIWinter15GS-00260_00018_v0__150219_203137_8002"
            "pdmvserv_EXO-RunIIWinter15GS-00257_00018_v0__150219_203127_3408",
            "pdmvserv_EXO-RunIIWinter15GS-00263_00018_v0__150219_203142_3879",
            "pdmvserv_HIG-2019GEMUpg14DR-00074_00075_v0__150220_161352_7819",
            "pdmvserv_EXO-2019GEMUpg14DR-00029_00075_v0__150220_161334_2222",
            "pdmvserv_EXO-RunIIWinter15GS-00262_00017_v0__150219_203101_4350",
            "pdmvserv_EXO-RunIIWinter15GS-00251_00017_v0__150219_202914_8701",
            "dmason_FSQ-RunIIFall14GS-Backfill-00001_00004_v1__150203_211550_1060",
            "pdmvserv_JME-2019GEMUpg14DR-00028_00074_v0__150219_143901_5821",
            "pdmvserv_BPH-2019GEMUpg14DR-00013_00075_v0__150220_161312_9935",
            "pdmvserv_TRK-2019GEMUpg14DR-00007_00075_v0__150220_161428_5615",
            "dmason_FSQ-RunIIFall14GS-Backfill-00001_00004_v1__150218_053230_3925"]
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
