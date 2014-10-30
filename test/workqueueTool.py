from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Database.CMSCouch import CouchServer
#couchUrl = "https://cmsweb.cern.ch/couchdb"
couchUrl = "https://cmsweb-testbed.cern.ch/couchdb"
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
    #options["key"] = "hernan_MultiCoreTest_64-cores_513_01_T1_ES_PIC_120625_155913_2050"
    #options["key"] = "hernan_IntegrationTest_PK_02_MCTest_T2_ES_CIEMAT_120208_184947"
    #options["key"] = "hernan_IntegrationTest_PK_02_MCTest_T2_UK_London_IC_120208_184905"
    #options["key"] = "samir_Samir2_130927_111013_5972"
    options["key"] = "samir_Samir8_IN2P3_131003_173356_6400"
    options["key"] = "hernan_IntegrationTest_PK_02_MCTest_ManySites_120208_185008"
    options["reduce"] = False
    result = couchdb.loadView("WorkQueue", "elementsByWorkflow", options)
    ids = []
    for entry in result["rows"]:
        ids.append(entry["id"])
        print entry["id"]
    if ids:
        deleteDocsByIDs(couchdb, ids)
print "done"
