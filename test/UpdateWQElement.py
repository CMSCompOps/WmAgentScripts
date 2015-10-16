from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Database.CMSCouch import CouchServer
couchUrl = "https://cmsweb.cern.ch/couchdb"
base_url = "https://cmsweb.cern.ch/couchdb"
wq_url = "%s/workqueue" % base_url
wq = WorkQueue(wq_url)

#eleIDs = ["b75463e64359fb06e0088646c2ca4f0c", "f45a0ddd615a822bb800bf0dcfca5299"]
#eleIDs = ["71499a32fa73859baad0ce6e03ad9af7", "da5e56bb7074051ef79e11994c02d8c2",
#          "e7d63b22f3e50706ba85a199fc091317", "f37357f6f96fd4f29c9fbc1727b65ef4",
#          "00985eb4c903b46b5c5054aa43f0dd7d", "12fb6c59e77ab658ae4f0f8f79baa1fc",
#          "43461b3c7e66123289e68da5e3b69654", "4790f1c603369279a0ca74a6242f2cf1"]
eleIDs = ["39090377b2f19dd78ac0e2257a1376a4"]
print wq.updateElements(*eleIDs, Status = "Done")

couchdb = CouchServer(couchUrl).connectDatabase("workqueue", False)
options = {}
options["stale"] = "ok"
options["include_docs"] = True
keys = ["pdmvserv_BTV-RunIWinter15DR-00008_00004_v0__150315_132457_3342",
        "pdmvserv_HCA-Fall14DR73-00002_00009_v0__150306_213226_7582",
        "pdmvserv_HIG-Summer12DR53X-02173_00376_v0__150312_144834_4346",
        "pdmvserv_SMP-Summer12DR53X-00018_00374_v0__150301_000335_5218",
        "pdmvserv_SMP-Summer12DR53X-00019_00374_v0__150301_035649_1287",
        "pdmvserv_TOP-Summer12DR53X-00292_00375_v0__150306_175854_1649",
        "pdmvserv_TOP-Summer12DR53X-00293_00375_v0__150306_175855_9433"]
options["reduce"] = False
result = couchdb.loadView("WorkQueue", "elementsByWorkflow", options, keys = keys)
ids = []
for entry in result["rows"]:
    ele = entry["doc"]["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"]
    if ele["Status"] == "Running" and ele['PercentComplete'] == 100:
        ids.append(entry["id"])
        print ele['PercentComplete']
#wq.updateElements(*ids, Status = "Done")
print len(ids)