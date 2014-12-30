from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader



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
     
def deleteWorkQueueElements(couchUrl, requestNames):
    
    dbnames = ["workqueue", "workqueue_inbox"]
    for dbname in dbnames:
        print dbname
        couchdb = CouchServer(couchUrl).connectDatabase(dbname, False)
        options = {}
        options["stale"] = "ok"
        options["reduce"] = False
        result = couchdb.loadView("WorkQueue", "elementsByWorkflow", options, requestNames)
        ids = []
        for entry in result["rows"]:
            ids.append(entry["id"])
            print entry["id"]
        if ids:
            print deleteDocsByIDs(couchdb, ids)
             
class WorkQueueDebug(object):

    """
    API for dealing with retrieving information from WorkQueue DataService
    """

    def __init__(self, couchURL, dbName = None):
        # if dbName not given assume we have to split
        if not dbName:
            couchURL, dbName = splitCouchServiceURL(couchURL)
        self.hostWithAuth = couchURL
        self.server = CouchServer(couchURL)
        self.db = self.server.connectDatabase(dbName, create = False)
        self.defaultOptions = {'stale': "update_after"}
        
    def getElementByStatus(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'elementsByStatus', self.defaultOptions)
        #return [x['key'] for x in data.get('rows', [])]
        return data['rows']

if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch/couchdb"
    wqUrl = "%s/workqueue_inbox" % baseUrl
    wmstatsUrl = "%s/wmstats" % baseUrl
    wqSvc = WorkQueueDebug(wqUrl)
    rows = wqSvc.getElementByStatus()
    print len(rows)
    reqNames = []
    for item in rows:
        reqNames.append(item['id'])
    ws = WMStatsReader(wmstatsUrl)
    requestsInfo = ws.getRequestByNames(reqNames)
    deleteRequests = []
    for key, value in requestsInfo.items():
        if (value["request_status"][-1]['status'].find("-archived") != -1) or \
           (value["request_status"][-1]['status'].find("aborted-completed") != -1) or \
           (value["request_status"][-1]['status'].find("rejected") != -1):
            print key, value["request_status"][-1]['status']
            deleteRequests.append(key)
    print len(deleteRequests)
    deleteWorkQueueElements(baseUrl, deleteRequests)        
#     for item in rows:
#         if item["doc"] == None:
#             print item
    print "done"