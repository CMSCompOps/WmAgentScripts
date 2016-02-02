from pprint import pprint
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.ReqMgr.ReqMgrReader import ReqMgrReader
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.RequestManager.RequestDB.Settings.RequestStatus import StatusList
from WMCore.Database.CMSCouch import CouchServer, Database
from WMCore.Wrappers import JsonWrapper

StatusForOutDS = [
    "announced"
    ]
 
STATUS = ["normal-archived"]

# StatusForOutDS = [
#     "rejected"
#     ]
# 
# STATUS = ["rejected-archived"]
# # StatusForOutDS = WMStatsReader.ACTIVE_STATUS


if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch"
    wmstatsUrl = "%s/couchdb/wmstats" % baseUrl
    reqMgrUrl = "%s/couchdb/reqmgr_workload_cache" % baseUrl
    
    testbedWMStats = WMStatsReader(wmstatsUrl)
    testbedReqMgr = ReqMgrReader(reqMgrUrl)
    args = {}
    args["endpoint"] = "%s/reqmgr/rest" % baseUrl
    reqMgr = RequestManager(args)
    
    print (testbedWMStats.couchURL)
    print (testbedWMStats.dbName)
    misMatchRequests = []
    wierdMisMatch = {}
    limit = 1000
    updateCount = 0
    totalScanned = 0
    
    couchDb = Database("reqmgr_workload_cache", "%s/couchdb" % baseUrl)
    
    for status in StatusForOutDS:
        
        requests = testbedWMStats.getRequestByStatus([status])
        #print requests

        results = testbedReqMgr.getRequestByStatus([status])
        #print results
        for reqName in results:
            if not requests.has_key(reqName):
                for nextStatus in STATUS:
                    try:
                        couchDb.updateDocument(reqName, "ReqMgr", "updaterequest",
                               fields={"RequestStatus": nextStatus})
                    except Exception, ex:
                        print reqName, nextStatus
                        raise ex
                updateCount += 1
                if (updateCount % 100) == 0:
                    print updateCount
    
    print updateCount
    print ("done")