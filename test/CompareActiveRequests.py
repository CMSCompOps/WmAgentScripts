from __future__ import print_function
from pprint import pprint
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.ReqMgr.ReqMgrReader import ReqMgrReader
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.RequestManager.RequestDB.Settings.RequestStatus import StatusList
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Wrappers import JsonWrapper

StatusForOutDS = WMStatsReader.ACTIVE_STATUS

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

    wmstatsNumb = 0
    reqmgrNum = 0
    for status in StatusForOutDS:
        
        requests = testbedWMStats.getRequestByStatus([status])

        results = testbedReqMgr.getRequestByStatus([status])
#         for value in results.values():
#             if value and value["RequestStatus"] != status:
#                 misMatchRequests.append(value["RequestName"])

        print("%s: %s: %s" % (status, len(requests), len(results)))
        for req in requests.keys():
            if req not in results:
                print("wmstats: %s: %s" % (status, req))
        for req in results:
            if req not in requests.keys():
                print("%s: %s" % (status, req)) 
        wmstatsNumb += len(requests)
        reqmgrNum += len(results)
                           
    print(wmstatsNumb)
    print(reqmgrNum)
    print ("done")
    