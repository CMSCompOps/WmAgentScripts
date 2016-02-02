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

StatusForOutDS = [
    "normal-archived",
    "rejected-archived",
    "aborted-archived",
    ]

PreviousStatusMap = {
    "normal-archived": "announced",
    "aborted-archived": "aborted-completed",
    "rejected-archived": "rejected"
    }
# StatusForOutDS = WMStatsReader.ACTIVE_STATUS


if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch"
    wmstatsUrl = "%s/couchdb/wmstats" % baseUrl
    reqMgrUrl = "%s/couchdb/reqmgr_workload_cache" % baseUrl
    
    testbedWMStats = WMStatsReader(wmstatsUrl)
    testbedReqMgr = ReqMgrReader(reqMgrUrl, couchapp = "ReqMgr")
    
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
    for status in StatusForOutDS:
        condition = True
        loopCount = 0
        while condition:
            skip = limit * loopCount
            requests = testbedWMStats.getRequestByStatus([status], limit = limit, skip = skip)
            loopCount += 1
            condition = len(requests)
            totalScanned += len(requests)
            print("status %s: %s scanned for all archived" % (status, totalScanned))
            
            results = testbedReqMgr.getRequestByNames(requests.keys())
            for value in results.values():
                if value and value["RequestStatus"] != status:
                    misMatchRequests.append(value["RequestName"])
                    
                    if value["RequestStatus"] == PreviousStatusMap[status]:
                        try:
                            reqMgr.reportRequestStatus(value["RequestName"], status)
                            updateCount += 1
                            print(updateCount)
                        except:
                            print("Update failed: %s %s: %s" % (value["RequestName"], value["RequestStatus"], status))
                    else:
                    
                        if value["RequestStatus"] == "aborted" and status == "aborted-archived":
                            try:
                                reqMgr.reportRequestStatus(value["RequestName"], "aborted-completed")
                                reqMgr.reportRequestStatus(value["RequestName"], "aborted-archived")
                                updateCount += 1
                                print(updateCount)
                            except:
                                print("Very wrong: %s %s: %s" % (value["RequestName"],  value["RequestStatus"], status))
#                         elif value["RequestStatus"] == None and status == "aborted-archived":
#                             reqMgr.reportRequestStatus(value["RequestName"], "aborted")
#                             reqMgr.reportRequestStatus(value["RequestName"], "aborted-completed")
#                             reqMgr.reportRequestStatus(value["RequestName"], "aborted-archived")
                        else:
                            print("%s %s: %s" % (value["RequestName"],  value["RequestStatus"], status))
                            wierdMisMatch[value["RequestName"]] = [value["RequestStatus"], status]
            print("Updated: %s" % updateCount)
                            
    print(len(misMatchRequests))
    print(len(set(misMatchRequests)))
    print(len(wierdMisMatch))
    print(len(set(wierdMisMatch)))
    print ("done")
    
    fileName = "mismatchArchived.txt"
    f = open(fileName, "w")
    JsonWrapper.dump(wierdMisMatch, f)
