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

StatusForOutDS = {"announced": ["normal-archived"],
                  "rejected": ["rejected-archived"],
                  "aborted": ["aborted-completed", "aborted-archived"]}

if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch"
    reqMgrUrl = "%s/couchdb/reqmgr_workload_cache" % baseUrl
    workloadSummaryUrl = "%s/couchdb/workloadsummary" % baseUrl
    
    testbedReqMgr = ReqMgrReader(reqMgrUrl)
    testbedWorkloadSummary = WMStatsReader(workloadSummaryUrl)
    
    args = {}
    args["endpoint"] = "%s/reqmgr/rest" % baseUrl
    reqMgr = RequestManager(args)
    
    print (testbedReqMgr.couchURL)
    print (testbedReqMgr.dbName)
  
    for status in StatusForOutDS.keys():
        updateCount = 0
        requests =  testbedReqMgr.getRequestByStatus([status])
        print("%s: %s" % (status, len(requests)))
        results = testbedWorkloadSummary._getAllDocsByIDs(requests, False)
        if results == None:
            print ("From workload summary %s: %s" % (status, 0))
            continue
        print("From workload summary %s: %s" % (status, len(results['rows'])))
        requestsDict = {}
        for row in results['rows']:
            if not row.has_key('error'):
                requestsDict[row['id']] = None
                #print(row['id'])
        for requestName in requests:
            if requestsDict.has_key(requestName):
                for newState in StatusForOutDS[status]:
                    reqMgr.reportRequestStatus(requestName, newState)
                updateCount += 1
                      
        print("update Count %s" % updateCount)
        
        