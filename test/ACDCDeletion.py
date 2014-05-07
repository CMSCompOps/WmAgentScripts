from __future__ import print_function
from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection
from WMCoreService.ReqMgrClient import ReqMgrClient

def isResubmission(a):
    return (a.get("request_type", "").lower() == "resubmission")
    
if __name__ == "__main__":
        
    url = "https://cmsweb.cern.ch/couchdb/wmstats"
    testbedWMStats = WMStatsClient(url)
    #print "start to getting job information from %s" % url
    #print "will take a while\n"
    requests = testbedWMStats.getRequestByStatus(["normal-archived", "aborted-archived", "rejected-archived"], jobInfoFlag = False)
    requestCollection = RequestInfoCollection(requests)
    #print requests
    filtered = requestCollection.filterRequests(isResubmission)
        
        
    #print "\n\n\n"
    url = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
    testbedReqMgr = WMStatsClient(url)
    #print "start to getting request from %s" % url
        
    requests = testbedReqMgr._getAllDocsByIDs(filtered.keys(), True)
       
    acdcReq = set()
    report = ""
    safeToDelete = set()
    originalRequests = set()
    for request in requests["rows"]:
        originalRequests.add(request['doc']['OriginalRequestName'])
        acdcReq.add(request["id"])
           
    requests = testbedReqMgr._getAllDocsByIDs(list(originalRequests), True)
       
    # saftey checking for request
    for request in requests["rows"]:
        if request['doc']['RequestStatus'] in ["announced", "failed", "rejected", "aborted"]:
            safeToDelete.add(request["id"])
            report += "%s\n" % request["id"]
           
    print(len(originalRequests))
    print(len(safeToDelete))
    print(len(acdcReq))
    fileName = "collectionList.txt"
    f = open(fileName, "w")
   
    print(report, file = f)
    
    from WMCore.ACDC.CouchService import CouchService
    acdcService = CouchService(url = "https://cmsweb.cern.ch/couchdb", database = "acdcserver")
    
    reportND = ""
    reportD = ""
    total = 0
    deletedReq = 0
    for req in safeToDelete:
        deleted = acdcService.removeFilesetsByCollectionName(req)
        if deleted == None:
            reportND += "%s\n" % req
        else:
            num = len(deleted)
            reportD +=  "%s :%s\n" % (req, num)
            total += num
            deletedReq += 1
            
    f1 = open("alreadyDeleted.txt", "w")
    f2 = open("deleted.txt", "w")
    print(reportD, file = f1)
    print(reportND, file = f2)        
    print("total deleted: request: %s doc: %s" % (deletedReq, total))
    print("done")
    
    