from __future__ import print_function
from WMCoreService.WMStatsClient import WMStatsClient
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection
from WMCoreService.ReqMgrClient import ReqMgrClient

def isResubmission(a):
    return (a.get("request_type", "").lower() == "resubmission")

def allTrue(a):
    return True
    
if __name__ == "__main__":
    baseURL = "https://cmsweb.cern.ch/couchdb"    
    url = "%s/reqmgr_workload_cache" % baseURL
    reqDB = RequestDBReader(url)
    #print "start to getting job information from %s" % url
    #print "will take a while\n"
#     requests = reqDB.getRequestByStatus(["normal-archived", "aborted-archived", "rejected-archived", "announced"], detail = False)
#     filteredRequests = []
#     for request in requests:
#         try:
#             if int(request.split("_")[-3]) > 140000:
#                 filteredRequests.append(request)
#         except:
#             print("Wrong format: %s" % request)

    from WMCore.ACDC.CouchService import CouchService
    acdcService = CouchService(url = baseURL, database = "acdcserver")
    
    reportND = ""
    reportD = ""
    total = 0
    deletedReq = 0
    failed  = ""
    filteredRequests = ["pdmvserv_EXO-Summer12DR53X-02887_T1_US_FNAL_MSS_00202_v0__140416_190735_4701"]
    for req in filteredRequests:
        try:
            deleted = acdcService.removeFilesetsByCollectionName(req)
            if deleted == None:
                reportND += "%s\n" % req
            else:
                num = len(deleted)
                reportD +=  "%s :%s\n" % (req, num)
                total += num
                deletedReq += 1
        except:
            failed += '"%s",' % req
            print("failed to get %s" % req)
            
    f1 = open("failed.txt", "w")
    f2 = open("deleted.txt", "w")
    print(failed, file = f1)
    print(reportD, file = f2)        
    print("total deleted: request: %s doc: %s" % (deletedReq, total))
    print("done")
