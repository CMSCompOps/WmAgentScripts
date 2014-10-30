from __future__ import print_function
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.RequestManager.RequestDB.Settings.RequestStatus import StatusList
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter

StatusForOutDS = [
    "assigned",
    "negotiating",
    "acquired",
    "running",
    "running-open",
    "running-closed",
    "completed",
    "closed-out",
    "announced",
    ]

def printFormat(requests, desc, fileName):
    report =  "**** %s : %s ****\n" % (desc, len(requests))
    for requestName in requests:
        report += "%s\n" % requestName
    f = open(fileName, "w")
    print(report, file = f)
    
def fixOutputDataset(wmstatsSrv, requestName, outputDSList):
    """
    bulk update for request documents.
    TODO: change to bulk update handler when it gets supported
    """
    outputDS = {"outputdatasets": outputDSList}
    result = wmstatsSrv.couchDB.updateDocument(requestName, wmstatsSrv.couchapp,
                                               'generalFields',
                                               fields={'general_fields': JSONEncoder().encode(outputDS)})
    return result

def fixOutputDatasetReqMgr(reqMgrSrv, requestName, outputDSList):
    """
    bulk update for request documents.
    TODO: change to bulk update handler when it gets supported
    """
    outputDS = {"OutputDatasets": outputDSList}
    result = reqMgrSrv.updateRequestProperty(requestName, outputDS)
    return result

def getOutputDSFromSpec(request):
    wh = WMWorkloadHelper()
    reqmgrSpecUrl = "%s/reqmgr_workload_cache/%s/spec" % (baseUrl, request)
    wh.load(reqmgrSpecUrl)
    candidate = wh.listOutputDatasets()
    return candidate

def updateOutputDataset(reqMgrDB, limit, skip):
    requests = reqMgrDB.getRequestByStatus(["normal-archived"], True, limit, skip)
    #requests = reqMgrDB.getRequestByStatus(StatusForOutDS, True, 1)
    #requests = reqMgrDB.getRequestByNames(["amaltaro_RVCMSSW_7_0_0_pre11TTbar_140128_155743_8106"])
    wrongOutDS = set()
    missingOutDS = set()
    for key, value in requests.items():
        reqName = key
        if value.has_key("OutputDatasets"):
            for outputDS in value["OutputDatasets"]:
                if "None" in outputDS:
                    #print reqName, outputDS
                    wrongOutDS.add(reqName)
                    print ("%s:%s" % (key, value["OutputDatasets"]))
        else:
            missingOutDS.add(reqName)
      
    print (len(wrongOutDS))
    print (len(missingOutDS))
   
   
    problemRequests = set()
    fixedRequests = set()
    for request in wrongOutDS:
        candidate = getOutputDSFromSpec(request)
        problemFlag = False
        if len(candidate) == 0:
            problemFlag = True
        for canOutDS in candidate:
            if "None" in canOutDS:
                problemFlag = True
                problemRequests.add(request)
                #print(canOutDS)
                    
        if not problemFlag:
            print (request)
            print(candidate)
            if fixOutputDatasetReqMgr(reqMgrDB, request, candidate) != "OK":
                print("update failed: %s" % request)
            else:
                fixedRequests.add(request)
  
      
    for request in missingOutDS:
        candidate = getOutputDSFromSpec(request)
        problemFlag = False
        if len(candidate) == 0:
            problemFlag = True
        for canOutDS in candidate:
            if "None" in canOutDS:
                problemFlag = True
                problemRequests.add(request)
                #print(canOutDS)
                    
        if not problemFlag:
            if fixOutputDatasetReqMgr(reqMgrDB, request, candidate) != "OK":
                print("update failed: %s" % request)
            else:
                fixedRequests.add(request)
                    
    print("still problem requests %s" % len(problemRequests)) 
    print("fixed requests %s" % len(fixedRequests))
    return len(requests)
    
if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch/couchdb"
#     url = "%s/wmstats" % baseUrl
#     testbedWMStats = WMStatsReader(url)
#      
#     requests = testbedWMStats.getRequestByStatus(StatusForOutDS, False)
#     requestsWithWrongOutputDS = set()
#     problemRequests = set()
#     fixedRequests = set()
#     for reqName, prop in requests.items():
#         if prop.has_key("outputdatasets"):
#             for outputDS in prop["outputdatasets"]:
#                 if "None" in outputDS:
#                     requestsWithWrongOutputDS.add(reqName)
#                     
#     print("wrong output %s" % len(requestsWithWrongOutputDS))
#        
#     destWMStats = WMStatsWriter(url)
#         
#     for request in requestsWithWrongOutputDS:
#         wh = WMWorkloadHelper()
#         reqmgrSpecUrl = "%s/reqmgr_workload_cache/%s/spec" % (baseUrl, request)
#         wh.load(reqmgrSpecUrl)
#         candidate = wh.listOutputDatasets()
#         problemFlag = False
#         for canOutDS in candidate:
#             if "None" in canOutDS:
#                 problemFlag = True
#                 problemRequests.add(request)
#                  
#         if not problemFlag:
#             if fixOutputDataset(destWMStats, request, candidate) != "OK":
#                 print("update failed: %s" % request)
#             fixedRequests.add(request)
#                  
#     print("still problem requests %s" % len(problemRequests))
#     
#     printFormat(requestsWithWrongOutputDS, "Wrong output ds", "wrongOutDS.txt")
#     printFormat(fixedRequests, "fixed wrong output ds", "fixedWrongOutDS.txt")
#     printFormat(problemRequests, "Still wrong output ds", "stillWrongOutDS.txt")
#     print("all done")
    
    reqMgrUrl = "%s/reqmgr_workload_cache" % baseUrl
    reqMgrDB = RequestDBWriter(reqMgrUrl, couchapp = "ReqMgr")
    
    print("updating output dataset")
    
    limit = 500
    skip = 0
    totalScanned = 0
    condition = True
    while condition:
        condition = updateOutputDataset(reqMgrDB, limit, skip)
        skip += limit
        totalScanned += condition
        print(totalScanned)
        
    print ("all done: %s scanned" % totalScanned) 