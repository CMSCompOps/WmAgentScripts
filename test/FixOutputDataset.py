from __future__ import print_function
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.RequestManager.RequestDB.Settings.RequestStatus import StatusList

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
    "normal-archived",
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

if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch/couchdb"
    url = "%s/wmstats" % baseUrl
    testbedWMStats = WMStatsReader(url)
     
    requests = testbedWMStats.getRequestByStatus(StatusForOutDS, False)
    requestsWithWrongOutputDS = set()
    problemRequests = set()
    fixedRequests = set()
    for reqName, prop in requests.items():
        if prop.has_key("outputdatasets"):
            for outputDS in prop["outputdatasets"]:
                if "None" in outputDS:
                    requestsWithWrongOutputDS.add(reqName)
                    
    print("wrong output %s" % len(requestsWithWrongOutputDS))
       
    destWMStats = WMStatsWriter(url)
        
    for request in requestsWithWrongOutputDS:
        wh = WMWorkloadHelper()
        reqmgrSpecUrl = "%s/reqmgr_workload_cache/%s/spec" % (baseUrl, request)
        wh.load(reqmgrSpecUrl)
        candidate = wh.listOutputDatasets()
        problemFlag = False
        for canOutDS in candidate:
            if "None" in canOutDS:
                problemFlag = True
                problemRequests.add(request)
                 
        if not problemFlag:
            if fixOutputDataset(destWMStats, request, candidate) != "OK":
                print("update failed: %s" % request)
            fixedRequests.add(request)
                 
    print("still problem requests %s" % len(problemRequests))
    
    printFormat(requestsWithWrongOutputDS, "Wrong output ds", "wrongOutDS.txt")
    printFormat(fixedRequests, "fixed wrong output ds", "fixedWrongOutDS.txt")
    printFormat(problemRequests, "Still wrong output ds", "stillWrongOutDS.txt")
    print("all done")
    
    
 
 
 
 
#     reqMgrDB = CouchServer(baseUrl).connectDatabase("reqmgr_workload_cache", False)
#     requests = reqMgrDB.allDocs({"include_docs": True})
#     wrongOutDS = set()
#     missingOutDS = set()
#     for row in requests['rows']:
#         reqName = row['id']
#         if row['doc'].has_key("OutputDatasets"):
#             for outputDS in row['doc']["OutputDatasets"]:
#                 if "None" in outputDS:
#                     #print reqName, outputDS
#                     wrongOutDS.add(reqName)
#         else:
#             missingOutDS.add(reqName)
#     
#     print len(wrongOutDS)
#     print len(missingOutDS)
    
#     testbedWMStats = WMStatsReader(url)
#     print "start to getting request from %s" % url
#      
#     requests = testbedWMStats._getAllDocsByIDs(missingR, True)
#     print "results from :%s" % url
#     print len(requests['rows'])
#     url = "http://localhost:5984/wmstats"
#     destWMStats = WMStatsClient(url)
#     i = 0
#     for v in requests["rows"]:
# #         for key, value in v["doc"].items():
# #             print "%s : %s" % (key, value)
#         doc = Document(v["id"], mapToWMStatsDoc(v["doc"]))   
#         print destWMStats.couchdb.commitOne(doc)
#         i += 1
#     print "All done %s" % i    