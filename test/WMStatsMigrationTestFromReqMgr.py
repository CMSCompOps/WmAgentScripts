from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.CouchClient import Document
from WMCoreService.ReqMgrClient import ReqMgrClient

def convertStateTransition(stateTransition):
    converted = []
    for item in stateTransition:
        convertedItem = {}
        convertedItem["status"] = item["Status"]
        convertedItem["update_time"] = item["UpdateTime"]
        converted.append(convertedItem)
    return converted

def mapToWMStatsDoc(doc):
    url = "https://cmsweb.cern.ch"
    reqMgr = ReqMgrClient(url)
    results = reqMgr.queryRequests(None, doc.get("RequestName"))
    
    mapDoc = {
           "inputdataset": doc.get("InputDatasets", []),
           "vo_group": "",
           "prep_id": doc.get("PrepID", ""),
           "group": doc.get("Group", ""),
           "request_date": doc.get("RequestDate", ""),
           "campaign": doc.get("Campaign", ""),
           "workflow": doc.get("RequestName"),
           "user_dn": doc.get("RequestorDN", "None"),
           "vo_role": "",
           "priority": doc.get("RequestPriority"),
           "requestor": doc.get("Requestor"),
           "request_type": doc.get("RequestType"),
           "publish_dbs_url": "",
           "dbs_url": doc.get("DbsUrl", ""),
           "cmssw": results[0]["SoftwareVersions"],
           "async_dest": "",
           "type": "reqmgr_request",
           "outputdatasets": doc.get("OutputDatasets", []),
           "request_status": convertStateTransition(doc.get("RequestTransition", [])),
           "site_white_list": results[0]["Site Whitelist"],
           "teams": results[0]["teams"],
           "total_jobs": doc.get("TotalEstimatedJobs"),
           "input_events": doc.get("TotalInputEvents"),
           "input_lumis": doc.get("TotalInputLumis"),
           "input_num_files": doc.get("TotalInputFiles")
        }
    return mapDoc

if __name__ == "__main__":
    url = "https://cmsweb.cern.ch/couchdb/wmstats"
    testbedWMStats = WMStatsClient(url)
    print "start to getting request from %s" % url
    options = {}
    options["include_docs"] = False
    options["group_level"] = 1
    options["reduce"] = True 
    options["stale"] = "update_after"
    requests = testbedWMStats._getCouchView("latestRequest", options)
       
    reqNames = []
    for row in requests["rows"]:
        reqNames.append(row["key"][0])
       
    requests = testbedWMStats._getAllDocsByIDs(reqNames, False)
       
    print "total request from agent: %s" % len(reqNames)
    missingR = []
    for v in requests["rows"]:
        if v.has_key('error'):
            missingR.append(v["key"])
   
    print "total missing reqeust: %s" % len(missingR)
    print "\n\n\n"
    for m in missingR:
        print m
          
    print "\n\n\n"
    url = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
    testbedWMStats = WMStatsClient(url)
    print "start to getting request from %s" % url
    
    requests = testbedWMStats._getAllDocsByIDs(missingR, True)
    print "results from :%s" % url
    print len(requests['rows'])
    url = "http://localhost:5984/wmstats"
    destWMStats = WMStatsClient(url)
    i = 0
    for v in requests["rows"]:
#         for key, value in v["doc"].items():
#             print "%s : %s" % (key, value)
        doc = Document(v["id"], mapToWMStatsDoc(v["doc"]))   
        print destWMStats.couchdb.commitOne(doc)
        i += 1
    print "All done %s" % i    