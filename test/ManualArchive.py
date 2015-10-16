from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfoCollection

if __name__ == "__main__":
    baseURL = "https://cmsweb-testbed.cern.ch/couchdb"
    url = "%s/wmstats" % baseURL
    reqDBURL = "%s/reqmgr_workload_cache" % baseURL
    testbedWMStats = WMStatsReader(url, reqDBURL)
    reqdbWriter = RequestDBWriter(reqDBURL)
    print "start to getting job information from %s" % url
    print "will take a while\n"
    requests = testbedWMStats.getRequestByStatus(["aborted"], jobInfoFlag = True, legacyFormat = True)
    for requestName, requestInfo in requests.items():
        print requestName + ":" + requestInfo['RequestStatus']
    print len(requests)
    
    requestCollection = RequestInfoCollection(requests)
    result = requestCollection.getJSONData()
    print result
    print "\ntotal %s requests retrieved" % len(result)
    requestsDict = requestCollection.getData()
    needToArchieList = []
    noNeedToArchieList = []
    for requestName, requestInfo in requestsDict.items():
        print requestName + " :"
        print "\tcreated jobs: %s" % requestInfo.getJobSummary().getTotalJobs()
        if requestInfo.getJobSummary().getTotalJobs() == 0:
            print "aborted-completed"
            print reqdbWriter.updateRequestStatus(requestName, "aborted-completed")
            print "aborted-archived"
            print reqdbWriter.updateRequestStatus(requestName, "aborted-archived")
            needToArchieList.append(requestName)
        else:
            noNeedToArchieList.append(requestName)
    print "Need to archive: %s" % len(needToArchieList)
    print "No archive (there is jobs in wmagent) %s" %  len(noNeedToArchieList) 
    print "done"