from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection

if __name__ == "__main__":
    url = "https://cmsweb.cern.ch/couchdb/wmstats"
    testbedWMStats = WMStatsClient(url)
    print "start to getting job information from %s" % url
    print "will take a while\n"
    requests = testbedWMStats.getRequestByStatus(["aborted-completed"], jobInfoFlag = True)
    requestCollection = RequestInfoCollection(requests)
    result = requestCollection.getJSONData()
    print result
    print "\ntotal %s requests retrieved" % len(result)
    requestsDict = requestCollection.getData()
    needToArchieList = []
    for requestName, requestInfo in requestsDict.items():
        print requestName + " :"
        print "\tcreated jobs: %s" % requestInfo.getJobSummary().getTotalJobs()
        if requestInfo.getJobSummary().getTotalJobs() == 0:
            #testbedWMStats.updateRequestStatus(requestName, "aborted-completed")
            testbedWMStats.updateRequestStatus(requestName, "aborted-archieved")
            needToArchieList.append(requestName)
    print "Need to archive: %s" % len(needToArchieList)   
    print "done"