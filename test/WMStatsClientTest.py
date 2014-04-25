from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection

if __name__ == "__main__":
    url = "https://cmsweb-testbed.cern.ch/couchdb/wmstats"
    testbedWMStats = WMStatsClient(url)
    print "start to getting job information from %s" % url
    print "will take a while\n"
    requests = testbedWMStats.getRequestByStatus(["running-closed"], jobInfoFlag = True)
    requestCollection = RequestInfoCollection(requests)
    result = requestCollection.getJSONData()
    print result
    print "\ntotal %s requests retrieved" % len(result)
    requestsDict = requestCollection.getData()
    for requestName, requestInfo in requestsDict.items():
        print requestName + " :"
        print "\ttotalLuims: %s" % requestInfo.getTotalInputLumis()
        print "\ttotalEvents: %s" % requestInfo.getTotalInputEvents()
        print "\ttotal top level jobs: %s" % requestInfo.getTotalTopLevelJobs()
        print "\ttotal top level jobs in wmbs: %s" % requestInfo.getTotalTopLevelJobsInWMBS()
        print "\tProgress by output dataset:"
        summaryDict = requestInfo.getProgressSummaryByOutputDataset()
        for oDataset, summary in summaryDict.items():
            print "\t  %s" % oDataset
            print "\t   %s" %summary.getReport()
        print "\n"
    print "done"