from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection

if __name__ == "__main__":
    url = "https://cmsweb.cern.ch/couchdb/wmstats"
    testbedWMStats = WMStatsClient(url)
    print "start to getting job information from %s" % url
    print "will take a while\n"
    requests = testbedWMStats.getRequestByStatus(["aborted"], jobInfoFlag = True)
    for requestName, requestInfo in requests.items():
        print requestName + ":" + requestInfo['request_status'][-1]['status']
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
            print testbedWMStats.updateRequestStatus(requestName, "aborted-completed")
            print "aborted-archived"
            print testbedWMStats.updateRequestStatus(requestName, "aborted-archived")
            needToArchieList.append(requestName)
        else:
            noNeedToArchieList.append(requestName)
    print "Need to archive: %s" % len(needToArchieList)
    print "No archive (there is jobs in wmagent) %s" %  len(noNeedToArchieList) 
    print "done"