from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfoCollection

def moveToArchived(reqmgrdb_url, wmstats_url):
    """
    gather active data statistics
    """
    
    testbedWMStats = WMStatsReader(wmstats_url, reqmgrdb_url)
    reqdbWriter = RequestDBWriter(reqmgrdb_url)
    
     
    
    statusTransition = {"aborted": ["aborted-completed", "aborted-archived"],
                        "rejected": ["rejected-archived"]}
    
    for status, nextStatusList in statusTransition.items():
        
        requests = testbedWMStats.getRequestByStatus([status], 
                                            jobInfoFlag = True, legacyFormat = True)
        
        print "checking %s workflows: %d" % (status, len(requests))
        
        if len(requests) > 0:
        
            requestCollection = RequestInfoCollection(requests)
            
            requestsDict = requestCollection.getData()
            numOfArchived = 0
            
            for requestName, requestInfo in requestsDict.items():
                
                if requestInfo.getJobSummary().getTotalJobs() == 0:
                    for nextStatus in nextStatusList: 
                        reqdbWriter.updateRequestStatus(requestName, nextStatus)
                    numOfArchived += 1
            
            print "Total %s-archieved: %d" % (status, numOfArchived)  
              
    return

if __name__ == "__main__":
    rurl = "https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache"
    wurl = "https://cmsweb-testbed.cern.ch/couchdb/wmstats"
    moveToArchived(rurl, wurl)