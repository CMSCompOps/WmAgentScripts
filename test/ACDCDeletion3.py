from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader

def acdcCleanup(reqmgrdb_url, acdc_url):
    """
    gather active data statistics
    """
    
    reqDB = RequestDBReader(reqmgrdb_url)

    from WMCore.ACDC.CouchService import CouchService
    baseURL, acdcDB = splitCouchServiceURL(acdc_url)
    acdcService = CouchService(url = baseURL, database = acdcDB)
    originalRequests = acdcService.listCollectionNames()
    
    if len(originalRequests) == 0:
        return 
    # filter requests
    
    results = reqDB._getCouchView("byrequest", {}, originalRequests)
    #print results
    # checkt he status of the requests [announced, rejected-archived, aborted-archived, normal-archived]
    deleteStates = ["announced", "rejected-archived", "aborted-archived", "normal-archived"]
    filteredRequests = []
    for row in results["rows"]:
        if row["value"][0] in deleteStates:
            filteredRequests.append(row["key"])
             
    total = 0
    for req in filteredRequests:
        try:
            deleted = acdcService.removeFilesetsByCollectionName(req)
            if deleted == None:
                print "request alread deleted %s" % req
            else:
                total += len(deleted)
                print "request %s deleted" % req
        except:
            print "request deleted failed: will try again %s" % req
    print "total %s requests deleted" % total        
    return

if __name__ == "__main__":
    rurl = "https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache"
    aurl = "https://cmsweb-testbed.cern.ch/couchdb/acdcserver"
    acdcCleanup(rurl, aurl)