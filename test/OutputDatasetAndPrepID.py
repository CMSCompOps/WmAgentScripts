from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
from WMCore.Lexicon import splitCouchServiceURL
from pprint import pprint    

def getOutputDatasetPrepID():
    
    baseUrl = "https://cmsweb.cern.ch/couchdb"
#     wmstatsDBName = "wmstats"
#     server = CouchServer(baseUrl)
#     wmstatsDB = server.connectDatabase(wmstatsDBName, create = False)
#    data = wmstatsDB.loadView('WMStats', 'requestByPrepID', 
#                              {'stale': "update_after", 'include_docs' : True})
    
    reqmgrDBName = "reqmgr_workload_cache"
    server = CouchServer(baseUrl)
    reqmgrDB = server.connectDatabase(reqmgrDBName, create = False)
    
    data = reqmgrDB.loadView('ReqMgr', 'byprepid', 
                              {'stale': "update_after", 'include_docs' : True})
    result = {}
    problemDS = {}
    for row in data['rows']:
        prepID = row["doc"]["PrepID"]
        status = row["doc"]["RequestStatus"]
        if status == None:
            continue
        if (status.find("rejected") != -1 or 
            status.find("aborted") != -1 or
            status.find("failed") != -1 or
            status.find("rejected-archived") != -1 or 
            status.find("aborted-archived") != -1):
            continue
        
        for outDS in row["doc"]["OutputDatasets"]:
            if type(outDS) == list:
                #print "%s: %s" % (row["doc"]['workflow'], outDS)
                outDS = outDS[0]
            if result.has_key(outDS) and prepID != result[outDS]:
                if outDS.find("None-") == -1:
                    problemDS.setdefault(outDS, set())
                    problemDS[outDS].add(result[outDS])
                    problemDS[outDS].add(prepID)
            elif prepID != None:
                result[outDS] = prepID
            else:
                print "%s has None PrepID" % outDS
                
    print "Problem DS: %s" % len(problemDS)
    #pprint(problemDS)
    return result

if __name__ == "__main__":
    
    
    outputDatasetPair = getOutputDatasetPrepID()
    print len(outputDatasetPair)
    print "done"
    
    
    
        