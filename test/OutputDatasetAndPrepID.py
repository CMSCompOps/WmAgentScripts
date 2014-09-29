from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
from WMCore.Lexicon import splitCouchServiceURL
from pprint import pprint    

def getOutputDatasetPrepID(wmstatsDB):
    data = wmstatsDB.loadView('WMStats', 'requestByPrepID', 
                              {'stale': "update_after", 'include_docs' : True})

    result = {}
    problemDS = {}
    for row in data['rows']:
        prepID = row["doc"]["prep_id"]
        status = row["doc"]["request_status"][-1]['status']
        
        if (status.find("rejected") != -1 or 
            status.find("aborted") != -1 or
            status.find("failed") != -1):
            continue
        
        for outDS in row["doc"]["outputdatasets"]:
            if type(outDS) == list:
                #print "%s: %s" % (row["doc"]['workflow'], outDS)
                outDS = outDS[0]
            if result.has_key(outDS) and prepID != result[outDS]:
                if outDS.find("None-") == -1:
                    problemDS.setdefault(outDS, set())
                    problemDS[outDS].add(result[outDS])
                    problemDS[outDS].add(prepID)
            else:
                result[outDS] = row["doc"]["prep_id"]
    #print "Problem DS: %s" % len(problemDS)
    #pprint(problemDS)
    return result

if __name__ == "__main__":
    
    baseUrl = "https://cmsweb.cern.ch/couchdb"
    wmstatsDBName = "wmstats"
    server = CouchServer(baseUrl)
    wmstatsDB = server.connectDatabase(wmstatsDBName, create = False)
    
    outputDatasetPair = getOutputDatasetPrepID(wmstatsDB)
    print len(outputDatasetPair)
    print "done"
    
    
    
        