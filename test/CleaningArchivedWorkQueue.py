from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from pprint import pprint

if __name__ == "__main__":
    #baseUrl = "https://cmsweb.cern.ch/couchdb"
    baseUrl = "https://cmsweb-testbed.cern.ch/couchdb"
    wqUrl = "%s/workqueue" % baseUrl
    wmstatsUrl = "%s/wmstats" % baseUrl
    reqdbURL = "%s/reqmgr_workload_cache" % baseUrl
    wqSvc = WorkQueue(wqUrl)
    reqNames = wqSvc.getWorkflowNames()
    reqNamesInbox = wqSvc.getWorkflowNames(inboxFlag = True)
    reqNames = list(set(reqNames).union(set(reqNamesInbox)))
    print len(reqNames)
    #pprint(reqNames)
    #ws = WMStatsReader(wmstatsUrl, reqdbURL)
    ws = RequestDBReader(reqdbURL)
    requestsInfo = ws.getRequestByNames(reqNames)
    deleteRequests = []
    for key, value in requestsInfo.items():
        if ((value["RequestStatus"] == None) or \
           (value["RequestStatus"].find("announced") != -1) or \
           (value["RequestStatus"].find("-archived") != -1) or \
           (value["RequestStatus"].find("aborted-completed") != -1) or \
           #(value["RequestStatus"].find("aborted") != -1) or \
           (value["RequestStatus"].find("rejected") != -1)):
            print key, value["RequestStatus"]
            deleteRequests.append(key)
    print deleteRequests
    print len(deleteRequests)

    #print wqSvc.deleteWQElementsByWorkflow(deleteRequests)

    print "done"