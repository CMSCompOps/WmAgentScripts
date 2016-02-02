from WMCoreService.WMStatsClient import WMStatsClient

def formatMisMatch(data, comments=""):
    print "%s : %s" % (comments, len(data))
    for key, value in data.items():
        print "%s: %s %s" % (key, value[0], value[1])
        
if __name__ == "__main__":
    url = "https://cmsweb.cern.ch/couchdb/wmstats"
    #url = "https://cmsweb-testbed.cern.ch/couchdb/wmstats"
    wmstats = WMStatsClient(url)
    print "start to getting job information from %s" % url
    print "will take a while\n"
    requests = wmstats._getRequestByStatus(None, False)
    requestDiff = {}
    for row in requests['rows']:
        requestDiff[row['id']] = row['key']
    
    url = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
    #url = "https://cmsweb-testbed.cern.ch/couchdb/wmstats"
    reqMgr = WMStatsClient(url)
    print "start to getting request from %s" % url
    
    requests = reqMgr._getAllDocsByIDs(requestDiff.keys(), True)
    missingDoc = {}
    missingTransition = {}
    unsyncStatus = {}
    unsyncDocs = []
    transitionCount = {}
    for row in requests["rows"]:
        wmstatsStatus = requestDiff[row['id']]
        if not row['doc']:
            missingDoc[row['id']] = [wmstatsStatus, 'deleted']
            continue
        if not row['doc'].has_key('RequestTransition'):
            missingTransition[row['id']] = [wmstatsStatus, row['doc']['RequestStatus']]
            continue
        reqMgrStatus = row['doc']['RequestTransition'][-1]['Status']
        if (wmstatsStatus != reqMgrStatus):
            print wmstatsStatus, reqMgrStatus
            unsyncStatus[row['id']] = [wmstatsStatus, reqMgrStatus]
            unsyncDocs.append(row['doc'])
            transitionCount.setdefault(wmstatsStatus, {})
            transitionCount[wmstatsStatus].setdefault(reqMgrStatus, 0)
            transitionCount[wmstatsStatus][reqMgrStatus] += 1

    formatMisMatch(missingDoc, "missing Doc")
    print 
    formatMisMatch(missingTransition, "missing Transition request name:wmstats reqmgr")
    print 
    print "Status"
    print "wmstats: reqmgr: number"
    for key, value in transitionCount.items():
        for k, v in value.items():
            print "%s: %s: %s" % (key, k, v)
    print
    formatMisMatch(unsyncStatus, "mismatch requestname:wmstats reqmgr")
    
    #Activate if you want to match        
    #wmstats.replaceRequestTransitionFromReqMgr(unsyncDocs)
    print "*** done"