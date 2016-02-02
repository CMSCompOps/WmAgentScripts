from WMCoreService.WMStatsClient import WMStatsClient

if __name__ == "__main__":
    url = "https://cmsweb.cern.ch/couchdb/wmstats"
    testbedWMStats = WMStatsClient(url)
    print "start to getting request from %s" % url
    options = {}
    options["include_docs"] = False
    options["group_level"] = 1
    options["reduce"] = True 
    options["stale"] = "update_after"
    requests = testbedWMStats._getCouchView("latestRequest", options)
    
    reqNames = []
    for row in requests["rows"]:
        reqNames.append(row["key"][0])
    
    requests = testbedWMStats._getAllDocsByIDs(reqNames, False)
    
    print "total request from agent: %s" % len(reqNames)
    missingR = []
    for v in requests["rows"]:
        if v.has_key('error'):
            missingR.append(v["key"])

    print "total missing reqeust: %s" % len(missingR)
    print "\n\n\n"
    for m in missingR:
        print m
    
    
