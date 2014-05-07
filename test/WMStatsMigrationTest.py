from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection

if __name__ == "__main__":
    url = "https://cmsweb.cern.ch/couchdb/wmstats"
    testbedWMStats = WMStatsClient(url)
    print "start to getting request from %s" % url
    print "will take a while\n"
    f = open("./missing_wf.txt")
    ids = set()
    for line in f:
        ids.add(line.strip())
    print "number of missing request : %s" % len(ids)
    requests = testbedWMStats._getAllDocsByIDs(list(ids), False)
    print "results from :%s" % url
    print len(requests['rows'])
    missingR = []
    for v in requests["rows"]:
        if v.has_key('error'):
            missingR.append(v["key"])
            print v
    print len(missingR)
    