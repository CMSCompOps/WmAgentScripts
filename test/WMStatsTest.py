from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from pprint import pprint

if __name__ == "__main__":
    baseURL = "https://cmsweb.cern.ch/couchdb"
    url = "%s/wmstats" % baseURL
    reqDBURL = "%s/reqmgr_workload_cache" % baseURL
    testbedWMStats = WMStatsWriter(url, reqdbURL=reqDBURL)
    import pdb
    
    #pdb.set_trace()
    result = testbedWMStats.deleteOldDocs(0.15*24)
    pprint(len(result))
    result = testbedWMStats.getArchivedRequests()
    pprint(result)
    
    for req in result:
        result = testbedWMStats.deleteDocsByWorkflow(req)
        pprint(result)