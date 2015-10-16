from WMCore.Services.WMStats.WMStatsReader import WMStatsReader

wmstats_url = "https://cmsweb-testbed.cern.ch/couchdb/tier0_wmstats"
reqmgrdb_url = "https://cmsweb-testbed.cern.ch/couchdb/t0_request"
app = "T0Request"
#wmstats_url = "https://cmsweb-testbed.cern.ch/couchdb/wmstats"
#reqmgrdb_url = "https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache"
#app = "ReqMgr"
wmstatsDB = WMStatsReader(wmstats_url, reqmgrdb_url, reqdbCouchApp = "T0Request")
import pdb
#pdb.set_trace()
jobData = wmstatsDB.getT0ActiveData(jobInfoFlag = True)
print jobData
                
