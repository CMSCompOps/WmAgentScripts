import mismatchArchived
from pprint import pprint
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.ReqMgr.ReqMgrReader import ReqMgrReader
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.RequestManager.RequestDB.Settings.RequestStatus import StatusList
from WMCore.Database.CMSCouch import CouchServer, Database
from WMCore.Wrappers import JsonWrapper

pprint(mismatchArchived.mismatch)
print len(mismatchArchived.mismatch)
exceptionList = ["jbadillo_TOP-Summer12-00234_00073_v0__140124_154429_3541",
                 "pdmvserv_HIG-2019GEMUpg14DR-00021_00041_v0_age1k_PU140bx25_140709_191435_3571"]

couchDb = Database("reqmgr_workload_cache", "https://cmsweb.cern.ch/couchdb")

needToFix = mismatchArchived.mismatch.items()
for request, statusArray in needToFix:
    if request in exceptionList:
        print request, statusArray
    else:
        report = couchDb.updateDocument(request, "ReqMgr", "updaterequest",
                               fields={"RequestStatus": statusArray[1]})
        print report