from __future__ import print_function
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

def getWorkloadFromSpec(baseUrl, db, request):
    wh = WMWorkloadHelper()
    reqmgrSpecUrl = "%s/%s/%s/spec" % (baseUrl, db, request)
    wh.load(reqmgrSpecUrl)
    print (wh.name())
    for task in wh.getTopLevelTask():
        if task.taskType() == "MultiProcessing":
            print ("%s: %s" % (task.name(), task.taskType()))
            task.setTaskType("Processing")
            print ("%s: %s" % (task.name(), task.taskType()))
    return wh

if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch/couchdb"
    for db in ["reqmgr_workload_cache", "workqueue"]:
        wh = getWorkloadFromSpec(baseUrl, db, "amaltaro_CosmicsSP_732_Feb9test_732_150302_231736_5479")
        #wh.saveCouch(baseUrl, db)