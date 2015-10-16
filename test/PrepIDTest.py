from __future__ import print_function
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

def getWorkloadFromSpec(baseUrl, request):
    wh = WMWorkloadHelper()
    reqmgrSpecUrl = "%s/reqmgr_workload_cache/%s/spec" % (baseUrl, request)
    wh.load(reqmgrSpecUrl)
    print (wh.name())
    print (wh.getPrepID())
    for task in wh.getTopLevelTask():
        if task.taskType() == "MultiProcessing":
            print ("%s: %s, %s" % (task.name(), task.getPrepID(), task.taskType()))


if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch/couchdb"
    getWorkloadFromSpec(baseUrl, "amaltaro_CosmicsSP_732_Feb9test_732_150302_231736_5479")
