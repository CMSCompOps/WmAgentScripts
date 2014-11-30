from __future__ import print_function
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper



def getWorkloadFromSpec(baseUrl, request):
    wh = WMWorkloadHelper()
    reqmgrSpecUrl = "%s/reqmgrdb/%s/spec" % (baseUrl, request)
    wh.load(reqmgrSpecUrl)
    print (wh.name())
    print (wh.getPrepID())
    for task in wh.getAllTasks():
        print ("%s: %s" % (task.name(), task.getPrepID()))


if __name__ == "__main__":
    baseUrl = "http://cmssrv95.fnal.gov:5984"
    getWorkloadFromSpec(baseUrl, "fbloggs_test_141129_155401_4254")
