from WMCoreService.WMStatsClient import WMStatsClient
from WMCore.Services.RequestManager.RequestManager import RequestManager

if __name__ == "__main__":
    url = "https://cmsweb.cern.ch/couchdb/wmstats"
    testbedWMStats = WMStatsClient(url)
    args = {}
    args["endpoint"] = "https://cmsweb.cern.ch/reqmgr/rest"
    reqMgr = RequestManager(args)
    print "start to getting job information from %s" % url
    print "will take a while\n"
    requestNames = ["spinoso_TRK-UpgradePhase2LB4PS_2013-00011_R2614_B4_STEP0ATCERN_130528_215818_8886",
                    "spinoso_TRK-UpgradePhase2LB4PS_2013-00012_R2614_B4_STEP0ATCERN_130528_215813_359",
                    "spinoso_TRK-UpgradePhase2LB4PS_2013-00013_R2614_B4_STEP0ATCERN_130528_215807_4448",
                    "spinoso_TRK-UpgradePhase2LB6PS_2013-00011_R2612_B4_STEP0ATCERN_130528_181049_8032",
                    "spinoso_TRK-UpgradePhase2LB6PS_2013-00012_R2612_B4_STEP0ATCERN_130528_181044_4416",
                    "spinoso_TRK-UpgradePhase2LB6PS_2013-00013_R2612_B4_STEP0ATCERN_130528_181037_9904"]
    for requestName in requestNames:
        reqInfo = reqMgr.getRequest(requestName)
        print reqInfo["RequestStatus"]
        testbedWMStats.updateRequestStatus(requestName, "closed-out")
        print "announced"
        testbedWMStats.updateRequestStatus(requestName, "announced")
        reqMgr.reportRequestStatus(requestName, "normal-archived")
