from WMCore.Services.RequestManager.RequestManager import RequestManager

if __name__ == "__main__":
    args = {}
    args["endpoint"] = "https://reqmgr2-dev.cern.ch/reqmgr/rest"
    reqMgr = RequestManager(args)
    print "Test..."
    requestNames = ["hernan_MultiCoreTest_64-cores_513_01_T1_ES_PIC_120625_155913_2050"]
    for requestName in requestNames:
        reqInfo = reqMgr.getRequest(requestName)
        print reqInfo["RequestStatus"]