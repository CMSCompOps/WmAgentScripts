#!/usr/bin/env python
from WMCore.Services.RequestManager.RequestManager import RequestManager

if __name__ == "__main__":
    args = {}
    args["endpoint"] = "https://reqmgr2-dev.cern.ch/reqmgr/rest"
    reqMgr = RequestManager(args)
    #requestNames = ["sryu_MonteCarlo_LHE_mc_lumi_validation_150923_164422_7122",
    requestNames = ["sryu_MonteCarlo_LHE_mc_lumi_validation_150923_164003_3836"]
    for requestName in requestNames:
        #reqInfo = reqMgr.getRequest(requestName)
        #print reqInfo
        reqMgr.reportRequestStatus(requestName, "running-closed")