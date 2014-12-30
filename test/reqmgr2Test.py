#!/usr/bin/env python
import os
import sys
from optparse import OptionParser
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter

needToClose = ["Requestor-OVERRIDE-ME_HG1304_LHETest_1_130409_135008_3869"]

baseUrl = "https://reqmgr2-dev.cern.ch/couchdb"
url = "%s/wmstats" % baseUrl
wmstats = WMStatsWriter(url)

reqUrl = "%s/reqmgr_workload_cache" % baseUrl
reqDB = RequestDBWriter(reqUrl, couchapp = "ReqMgr")
#report = reqDB.getRequestByNames(needToClose)
#print report
for request in needToClose:
    print request
    report = reqDB.updateRequestStatus(request, "assignment-approved")
    # print report
    #report = wmstats.updateRequestStatus(request, "closed-out")
    print report