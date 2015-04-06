#!/usr/bin/env python
import os
import sys
from optparse import OptionParser
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter

needToClose = []

needToCompletd = ["boudoul_RVCMSSW_6_2_0_SLHC24QQH1352T_Tauola_14TeV__HGCalV6_150312_134838_7523"]

baseUrl = "https://cmsweb.cern.ch/couchdb"
url = "%s/wmstats" % baseUrl
wmstats = WMStatsWriter(url)

reqUrl = "%s/reqmgr_workload_cache" % baseUrl
reqDB = RequestDBWriter(reqUrl, couchapp = "ReqMgr")
#report = reqDB.getRequestByNames(needToClose)
#print report
for request in needToCompletd:
    print request
    report = reqDB.updateRequestStatus(request, "completed")
    print report
    report = wmstats.updateRequestStatus(request, "completed")
    print report