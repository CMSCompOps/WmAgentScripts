#!/usr/bin/env python
import os
import sys
from optparse import OptionParser
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Database.CMSCouch import CouchServer, Database, Document

needToClose = [
"pdmvserv_EXO-Summer12DR53X-03250_No_custT1_00256_v0__140618_075551_7526"]

needToClose = [
"pdmvserv_HIG-Spring14dr-00093_T1_FR_CCIN2P3_MSS_00108_v0__140623_125441_381",
"pdmvserv_HIG-Spring14dr-00094_T1_FR_CCIN2P3_MSS_00109_v0__140623_125446_4394",
"pdmvserv_HIG-Spring14dr-00095_T1_FR_CCIN2P3_MSS_00108_v0__140623_125434_6117",
"pdmvserv_HIG-Spring14dr-00096_T1_FR_CCIN2P3_MSS_00109_v0__140623_125438_9410", 
"pdmvserv_HIG-Spring14dr-00098_T1_FR_CCIN2P3_MSS_00109_v0__140623_125456_8575",
"pdmvserv_HIG-Spring14dr-00101_T1_FR_CCIN2P3_MSS_00108_v0__140623_125904_9460",
"pdmvserv_HIG-Spring14dr-00102_T1_FR_CCIN2P3_MSS_00109_v0__140623_130027_8450",
"pdmvserv_HIG-Spring14dr-00103_T1_FR_CCIN2P3_MSS_00108_v0__140623_130057_5484",
"pdmvserv_HIG-Spring14dr-00104_T1_FR_CCIN2P3_MSS_00109_v0__140623_130101_9071",
"pdmvserv_TSG-Spring14dr-00016_T1_US_FNAL_MSS_00006_v0__140411_185146_3203"
]

needToArchive = [
"pdmvserv_TOP-Spring14miniaod-00001_T0_CH_CERN_MSS_00001_v0__140621_133148_8160",
"pdmvserv_TOP-Spring14miniaod-00002_T0_CH_CERN_MSS_00002_v0__140621_133150_3452"]


url = "https://cmsweb.cern.ch/couchdb/wmstats"
wmstats = WMStatsWriter(url)

couchDb = Database("reqmgr_workload_cache", "https://cmsweb.cern.ch/couchdb")

for request in needToArchive:
    report = couchDb.updateDocument(request, "ReqMgr", "updaterequest",
                               fields={"RequestStatus": "normal-archived"})
    print report
    #report = wmstats.updateRequestStatus(request, "closed-out")
    #print report