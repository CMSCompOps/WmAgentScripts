#!/usr/bin/env python
import os
import sys
from optparse import OptionParser
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter

needToClose = ["nancy_RVCMSSW_7_1_0RunMinBias2010B__RelVal_mb2010B_140623_143809_6818",
"nancy_RVCMSSW_7_1_0MinimumBias2010A__RelVal_run2010A_140623_143801_7006",
"nancy_RVCMSSW_7_1_0RunCosmicsA__RelVal_cos2010A_140623_143708_387",
"nancy_RVCMSSW_7_1_0SingleMuPt10__FastSim_140623_142911_1823",
"nancy_RVCMSSW_7_1_0TTbar_13__FastSim_140623_142854_2217",
"nancy_RVCMSSW_7_1_0SingleMuPt100__FastSim_140623_142839_8743",
"nancy_RVCMSSW_7_1_0ZTT_13__FastSim_140623_142832_7564",
"nancy_RVCMSSW_7_1_0ZmumuJets_Pt_20_300_13_140623_142707_8667",
"nancy_RVCMSSW_7_1_0ZpTT_1500_13TeV_Tauola_140623_142657_2953",
"nancy_RVCMSSW_7_1_0ZpEE_2250_13TeV_Tauola_140623_142643_6915",
"nancy_RVCMSSW_7_1_0ZpMM_2250_13TeV_Tauola_140623_142631_7033",
"nancy_RVCMSSW_7_1_0RSKKGluon_m3000GeV_13_140623_142621_9838",
"nancy_RVCMSSW_7_1_0QCD_Pt_600_800_13_140623_142600_3114",
"nancy_RVCMSSW_7_1_0QCD_FlatPt_15_3000HS_13_140623_142550_5583",
"nancy_RVCMSSW_7_1_0LM1_sfts_13_140623_142541_2283",
"nancy_RVCMSSW_7_1_0Wjet_Pt_3000_3500_13_140623_142533_1008",
"nancy_RVCMSSW_7_1_0Wjet_Pt_80_120_13_140623_142523_7557",
"nancy_RVCMSSW_7_1_0QQH1352T_Tauola_13_140623_142512_4346",
"nancy_RVCMSSW_7_1_0PhotonJets_Pt_10_13_140623_142502_8872",
"nancy_RVCMSSW_7_1_0H130GGgluonfusion_13_140623_142452_3168",
"nancy_RVCMSSW_7_1_0ZTT_13_140623_142440_8950",
"nancy_RVCMSSW_7_1_0ZEE_13_140623_142418_8228",
"nancy_RVCMSSW_7_1_0QCD_Pt_80_120_13_140623_142409_6956",
"nancy_RVCMSSW_7_1_0WM_13_140623_142400_2185",
"nancy_RVCMSSW_7_1_0WE_13_140623_142349_843",
"nancy_RVCMSSW_7_1_0TTbar_13_140623_142335_7807",
"nancy_RVCMSSW_7_1_0TTbarLepton_13_140623_142325_3629",
"nancy_RVCMSSW_7_1_0SingleMuPt1000_UP15_140623_142303_7823",
"nancy_RVCMSSW_7_1_0SingleMuPt100_UP15_140623_142252_6092",
"nancy_RVCMSSW_7_1_0SingleMuPt10_UP15_140623_142242_6386",
"nancy_RVCMSSW_7_1_0SingleGammaPt35_UP15_140623_142232_4654",
"nancy_RVCMSSW_7_1_0SingleGammaPt10_UP15_140623_142222_9158",
"nancy_RVCMSSW_7_1_0SingleElectronPt35_UP15_140623_142214_7286",
"nancy_RVCMSSW_7_1_0SingleElectronPt1000_UP15_140623_142206_4924",
"nancy_RVCMSSW_7_1_0SingleElectronPt10_UP15_140623_142158_5351",
"nancy_RVCMSSW_7_1_0WpM_13_140623_142149_516",
"nancy_RVCMSSW_7_1_0ZpMM_13_140623_142132_1840",
"nancy_RVCMSSW_7_1_0MinBias_13_140623_142121_6257",
"nancy_RVCMSSW_7_1_0ADDMonoJet_d3MD3_13_140623_142112_5652",
"nancy_RVCMSSW_7_1_0Higgs200ChargedTaus_13_140623_142103_6002",
"nancy_RVCMSSW_7_1_0BeamHalo_13_140623_142051_3628",
"nancy_RVCMSSW_7_1_0SingleMuPt1_UP15_140623_142024_2291",
"nancy_RVCMSSW_7_1_0ProdQCD_Pt_3000_3500_13_140623_142012_2246",
"nancy_RVCMSSW_7_1_0ProdTTbar_13_140623_142001_4215",
"nancy_RVCMSSW_7_1_0ProdMinBias_13_140623_141952_3832"]

needToClose = []

baseUrl = "https://cmsweb-testbed.cern.ch/couchdb"
url = "%s/wmstats" % baseUrl
wmstats = WMStatsWriter(url)

reqUrl = "%s/reqmgr_workload_cache" % baseUrl
reqDB = RequestDBWriter(reqUrl, couchapp = "ReqMgr")
#report = reqDB.getRequestByNames(needToClose)
#print report
for request in needToClose:
    print request
    report = reqDB.updateRequestStatus(request, "closed-out")
    print report
    report = wmstats.updateRequestStatus(request, "closed-out")
    print report