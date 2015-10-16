from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from pprint import pprint

base_url = "https://cmsweb-testbed.cern.ch"
reqmgr_url = "%s/reqmgr2" % base_url
wmstats_url = "%s/couchdb/wmstats" % base_url

reqmgr = ReqMgr(reqmgr_url)
result = reqmgr.getRequestByStatus(["completed", "running-open", "running-closed", "acquired"])


req_names = []
for name, value in result.items():
    if "TotalInputEvents" not in value:
        req_names.append(name)
print len(req_names)

# req_names = ["amaltaro_700_pre11_ZTT_PU_HG1504c_Ag105_150327_003541_4665",
#              "amaltaro_700_pre11_ZJetsLNu_LHE_HG1504c_Ag105_150327_003529_1736",
#              "amaltaro_700_pre11_TTbar_HG1504c_Ag105_150327_003510_5026",
#              "amaltaro_700_pre11_SingleMuPt100FastSim_HG1504c_Ag105_150327_003457_3297",
#              "amaltaro_700_pre11_RunTau2012A_HG1504c_Ag105_150327_003442_1392",
#              "amaltaro_700_pre11_PyquenZeemumuJets_PU_HG1504c_Ag105_150327_003423_7143",
#              "amaltaro_700_pre11_ProdMinBias_HG1504c_Ag105_150327_003405_7825",
#              "amaltaro_700_pre11_H130GGgluonfusion_PU_HG1504c_Ag105_150327_003350_9355",
#              "amaltaro_ReRecoSkim_HG1504c_Ag105_150327_003104_1360",
#              "amaltaro_TaskChainRunJet2012C_multiRun_HG1504c_Ag105_150327_003043_5824",
#              "amaltaro_TaskChain_Multicore_HG1504c_Ag105_150327_003017_2584",
#              "amaltaro_TaskChain_MC_HG1504c_Ag105_150327_002950_4033",
#              "amaltaro_MonteCarlo_Ext_HG1504c_Ag105_150327_002931_4916",
#              "amaltaro_TaskChain_Data_HG1504c_Ag105_150327_002909_1612",
#              "amaltaro_TaskChain_Task_Multicore_HG1504c_Ag105_150327_002840_1928",
#              "amaltaro_MonteCarloFromGEN_HG1504c_Ag105_150327_002818_3374",
#              "amaltaro_TaskChain_MC_PU_HG1504c_Ag105_150327_002748_8840",
#              "amaltaro_ReDigi_HG1504c_Ag105_150327_002728_84",
#              "amaltaro_ReDigi_cmsRun2_HG1504c_Ag105_150327_002707_5762",
#              "amaltaro_MonteCarlo_HG1504c_Ag105_150327_002648_1912",
#              "amaltaro_MonteCarlo_LHE_HG1504c_Ag105_150327_002628_6236",
#              "amaltaro_MonteCarlo_eff_HG1504c_Ag105_150327_002609_6509",
#              "amaltaro_700_pre11_ZTT_PU_HG1504c_Ag100_150326_232632_9892",
#              ]
wmstats = WMStatsReader(wmstats_url)
result = wmstats._getAllDocsByIDs(req_names)
data = {}
for row in result["rows"]:
    data[row['id']] = {} 
    data[row['id']]['total_jobs'] = row['doc']['total_jobs']
    data[row['id']]['input_events'] = row['doc']['input_events']
    data[row['id']]['input_lumis'] = row['doc']['input_lumis']
    data[row['id']]['input_num_files'] = row['doc']['input_num_files']

#pprint(data)


for req_name, stat in data.items():
    try:
        result = reqmgr.updateRequestStats(req_name, stat)
    except:
        print req_name

        

#pprint(reqmgr.getRequestByNames(req_names))