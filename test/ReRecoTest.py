from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory
m = MonteCarloWorkloadFactory()
#m("M", {})

a = {         
    "Comments": "Simple ReReco workflow. Two runs whitelisted. Harvesting enabled. Half an hour opened",
    "Campaign": "Campaign-OVERRIDE-ME",        
    "RequestString": "RequestString-OVERRIDE-ME",
    "CMSSWVersion": "CMSSW_7_0_3",
    "GlobalTag": "FT_R_70_V1::All",
    "RequestPriority": 10000,
    "ScramArch": "slc6_amd64_gcc481",
    "RequestType": "ReReco",
    "RunWhitelist": [203777, 204511],
    "ConfigCacheUrl": "https://cmsweb.cern.ch/couchdb",
    "ConfigCacheID": "a6d326cfbc11d3e636125d7b08b6624a",
    "DQMConfigCacheID": "a6d326cfbc11d3e636125d7b08b54b1b",
    "DQMUploadUrl": "https://cmsweb-testbed.cern.ch/dqm/dev",
    "EnableHarvesting": "True",
    "IncludeParents": "False",
    "Group": "DATAOPS",
    "SplittingAlgo": "LumiBased",
    "LumisPerJob": 2,
    "TimePerEvent": 2.0,
    "SizePerEvent": 2000,
    "InputDataset": "/DoubleElectron/Run2012D-v1/RAW",
    "DbsUrl": "https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader",
    "inputMode": "couchDB",
    "Scenario": "pp",      
    "OpenRunningTimeout" : 1800
}
r = ReRecoWorkloadFactory()

r("TestReReco", a)