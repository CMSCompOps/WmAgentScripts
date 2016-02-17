from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.DBS.DBSReader import DBSReader

pudataset = "/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-MCRUN2_71_V1-v2/GEN-SIM"
dbsurl = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
dbsReader = DBSReader(endpoint = dbsurl)
def getPileupByBlock(datasets):
    resultDict = {}
    fakeSE = []
    for dataset in datasets:
        blockDict = {}
        blockNames = dbsReader.listFileBlocks(dataset)
            # DBS listBlocks returns list of DbsFileBlock objects for each dataset,
            # iterate over and query each block to get list of files
        for dbsBlockName in blockNames:
            blockDict[dbsBlockName] = {"FileList": sorted(dbsReader.lfnsInBlock(dbsBlockName)),
                                       "PhEDExNodeNames": dbsReader.listFileBlockLocation(dbsBlockName),
                                       "NumberOfEvents": dbsReader.getDBSSummaryInfo(block=dbsBlockName)['NumberOfEvents']}
            blockDict[dbsBlockName]['PhEDExNodeNames'].extend(x for x in fakeSE if x not in \
                                                                  blockDict[dbsBlockName]['PhEDExNodeNames'])
        resultDict[dataset] = blockDict
    return resultDict


print getPileupByBlock([pudataset])