from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.DBS.DBSReader import DBSReader
from pprint import pprint
pudataset = "/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-MCRUN2_71_V1-v2/GEN-SIM"
dbsurl = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
dbsReader = DBSReader(endpoint = dbsurl)
phedex = PhEDEx()
def getPileupByBlock(datasets):
    resultDict = {}
    fakeSE = []
    node_filter = set(['UNKNOWN', None])
    for dataset in datasets:
        blockDict = {}
        summary = dbsReader.getDBSSummaryInfo(dataset=dataset)
        print summary
        eventsCount = 0
        blockFileInfo = dbsReader.getFileListByDataset(dataset=dataset, detail=True)
        for fileInfo in blockFileInfo:
            blockDict.setdefault(fileInfo['block_name'], {'FileList': [], 
                                                          'NumberOfEvents': 0, 
                                                          'PhEDExNodeNames': []})
            blockDict[fileInfo['block_name']]['FileList'].append({'logical_file_name': fileInfo['logical_file_name']})
            blockDict[fileInfo['block_name']]['NumberOfEvents'] += fileInfo['event_count']
            eventsCount += fileInfo['event_count']
        
        blocksInfo = phedex.getReplicaPhEDExNodesForBlocks(dataset=dataset, complete='y')
        for block in blocksInfo:
            nodes = set(blocksInfo[block]) - node_filter | set(fakeSE)
            blockDict[block]['PhEDExNodeNames'] = list(nodes)
            blockDict[block]['FileList'] = sorted(blockDict[block]['FileList'])
        
        pprint(blockDict['/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-MCRUN2_71_V1-v2/GEN-SIM#1ae16114-7717-11e5-8413-001e67abf094'])
        print len(blockDict['/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-MCRUN2_71_V1-v2/GEN-SIM#1ae16114-7717-11e5-8413-001e67abf094']['PhEDExNodeNames'])    
        print eventsCount
        print len(blockDict)
        print len(blocksInfo)
    return


getPileupByBlock([pudataset])