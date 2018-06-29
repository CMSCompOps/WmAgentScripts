#!/usr/bin/env python

import sys
import json
from das_client import get_data
from reqMgrClient import WorkflowWithInput
#das_host='https://das.cern.ch'
#das_host='https://cmsweb.cern.ch'
das_host='https://cmsweb-testbed.cern.ch'
#das_host='https://das-dbs3.cern.ch'


def getBlocksLumi(dataset):
    Blockslumis=[]
    query="block, run, lumi dataset="+dataset
    #TODO replace for DBS query
    das_data = get_data(das_host,query,0,0,0)
    getL
    if isinstance(das_data, basestring):
            result = json.loads(das_data)
    else:
            result = das_data
    if result['status'] == 'fail' :
            print 'DAS query failed with reason:',result['reason']
    else:
        preresult=result['data']
        for block in preresult:
            lumisBlock=block['lumi'][0]['number']
            lumis=[]
            for lumiRange in lumisBlock:
                if lumiRange[0]==lumiRange[1]:
                    lumis.append(lumiRange[0])
                else:
                    lumis=lumis+range(lumiRange[0], lumiRange[1])
            Blockslumis.append((block['block'][0]['name'],lumis))
    return Blockslumis


def getLumisInDataset(dataset):
    lumis=[]
    query="run lumi dataset="+dataset
    das_data = get_data(das_host,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
    #preresult=result['data'][0]['lumi'][[0]['number'][0]
    #print result    
    preresult=result['data'][0]['lumi'][0]['number']
    for lumiRange in preresult:
        if lumiRange[0]==lumiRange[1]:
            lumis.append(lumiRange[0])
        else:
            lumis=lumis+range(lumiRange[0], lumiRange[1])
    return lumis

def getBlocksNotProcessed(lumisOutput, BlockLumisInput):
    BlocksNotProcessed=[]
    for blockLumiPair in BlockLumisInput:
        BlockProcessed=False
        lumisBlock=blockLumiPair[1]
        for lumi in lumisBlock:
            if lumi in lumisOutput:
                BlockProcessed=True
        if BlockProcessed==False:
            BlocksNotProcessed.append(blockLumiPair[0])
    return [x.encode() for x in BlocksNotProcessed]


def getListUnprocessedBlocks(url, workflow):
    wfInfo = WorkflowWithInput(workflow)
    outputDataSets = wfInfo.outputDatasets
    inputDataset = wfInfo.inputDatasets
    lumisOutput = getLumisInDataset(outputDataSets[0])
    BlockLumisInput = getBlocksLumi(inputDataset)
    BlocksNotProcessed = getBlocksNotProcessed(lumisOutput, BlockLumisInput)
    return BlocksNotProcessed

def main():
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:unprocessedBlocks workflowname"
        sys.exit(0)
    workflow=args[0]
    url='cmsweb.cern.ch'
    BlocksNotProcessed=getListUnprocessedBlocks(url, workflow)
    print "Number of blocks not processed", str(len(BlocksNotProcessed))
    print [x.encode() for x in BlocksNotProcessed]
    sys.exit(0);

if __name__ == "__main__":
    main()
