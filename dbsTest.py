#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, json, phedexSubscription
from xml.dom.minidom import getDOMImplementation
from das_client import get_data

def getWorkflowType(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.loads(r2.read())
    requestType=request['RequestType']
    return requestType


def getRunWhitelist(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.loads(r2.read())
    runWhitelist=request['RunWhitelist']
    return runWhitelist

def getBlockWhitelist(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.loads(r2.read())
    BlockWhitelist=request['BlockWhitelist']
    return BlockWhitelist

def getInputDataSet(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.loads(r2.read())
    inputDataSets=request['InputDataset']
    if len(inputDataSets)<1:
        print "No InputDataSet for workflow " +workflow
    else:
        return inputDataSets

def getEventsRun(url, dataset, run):
    output=os.popen("./dbssql --input='find dataset,sum(block.numevents) where dataset="+dataset+" and run="+str(run)+"' "+"|awk '{print $2}' | grep '[0-9]\{1,\}'").read()
    try:
        int(output)
        return int(output)
    except ValueError:
        return -1   


def getEventCountDataSet(dataset):
    output=os.popen("./dbssql --input='find dataset,sum(block.numevents) where dataset="+dataset+"'"+ "|awk '{print $2}' | grep '[0-9]\{1,\}'").read()
    try:
        int(output)
        return int(output)
    except ValueError:
        return -1

def getRunLumiCountDataset(dataset):
    output=os.popen("./dbssql --limit=10000 --input='find run, count(lumi) where dataset="+dataset+"'"+ "| grep '[0-9]\{1,\}' | awk '{s+=$2}END{print s}'").read()
    try:
        int(output)
        return int(output)
    except ValueError:
        return -1


def getRunLumiCountDatasetList(dataset, runlist):
    lumis=0
    runChunks=chunks(runlist,30)
    for runList in runChunks:
        querry="./dbssql --limit=10000 --input='find run, count(lumi) where dataset="+dataset+" AND ("
        for run in runList:
            querry=querry+" run="+str(run) +" OR "
        querry=querry+' run= '+str(runList[0]) +')'
        querry=querry+"'| grep '[0-9]\{1,\}' | awk '{s+=$2}END{print s}'"
        output=os.popen(querry).read()
        if not output:
            lumis=lumis
        try:
            lumis=lumis+int(output)
        except ValueError:
            lumis=lumis
    return lumis
  

# SPlits a list of chunks of size(n)
def chunks(lis, n):
    return [lis[i:i+n] for i in range(0, len(lis), n)]

#Return the number of events for a given dataset given a blocklist
def EventsBlockList(dataset, blocklist):
    events=0
    if len(blocklist)==0:
        return getEventCountDataSet(dataset)
    blockChunks=chunks(blocklist,10)
    for blockList in blockChunks:
        querry="./dbssql --input='find dataset,sum(block.numevents) where dataset="+dataset+' AND ('
        if len(blockList)==0:
            continue
        for block in blockList:
            querry=querry+' block= '+block+' OR'
        querry=querry+' block='+str(blockList[0]) +' )'
        querry=querry+"'|awk '{print $2}' | grep '[0-9]\{1,\}'"
        output=os.popen(querry).read()
        if not output:
            events=events
        try:
            events=events+int(output)
        except ValueError:
            events=events
    return events


#Return the number of events for a given dataset given a runlist
def EventsRunList(dataset, runlist):
    events=0
    runChunks=chunks(runlist,30)
    for runList in runChunks:
        querry="./dbssql --input='find dataset,sum(block.numevents) where dataset="+dataset+' AND ('
        for run in runList:
            querry=querry+" run="+str(run) +" OR "
        querry=querry+' run= '+str(runList[0]) +')'
        querry=querry+"'| grep '[0-9]\{1,\}' | awk '{s+=$2}END{print s}'"
        #querry=querry+"'|awk '{print $2}' | grep '[0-9]\{1,\}'"
        output=os.popen(querry).read()
        if not output:
            events=events
        try:
            events=events+int(output)
        except ValueError:
            events=events
    return events

def getRunLumiCountDatasetBlockList(dataset,blockList):
    lumis=0
    querry="./dbssql --limit=1000000 --input='find block, run, lumi where dataset="+dataset+"' |egrep '("
    for block in blockList:
        querry=querry+block+ "|"
    querry=querry+blockList[0]+")'"
    querry=querry+"| awk '{print $2, $3}' | sort | uniq | wc -l"
    output=os.popen(querry).read()
    if not output:
        lumis=lumis
    try:
        lumis=lumis+int(output)
    except ValueError:
        lumis=lumis
    #print lumis    
    return lumis

def handleTaskChain(request):
    # Check if it's MC from scratch
    if 'RequestNumEvents' in request['Task1']:
        if request['Task1']['RequestNumEvents'] is not None:
            return request['Task1']['RequestNumEvents']

    blockWhitelist = blockBlacklist = runWhitelist = runBlacklist = []
    if 'InputDataset' in request['Task1']:
        inputDataSet=request['Task1']['InputDataset']
        if 'BlockWhitelist' in request['Task1']:
            blockWhitelist=request['Task1']['BlockWhitelist']
        if 'BlockBlacklist' in request['Task1']:
            blockBlacklist=request['Task1']['BlockBlacklist']
        if 'RunWhitelist' in request['Task1']:
            runWhitelist=request['Task1']['RunWhitelist']
        if 'RunBlacklist' in request['Task1']:
            runBlacklist=request['Task1']['RunBlacklist']

        if len(blockWhitelist)>0:
            return getRunLumiCountDatasetBlockList(inputDataSet,blockWhitelist)
        if len(blockBlacklist)>0:
            return getRunLumiCountDataset(inputDataSet)-getRunLumiCountDatasetBlockList(inputDataSet,blockBlacklist)
        if len(runWhitelist)>0:
            return getRunLumiCountDatasetList(inputDataSet, runWhitelist)
        else:
            return getRunLumiCountDataset(inputDataSet)

### TODO: implement multi white/black list
#        if len(blockWhitelist)>0 and len(runWhitelist)>0:
#            print "Hey, you have block and run white list :-D"
#            return getRunLumiCountDatasetBlockList(inputDataSet,BlockWhitelist)
#        elif len(blockWhitelist)>0 and len(runWhitelist)==0:
#            print "Hey, you have block white list but NOT run white list :-D"
#        elif len(blockWhitelist)==0 and len(runWhitelist)>0:
#            print "Hey, you have NO block white list but you do have run white list :-D"
#            return getRunLumiCountDatasetList(inputDataSet, runWhitelist)
#        elif len(blockWhitelist)==0 and len(runWhitelist)==0:
#            print "Hey, you have NO block and run white list :-D"
#
#        if len(BlockBlacklist)>0 and len(runBlacklist)>0:
#            print "Hey, you have block and run black list :-("
#            return getRunLumiCountDataset(inputDataSet)-getRunLumiCountDatasetBlockList(inputDataSet,BlockBlacklist)
#        elif len(BlockBlacklist)>0 and len(runBlacklist)==0:
#            print "Hey, you have block black list but NOT run black list :-("
#        elif len(BlockBlacklist)==0 and len(runBlacklist)>0:
#            print "Hey, you have NO block black list but you do have run black list :-("
#        elif len(BlockBlacklist)==0 and len(runBlacklist)==0:
#            print "Hey, you have NO block and run black list :-("

def getInputEvents(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.loads(r2.read())
    while 'exception' in request:
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
    requestType=request['RequestType']
    if requestType=='MonteCarlo' or requestType=='LHEStepZero':
        if 'RequestNumEvents' in request:
            if request['RequestNumEvents']>0:
                return request['RequestNumEvents']
        if 'RequestSizeEvents' in request:
            return request['RequestSizeEvents']
        else:
            return 0
    if requestType == 'TaskChain':
        return handleTaskChain(request)

    BlockWhitelist=request['BlockWhitelist']
    BlockBlacklist=request['BlockBlacklist']
    inputDataSet=request['InputDataset']
    runWhitelist=[]
    if 'RunWhitelist' in request:
	runWhitelist=request['RunWhitelist']
    if requestType=='ReReco':
        if len(BlockWhitelist)>0:
            return getRunLumiCountDatasetBlockList(inputDataSet,BlockWhitelist)
        if len(BlockBlacklist)>0:
            return getRunLumiCountDataset(inputDataSet)-getRunLumiCountDatasetBlockList(inputDataSet,BlockBlacklist)
        if len(runWhitelist)>0:
            return getRunLumiCountDatasetList(inputDataSet, runWhitelist)
        else:
            return getRunLumiCountDataset(inputDataSet)
    events=getEventCountDataSet(inputDataSet)
    if len(BlockBlacklist)>0:
        events=events-EventsBlockList(inputDataSet, BlockBlacklist)
    if len(runWhitelist)>0:
        events=EventsRunList(inputDataSet, runWhitelist)
    if len(BlockWhitelist)>0:
        events=EventsBlockList(inputDataSet, BlockWhitelist)
    if 'FilterEfficiency' in request.keys():
        return float(request['FilterEfficiency'])*events
    else:
        return events

def getOutputEvents(url, workflow, dataset):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.loads(r2.read())
    while 'exception' in request:
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
    requestType=request['RequestType']
    if requestType=='ReReco':
        return getRunLumiCountDataset(dataset)
    elif requestType=='TaskChain':
        if 'InputDataset' in request['Task1']:
             return getRunLumiCountDataset(dataset)
        else:
             return getEventCountDataSet(dataset)
    else:
        return getEventCountDataSet(dataset)

  
def main():
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:dbsTest workflowname"
        sys.exit(0)
    workflow=args[0]
    url='cmsweb.cern.ch'
    outputDataSets=phedexSubscription.outputdatasetsWorkflow(url, workflow)
    inputEvents=getInputEvents(url, workflow)
    for dataset in outputDataSets:
        outputEvents=getOutputEvents(url, workflow, dataset)
        if inputEvents!=0:
            print dataset+" match: "+str(outputEvents/float(inputEvents)*100) +"%"
        else:
            print "Input Events 0"
    sys.exit(0);

if __name__ == "__main__":
    main()

