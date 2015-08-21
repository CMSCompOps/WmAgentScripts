#!/usr/bin/env python
"""
    This File used to queries DBS, DAS and request manager
    it is substitued and organized in other files.
    Use dbs3Client.py instead.
    @DEPRECATED

"""


import urllib2, urllib, httplib, sys, re, os, json
from deprecated import phedexSubscription
from xml.dom.minidom import getDOMImplementation
from das_client import get_data
#das_host='https://das.cern.ch'
das_host='https://cmsweb.cern.ch'
#das_host='https://cmsweb-testbed.cern.ch'
#das_host='https://das-dbs3.cern.ch'
#das_host='https://dastest.cern.ch'

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
    while 'exception' in request:
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
    inputDataSets=request['InputDataset']
    if len(inputDataSets)<1:
        print "No InputDataSet for workflow " +workflow
    else:
        return inputDataSets


def duplicateRunLumi(dataset):
    """
    checks if output dataset has duplicate lumis
    for every run.
    """
    RunlumisChecked={}
    query="file run lumi dataset="+dataset
    das_data = get_data(das_host,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        preresult=result['data'] 
    #check ever file in dataset    
    for filename in preresult:
        run=filename['run'][0]['run_number']
        #add run if new
        if run not in RunlumisChecked:
            RunlumisChecked[run]=set()
        newLumis=filename['lumi'][0]['number']
        #check every lumi on range        
        for lumiRange in newLumis:
            newlumiRange=range(lumiRange[0], lumiRange[1]+1)
            for lumi in newlumiRange:
                #if already checked in the same run
                if lumi in RunlumisChecked[run]:
                    return True
                else:
                    RunlumisChecked[run].add(lumi)
    return False

def duplicateLumi(dataset):
    """
    checks if output dataset has a duplicate lumi
    """
    #registry of lumis checked, better a set
    lumisChecked=set()
    #get dtaset info frm das
    query="file lumi dataset="+dataset
    das_data = get_data(das_host,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        preresult=result['data']
    #check each file    
    for filename in preresult:
        newLumis=filename['lumi'][0]['number']
        #for each file we check each lumi range.
        for lumiRange in newLumis:
            newlumiRange=[lumiRange[0]]
            if lumiRange[0]<lumiRange[1]:
                newlumiRange=range(lumiRange[0], lumiRange[1])
            #check each lumi, if its in the lumiset            
            for lumi in newlumiRange:
                if lumi in lumisChecked:
                    return True
                else:
                    lumisChecked.add(lumi)
    return False

def getRunsInDataset(das_url, dataset):
    query="run dataset="+dataset
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        runs=[]
        preresult=result['data']
        for run in preresult:
            runs.append(run['run'][0]['run_number'])
        return runs

def getNumberofFilesPerRun(das_url, dataset, run):
    query="file dataset="+dataset+" run="+str(run)+" | count(file.name)"
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        return result['data'][0]['result']['value']

#Return true if there are duplicate evnets , false otherwise
def duplicateEventsMonteCarlo(dataset):
    das_url=das_host
    runs=getRunsInDataset(das_url, dataset)
    for run in runs:
        NumFilesRun=getNumberofFilesPerRun(das_url, dataset, run)
        NumLumis=getRunLumiCountDatasetRun(das_url, dataset, run)
        if NumLumis>NumFilesRun:#It means at least one lumi is split into more than one file
            return True
    return False

#Return the number of events for a given dataset given a runlist
def EventsRunList(das_url, dataset, runlist):
    events=0
    for run in runlist:
       events=events+getEventsRun(das_url, dataset, run)
    return events

# Return the number of events in a dataset and in a run
def getEventsRun(das_url, dataset, run):
    query="summary dataset dataset="+dataset+" run="+str(run)+" | grep grep summary.nevents"
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        if len(result['data'])==0:#dataset not yet registered in DBS
            return 0
        preresult=result['data'][0]['summary']
        for key in preresult:
            if 'nevents' in key:
                return key['nevents']        
        return -1



#Returns the number of events in a dataset using DAS
def getEventCountDataSet(das_url, dataset):
    query="dataset dataset="+dataset+"  status=* | grep dataset.nevents"
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data       
    if result['status'] == 'fail' :
        print 'DAS query' + query+' failed with reason:',result['reason']
    else:
        if len(result['data'])==0:#dataset not yet registered in DBS
            return 0
        preresult=result['data'][0]['dataset']
        for key in preresult:
            if 'nevents' in key:
                return key['nevents']        
        return -1

#Returns a list of runs of a dataset
def getRunsDataset(das_url, dataset):
    runList=[]
    query="run dataset="+dataset+"| grep run.run_number"
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        if len(result['data'])==0:#dataset not yet registered in DBS
            return runList
        preresult=result['data']
        for key in preresult:
            if 'run' in key:
                runList.append(key['run'][0]['run_number'])
    return runList

#Returns the number of lumis in a given run for a dataset using DAS.
def getRunLumiCountDatasetRun(das_url, dataset, run):
    lumis=0
    query="summary dataset="+dataset+"  run="+str(run)+" | sum(summary.nlumis)"
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        if len(result['data'])==0:#dataset not yet registered in DBS
            return 0
        preresult=result['data']
        for key in preresult:
            if 'result' in key:
                return key['result']['value']        
        return -1

# Get the number of unique lumis in a dataset
def getRunLumiCountDataset(das_url, dataset):
        lumis=0
        query="summary dataset="+dataset+" | grep summary.nlumis"
        das_data = get_data(das_url,query,0,0,0)
        if isinstance(das_data, basestring):
            result = json.loads(das_data)
        else:
            result = das_data
        if result['status'] == 'fail' :
            print 'DAS query failed with reason:',result['reason']
        else:
            if len(result['data'])==0:#dataset not yet registered in DBS
                    return 0
            preresult=result['data'][0]
            for key in preresult:
                if 'summary' in key:
                    return preresult['summary'][0]['nlumis']               
            return -1


#Returns the number of lumis in a dataset and in a given runlist
def getRunLumiCountDatasetListDAS(das_url,dataset, runlist):
    lumis=0
    runChunks=chunks(runlist,30)
    for runList in runChunks:
        lumis=lumis+getRunLumiCountDatasetListLimited(das_url,dataset, runList)
    return lumis

#Returns the number of lumis in a dataset and in a given runlist but with a limit of 30 runs    
def getRunLumiCountDatasetListLimited(das_url,dataset, runlist):
    lumis=0
    query="summary dataset="+dataset+" run in "+str(runlist)+ " | grep summary.nlumis"
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        if len(result['data'])==0:#dataset not yet registered in DBS
            return 0
        preresult=result['data'][0]
        for key in preresult:
            if 'summary' in key:
                return preresult['summary'][0]['nlumis']        
        return -1
    

#GEN check generator true if everything is fine, false otherwise
def checkCorrectLumisEventGEN(dataset):
    das_url=das_host
    numlumis=getRunLumiCountDataset(das_url, dataset)
    numEvents=getEventCountDataSet(das_url, dataset)
    if numlumis>=numEvents/300.0:
        return True
    else:
        return False
  

# SPlits a list of chunks of size(n)
def chunks(lis, n):
    return [lis[i:i+n] for i in range(0, len(lis), n)]
# Return the number of events in a block using DAS
def getEventsBlock(das_url, block_name):
    query="block="+block_name+"  | grep block.nevents"
    das_data = get_data(das_url,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
    if result['status'] == 'fail' :
        print 'DAS quert ', query    
        print 'failed with reason:',result['reason']
    else:
        if len(result['data'])==0:#dataset not yet registered in DBS
            return 0
        preresult=result['data'][0]['block']
        for key in preresult:
            if 'nevents' in key:
                return key['nevents']        
        return -1
    return 0

#Return the number of events for a given dataset given a blocklist
def EventsBlockList(das_url, dataset, blocklist):
    events=0
    if len(blocklist)==0:
        return getEventCountDataSet(das_url, dataset)
    for block in blocklist:
        #if surrouned with "['  ...  ']"
        if block[:2] == "['" and block[-2:] == "']":
            block = block[2:-2]
        events=events+getEventsBlock(das_url, block)
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
            return getRunLumiCountDataset(das_host,inputDataSet)-getRunLumiCountDatasetBlockList( inputDataSet,blockBlacklist)
        if len(runWhitelist)>0:
            return getRunLumiCountDatasetListDAS(das_host, inputDataSet, runWhitelist)
        else:
            return getRunLumiCountDataset(das_host,inputDataSet)

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
    #try until no exception
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
    #In case some parameters miss in the request like blockwhitelist, blockblack list and so on or it was injected as a string.
    for listitem in ["RunWhitelist", "RunBlacklist", "BlockWhitelist",
                           "BlockBlacklist"]:
        if listitem in request:
            if request[listitem]=='[]':
                request[listitem]=[]
            if request[listitem][:1] == "[" and request[listitem][-1:] == "]":
                request[listitem] = request[listitem][1:-1]
                request[listitem] = request[listitem].replace("'", "");
            if type(request[listitem]) is not list:#if there is not a list but some elements it creates a list
                request[listitem]=re.split(r",",request[listitem])
        else:
            request[listitem]=[]
    inputDataSet=request['InputDataset']
    if requestType=='ReReco':
        if len(request['BlockWhitelist'])>0:
            return getRunLumiCountDatasetBlockList(request['InputDataset'],request['BlockWhitelist'])
        if len(request['BlockBlacklist'])>0:
            return getRunLumiCountDataset(request['InputDataset'])-getRunLumiCountDatasetBlockList(request['InputDataset'],request['BlockBlacklist'])
        if len(request['RunWhitelist'])>0:
            return getRunLumiCountDatasetListDAS(das_host, request['InputDataset'], request['RunWhitelist'])
        else:
            return getRunLumiCountDataset(das_host, request['InputDataset'])
    
    events=getEventCountDataSet(das_host, request['InputDataset'])
    if len(request['BlockBlacklist'])>0:
        events=events-EventsBlockList(request['InputDataset'], request['BlockBlacklist'])
    if len(request['RunWhitelist'])>0:
        events=EventsRunList(das_host, request['InputDataset'], request['RunWhitelist'])
    if len(request['BlockWhitelist'])>0:
        events=EventsBlockList(das_host, request['InputDataset'], request['BlockWhitelist'])
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
        return getRunLumiCountDataset(das_host, dataset)
    elif requestType=='TaskChain':
        if 'InputDataset' in request['Task1']:
            return getRunLumiCountDataset(das_host, dataset)
        else:
            return getEventCountDataSet(das_host, dataset)
    else:
        return getEventCountDataSet(das_host, dataset)



def hasAllBlocksClosed(dataset):
    """
    checks if a given dataset has all blocks closed and 
    can be used as input
    """
    query="block dataset="+dataset
    das_data = get_data(das_host,query,0,0,0)['data']
    #traverse blocks
    for ds in das_data:                
        #print 'block', ds['block'][0]['name']
        for block in ds['block']:
            #print '  is_open', block['is_open'] if 'is_open' in block else "?"
            if 'is_open' not in block:
                pass
            elif block['is_open'] == 'y':
                return False
    return True

def main():
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:dbsTest workflowname"
        sys.exit(0)
    workflow=args[0]
    url='cmsweb.cern.ch'
    outputDataSets=deprecated.phedexSubscription.outputdatasetsWorkflow(url, workflow)
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

