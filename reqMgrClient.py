#!/usr/bin/env python
"""
    This client encapsulates several basic queries to request manager.

"""


import urllib2,urllib, httplib, sys, re, os, json, phedexSubscription
from xml.dom.minidom import getDOMImplementation
import dbs3Client as dbs3

#das_host='https://das.cern.ch'
#das_host='https://cmsweb.cern.ch'
das_host='https://cmsweb-testbed.cern.ch'
#das_host='https://das-dbs3.cern.ch'
#das_host='https://dastest.cern.ch'

def getWorkflowInfo(url, workflow, retries=4):
    """
    Retrieves workflow information
    Using Request Manager
    """
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.loads(r2.read())  
    #try until no exception
    while 'exception' in request and retries > 0:
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        retries-=1
    if 'exception' in request:
        raise Exception('Maximum queries to req manager retried',str(request))
    return request


def getWorkflowType(url, workflow):
    request = getWorkflowInfo(url,workflow)
    requestType=request['RequestType']
    return requestType

def getRunWhitelist(url, workflow):
    request = getWorkflowInfo(url,workflow)
    runWhitelist=request['RunWhitelist']
    return runWhitelist

def getBlockWhitelist(url, workflow):
    request = getWorkflowInfo(url,workflow)
    BlockWhitelist=request['BlockWhitelist']
    return BlockWhitelist

def getInputDataSet(url, workflow):
    request = getWorkflowInfo(url,workflow)
    inputDataSets=request['InputDataset']
    if len(inputDataSets)<1:
        print "No InputDataSet for workflow " +workflow
    else:
        return inputDataSets


def getInputEvents(url, workflow):
    """
    Gets the inputs events of a given workflow
    depending of the kind of workflow
    """
    request = getWorkflowInfo(url,workflow)
    requestType=request['RequestType']
    #if request is montecarlo or Step0, the numer of
    #input events is by the requsted events
    if requestType == 'MonteCarlo' or requestType == 'LHEStepZero':
        if 'RequestNumEvents' in request:
            if request['RequestNumEvents']>0:
                return request['RequestNumEvents']
        if 'RequestSizeEvents' in request:
            return request['RequestSizeEvents']
        else:
            return 0
    if requestType == 'TaskChain':
        return handleTaskChain(request)

    #if request is not montecarlo, then we need to check the size
    #of input datasets
    #This loops fixes the white and blacklists in the workflow
    #information,
    for listitem in ["RunWhitelist", "RunBlacklist",
                    "BlockWhitelist", "BlockBlacklist"]:
        if listitem in request:
            if request[listitem]=='[]':
                request[listitem]=[]
            #if there is not a list but some elements it creates a list
            if type(request[listitem]) is not list:
                request[listitem]=re.split(r",",request[listitem])
        #if not, an empty list will do        
        else:
            request[listitem]=[]

    inputDataSet=request['InputDataset']
    
    #it the request is rereco, we valiate white/black lists
    if requestType=='ReReco':
        # if there is block whte list, count only the selected block
        if request['BlockWhitelist']:
            return dbs3.getEventCountDataSetBlockList(inputDataSet,request['BlockWhitelist'])
        # if there is block black list, substract them from the total
        if request['BlockBlacklist']:
            return (dbs3.getEventCountDataSet(inputDataSet) - 
                    dbs3.getEventCountDataSet(inputDataSet,request['BlockBlacklist']))
        # same if a run whitelist
        if request['RunWhitelist']:
            return dbs3.getEventCountDataSetRunList(inputDataSet, request['RunWhitelist'])
        # otherwize, the full lumi count
        else:
            return dbs3.getEventCountDataset(inputDataSet)
    
    events = dbs3.getEventCountDataSet(inputDataSet)
    # if black list, subsctract them    
    if request['BlockBlacklist']:
        events=events-dbs3.getEventCountDataSetBlockList(inputDataSet, request['BlockBlacklist'])
    # if white list, only the ones in the whitelist.
    if request['RunWhitelist']:
        events=dbs3.getEventCountDataSetRunList(inputDataSet, request['RunWhitelist'])
    # if white list of blocks
    if request['BlockWhitelist']:
        events=dbs3.getEventCountDataSetBlockList(inputDataSet, request['BlockWhitelist'])
    if 'FilterEfficiency' in request:
        return float(request['FilterEfficiency'])*events
    else:
        return events

def getOutputEvents(url, workflow, dataset):
    """
    Gets the output events depending on the type
    if the request
    """
    request = getWorkflowInfo(url,workflow)
    return dbs3.getEventCountDataSet(dataset)


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
            return getRunLumiCountDatasetBlockList(das_host, inputDataSet,blockWhitelist)
        if len(blockBlacklist)>0:
            return getRunLumiCountDataset(das_host,inputDataSet)-getRunLumiCountDatasetBlockList(das_host, inputDataSet,blockBlacklist)
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


