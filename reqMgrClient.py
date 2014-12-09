#!/usr/bin/env python
"""
    This client encapsulates several basic queries to request manager.
    This uses ReqMgr rest api through HTTP
    url parameter is normally 'cmsweb.cern.ch'
"""

import urllib2,urllib, httplib, sys, re, os, json
from xml.dom.minidom import getDOMImplementation
import dbs3Client as dbs3

# default headers for PUT and POST methods
def_headers={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}

class Workflow:
    """
    Wraps all information available on ReqMgr
    To avoid querying the same stuff multiple times
    This is useful for closeout script
    """
    def __init__(self, name, url='cmsweb.cern.ch'):
        """
        Initialization
        """
        self.name = name
        self.url = url
        #from the workflow Info
        self.info = getWorkflowInfo(url, name)
        self.cache = getWorkloadCache(url, name)
        self.status = self.info['RequestStatus']
        self.type = self.info['RequestType']
        if 'SubRequestType' in self.info:
            self.subType = self.info['SubRequestType']
        else:
            self.subType = None
        self.outputDatasets = outputdatasetsWorkflow(url, name)
        if 'teams' in self.info and len(self.info['teams']) > 0 :
            self.team = self.info['teams'][0]
        else:
            self.team = 'NoTeam'
        if 'FilterEfficiency' in self.info:
            self.filterEfficiency = float(self.info['FilterEfficiency'])
        self.outEvents = {}


    def getInputEvents(self):
        """
        Gets the inputs events of a given workflow
        depending of the kind of workflow
        """
        raise Exception("Not implemented")
    
    def getOutputEvents(self, ds):
        """
        gets the output events on one of the output datasets
        """
        #We store the events to avoid checking them twice
        if ds not in self.outEvents:
            events = dbs3.getEventCountDataSet(ds)
            self.outEvents[ds] = events
        else:
            events = self.outEvents[ds]
        return events

    def percentageCompletion(self, ds):
        """
        Calculates Percentage of events produced for a given workflow
        taking a particular output dataset
        """
        inputEvents = self.getInputEvents()
        outputEvents = self.getOutputEvents(ds)
        if inputEvents == 0:
            return 0
        if not outputEvents:
            return 0
        perc = outputEvents/float(inputEvents)
        return perc
        
    def __getattr__(self, value):
        """
        To behave like a dictionary
        """
        return self.info[value]

class MonteCarlo(Workflow):
    """
    MonteCarlo from scratch (no dataset input needed).
    """
    def __init__(self, name, url='cmsweb.cern.ch'):
        Workflow.__init__(self, name, url)

    def getInputEvents(self):
        #if request is montecarlo or Step0, the numer of
        #input events is by the requsted events
        if self.type == 'MonteCarlo' or self.type == 'LHEStepZero':
            if 'RequestNumEvents' in self.info and self.info['RequestNumEvents']>0:
                    return self.info['RequestNumEvents']
            elif 'RequestSizeEvents' in self.info:
                return self.info['RequestSizeEvents']
            else:
                return 0
        else:
            raise Exception("Workflow with wrong type")

class StepZero(Workflow):
    """
    Step0 MonteCarlo, no dataset input needed either.
    """
    def __init__(self, name, url='cmsweb.cern.ch'):
        Workflow.__init__(self, name, url)

    def getInputEvents(self):
        #if request is montecarlo or Step0, the numer of
        #input events is by the requsted events
        if self.type == 'MonteCarlo' or self.type == 'LHEStepZero':
            if 'RequestNumEvents' in self.info and self.info['RequestNumEvents']>0:
                    return self.info['RequestNumEvents']
            elif 'RequestSizeEvents' in self.info:
                return self.info['RequestSizeEvents']
            else:
                return 0
        else:
            raise Exception("Workflow with wrong type")

class WorkflowWithInput(Workflow):
    """
    That needs at least one input dataset
    """
    inputlists = ["RunWhitelist", "RunBlacklist", "BlockWhitelist"
                , "BlockBlacklist"]
    
    def __init__(self, name, url='cmsweb.cern.ch'):
        Workflow.__init__(self, name, url)
        if 'InputDataset'in self.info and len(self.info['InputDataset']) > 0:
            self.inputDataset = self.info['InputDataset']
        else:
            raise Exception("This workflow has no input %s"%name)
        self.inputEvents = None
        #fix lists
        for li in self.inputlists:
            #if empty list or no list
            if li not in self.info or self.info[li]=='[]' or self.info[li]=='':
                self.info[li] = []
            #if there is not a list but some elements it creates a list
            if type(self.info[li]) is not list:
                #single element
                if '[' not in self.info[li]:
                    self.info[li] = [self.info[li]]
                #parse a list
                else:
                    self.info[li]= eval(self.info[li])
            #if not, an empty list will do        
            else:
                self.info[li]=[]
    
    def percentageCompletion(self, ds):
        """
        Corrects with filter efficiency
        """
        perc = Workflow.percentageCompletion(self, ds)
        if 'FilterEfficiency' in self.info:
            perc /= self.filterEfficiency
        return perc

    def getInputEvents(self):
        """
        Size of the input, taking white/blacklists into account
        """
        #to avoid quering it twice
        if self.inputEvents:
            return self.inputEvents
        
        events = dbs3.getEventCountDataSet(self.inputDataset)
        #take into account first block lists
        if self.info['BlockWhitelist']:
            events = dbs3.getEventCountDataSetBlockList(self.inputDataset, self.info['BlockWhitelist'])
        elif self.info['BlockWhitelist']:
            #substract black blocks
            self.info -= dbs3.getEventCountDataSetBlockList(self.inputDataset, self.info['BlockWhitelist'])
        elif self.info['RunWhitelist']:
            events = dbs3.getEventCountDataSetRunList(self.inputDataset, self.info['RunWhitelist'])
        elif self.info['RunBlacklist']:
            #substract black runs
            events -= dbs3.getEventCountDataSetRunList(self.inputDataset, self.info['RunBlacklist'])
        self.inputEvents = events
        return events    

class MonteCarloFromGen(WorkflowWithInput):
    """
    Montecarlo using a GEN dataset as input
    """
    def __init__(self, name, url='cmsweb.cern.ch'):
        WorkflowWithInput.__init__(self, name, url)

class ReReco(WorkflowWithInput):
    """
    """
    def __init__(self, name, url='cmsweb.cern.ch'):
        WorkflowWithInput.__init__(self, name, url)
        
class ReDigi(WorkflowWithInput):
    """
    Using a GEN-SIM dataset as input
    """
    def __init__(self, name, url='cmsweb.cern.ch'):
        WorkflowWithInput.__init__(self, name, url)

class StoreResults(WorkflowWithInput):
    """
    Uses a user dataset as input
    """
    def __init__(self, name, url='cmsweb.cern.ch'):
        WorkflowWithInput.__init__(self, name, url)

class TaskChain(Workflow):
    """
    Chained workflow. several steps
    """
    def __init__(self, name, url='cmsweb.cern.ch'):
        Workflow.__init__(self, name, url)
    
    def getInputEvents(self):
        return getInputEventsTaskChain(self.info)

    def getFilterEfficiency(self, task):
        """
        Filter efficiency of a given task
        """
        if task in self.info:
            if 'FilterEfficiency' in self.info[task]:
                filterEff = float(self.info[task]['FilterEfficiency'])
            else:
                filterEff = None
            return filterEff
        #if not found
        return None

def requestManagerGet(url, request, retries=4):
    """
    Queries ReqMgr through a HTTP GET method
    in every request manager query 
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url
    retries: number of retries
    """
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                            key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",request)
    r2=conn.getresponse()
    request = json.loads(r2.read())  
    #try until no exception
    while 'exception' in request and retries > 0:
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                                key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",request)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        retries-=1
    if 'exception' in request:
        raise Exception('Maximum queries to ReqMgr exceeded',str(request))
    return request

def requestManagerPost(url, request, params, head = def_headers):
    """
    Performs some operation on ReqMgr through
    an HTTP POST method.
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url for the POST method
    params: a dict with the POST parameters
    """
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                    key_file = os.getenv('X509_USER_PROXY'))
    headers = head
    encodedParams = urllib.urlencode(params)
    conn.request("POST", request, encodedParams, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return data

def requestManagerPut(url, request, params, head = def_headers):
    """
    Performs some operation on ReqMgr through
    an HTTP PUT method.
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url for the POST method
    params: a dict with the PUT parameters
    head: optional headers param. If not given it takes default value (def_headers)
    """
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                    key_file = os.getenv('X509_USER_PROXY'))
    headers = head
    encodedParams = urllib.urlencode(params)
    conn.request("PUT", request, encodedParams, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return data

def getWorkflowWorkload(url, workflow):
    """
    Gets the workflow loaded, splitted by lines.
    """
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                            key_file = os.getenv('X509_USER_PROXY'))
    request = '/reqmgr/view/showWorkload?requestName=' + workflow
    r1=conn.request("GET",request)
    r2=conn.getresponse()
    data = r2.read()
    #try until no exception
    while 'exception' in request and retries > 0:
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                                key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",request)
        r2=conn.getresponse()
        data = r2.read()
        retries-=1
    if 'exception' in request:
        raise Exception('Maximum queries to ReqMgr exceeded',str(request))

    workload = data.split('\n')
    return workload

def getWorkflowInfo(url, workflow):
    """
    Retrieves workflow information
    """
    request = requestManagerGet(url,'/reqmgr/reqMgr/request?requestName='+workflow)
    return request

def getWorkloadCache(url, workflow):
    """
    Retrieves the ReqMgr Workfload Cache
    """
    request = requestManagerGet(url, '/couchdb/reqmgr_workload_cache/'+workflow)
    return request

def getWorkflowStatus(url, workflow):
    """
    Retrieves workflow status
    """
    request = getWorkflowInfo(url,workflow)
    status = request['RequestStatus']
    return status

def getWorkflowType(url, workflow):
    request = getWorkflowInfo(url,workflow)
    requestType = request['RequestType']
    return requestType

def getWorkflowSubType(url, workflow):
    request = getWorkflowInfo(url,workflow)
    if 'SubRequestType' in request:
        requestSubType=request['SubRequestType']
        return requestSubType
    else:
        return None

def getWorkflowPriority(url, workflow):
    request = getWorkflowInfo(url,workflow)
    if 'RequestPriority' in request:
        return request['RequestPriority']
    else:    
        return 0

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
        #print "No InputDataSet for workflow " +workflow
        return None
    else:
        return inputDataSets

def outputdatasetsWorkflow(url, workflow):
    """
    returns the output datasets for a given workfow
    """
    datasets = requestManagerGet(url,'/reqmgr/reqMgr/outputDatasetsByRequestName?requestName='+workflow)
    return datasets

def getRequestTeam(url, workflow):
    """
    Retrieves the team on which the wf is assigned
    """
    request = getWorkflowInfo(url,workflow)
    if 'teams' not in request:
        return 'NoTeam'
    teams = request['teams']
    if len(teams)<1:
        return 'NoTeam'
    else:
        return teams[0]

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
        return getInputEventsTaskChain(request)

    #if request is not montecarlo, then we need to check the size
    #of input datasets
    #This loops fixes the white and blacklists in the workflow
    #information,
    for listitem in ["RunWhitelist", "RunBlacklist",
                    "BlockWhitelist", "BlockBlacklist"]:
        if listitem in request:
            #if empty
            if request[listitem]=='[]' or request[listitem]=='':
                request[listitem]=[]
            #if there is not a list but some elements it creates a list
            if type(request[listitem]) is not list:
                # if doesn't contain "[" is a single block
                if '[' not in request[listitem]:
                    #wrap in a list
                    request[listitem] = [request[listitem]]
                #else parse a list
                else:
                    request[listitem]= eval(request[listitem])
        #if not, an empty list will do        
        else:
            request[listitem]=[]

    inputDataSet=request['InputDataset']
    
    #it the request is rereco, we valiate white/black lists
    if requestType=='ReReco':
        # if there is block whte list, count only the selected block
        if request['BlockWhitelist']:
            events = dbs3.getEventCountDataSetBlockList(inputDataSet,request['BlockWhitelist'])
        # if there is block black list, substract them from the total
        if request['BlockBlacklist']:
            events = (dbs3.getEventCountDataSet(inputDataSet) - 
                    dbs3.getEventCountDataSet(inputDataSet,request['BlockBlacklist']))
            return events
        # same if a run whitelist
        if request['RunWhitelist']:
            events = dbs3.getEventCountDataSetRunList(inputDataSet, request['RunWhitelist'])
            return events
        # otherwize, the full lumi count
        else:
            events = dbs3.getEventCountDataset(inputDataSet)
            return events
    
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

    #TODO delete FilterEfficiency from here. TEST
    #if 'FilterEfficiency' in request:
    #return float(request['FilterEfficiency'])*events
    #else:
    return events


def getInputLumis(url, workflow):
    """
    Gets the input lumis of a given workflow
    depending of the kind of workflow
    """
    request = getWorkflowInfo(url,workflow)
    requestType=request['RequestType']
    #if request is montecarlo or Step0, the numer of
    #input events is by the requsted events
    if requestType == 'MonteCarlo' or requestType == 'LHEStepZero':
        raise Exception("This request has no input dataset")
    if requestType == 'TaskChain':
        return Exception("Not implemented yet")

    #if request is not montecarlo, then we need to check the size
    #of input datasets
    #This loops fixes the white and blacklists in the workflow
    #information,
    for listitem in ["RunWhitelist", "RunBlacklist",
                    "BlockWhitelist", "BlockBlacklist"]:
        if listitem in request:
            #if empty
            if request[listitem]=='[]' or request[listitem]=='':
                request[listitem]=[]
            #if there is not a list but some elements it creates a list
            if type(request[listitem]) is not list:
                # if doesn't contain "[" is a single block
                if '[' not in request[listitem]:
                    #wrap in a list
                    request[listitem] = [request[listitem]]
                #else parse a list
                else:
                    request[listitem]= eval(request[listitem])
        #if not, an empty list will do        
        else:
            request[listitem]=[]

    inputDataSet=request['InputDataset']
    totalLumis = dbs3.getLumiCountDataSet(inputDataSet)
    #it the request is rereco, we valiate white/black lists
    if requestType=='ReReco':
        # if there is block whte list, count only the selected block
        if request['BlockWhitelist']:
            lumis = dbs3.getLumiCountDataSetBlockList(inputDataSet,request['BlockWhitelist'])
        # if there is block black list, substract them from the total
        if request['BlockBlacklist']:
            lumis = (totalLumis - 
                    dbs3.getLumiCountDataSetBlockList(inputDataSet,request['BlockBlacklist']))
            return lumis
        # same if a run whitelist
        if request['RunWhitelist']:
            lumis = dbs3.getLumiCountDataSetRunList(inputDataSet, request['RunWhitelist'])
            return lumis
        # otherwize, the full lumi count
        else:
            lumis = totalLumis
            return lumis
    lumis = dbs3.getLumiCountDataSet(inputDataSet)
    # if black list, subsctract them    
    if request['BlockBlacklist']:
        lumis = totalLumis - dbs3.getLumiCountDataSetBlockList(inputDataSet, request['BlockBlacklist'])
    # if white list, only the ones in the whitelist.
    if request['RunWhitelist']:
        lumis = totalLumis.getLumiCountDataSetRunList(inputDataSet, request['RunWhitelist'])
    # if white list of blocks
    if request['BlockWhitelist']:
        lumis = dbs3.getLumiCountDataSetBlockList(inputDataSet, request['BlockWhitelist'])

    return lumis


def retrieveSchema(workflowName):
    """
    Creates the cloned specs for the original request
    Updates parameters
    """
    from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
    reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    helper.load(specURL)
    return helper

def getOutputEvents(url, workflow, dataset):
    """
    Gets the output events depending on the type
    of the request
    """
    request = getWorkflowInfo(url, workflow)
    return dbs3.getEventCountDataSet(dataset)

def getFilterEfficiency(url, workflow, task=None):
    """
    Gets the filter efficiency of a given request.
    It can be used for the filter efficiency inside a given
    Task. Returns None if the request has no filter efficiency.
    """
    request = getWorkflowInfo(url, workflow)
    if request["RequestType"] == "TaskChain":
        #get the task with the given input dataset
        if task in request:
            if 'FilterEfficiency' in request[task]:
                filterEff = float(request[task]['FilterEfficiency'])
            else:
                filterEff = None
            return filterEff
        #if not found
        return None
    else:
        if 'FilterEfficiency' in request:
            return float(request['FilterEfficiency'])
        else:
            return None


def getOutputLumis(url, workflow, dataset):
    """
    Gets the output lumis depending on the type
    of the request
    """
    request = getWorkflowInfo(url, workflow)
    return dbs3.getLumiCountDataSet(dataset)
    
def closeOutWorkflow(url, workflowname):
    """
    Closes out a workflow by changing the state to closed-out
    This does not care about cascade workflows
    """
    params = {"requestName" : workflowname,"status" : "closed-out"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def closeOutWorkflowCascade(url, workflowname):
    """
    Closes out a workflow, it will search for any Resubmission requests 
    for which the given request is a parent and announce them too.
    """
    params = {"requestName" : workflowname, "cascade" : True}
    data = requestManagerPost(url,"/reqmgr/reqMgr/closeout", params)
    return data

def announceWorkflow(url, workflowname):
    """
    Sets a workflow state to announced
    This does not care about cascade workflows
    """
    params = {"requestName" : workflowname,"status" : "announced"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def announceWorkflowCascade(url, workflowname):
    """
    Sets a workflow state to announced, it will search for any Resubmission requests 
    for which the given request is a parent and announce them too.
    """
    params = {"requestName" : workflowname, "cascade" : True}
    data = requestManagerPost(url,"/reqmgr/reqMgr/announce", params)
    return data


def setWorkflowApproved(url, workflowname):
    """
    Sets a workflow state to assignment-approved
    """
    params = {"requestName" : workflowname,"status" : "assignment-approved"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def setWorkflowRunning(url, workflowname):
    """
    Sets a workflow state to running
    """
    params = {"requestName" : workflowname,"status" : "running"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def rejectWorkflow(url, workflowname):
    """
    Sets a workflow state to rejected
    """
    params = {"requestName" : workflowname,"status" : "rejected"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def abortWorkflow(url, workflowname):
    """
    Sets a workflow state to aborted
    """
    params = {"requestName" : workflowname,"status" : "aborted"}
    data = requestManagerPut(url,"/reqmgr/reqMgr/request", params)
    return data

def cloneWorkflow(url, workflowname):
    """
    This clones a request
    """
    headers={"Content-Length": 0}
    params = {}
    data = requestManagerPut(url,"/reqmgr/reqMgr/clone/", params, headers)
    return data

def submitWorkflow(url, schema):
    """
    This submits a workflow into the ReqMgr, can be used for cloning
    and resubmitting workflows
    url: the instance ued, i.e. 'cmsweb.cern.ch'
    schema: A dictionary with the parameters needed to create
    the workflow
    
    """
    data = requestManagerPost(url,"/reqmgr/create/makeSchema", schema)
    return data

def setWorkflowSplitting(url, schema):
    """
    This sets the workflow splitting into ReqMgr
    """
    data = requestManagerPost(url,"/reqmgr/view/handleSplittingPage", schema)
    return data

def getInputEventsTaskChain(request):
    """
    Calculates input events for a taskchain based on the
    TaskChain properties and subtype
    """
    #TODO filter by subtype
    #if it's MC from scratch, it has a set number of requested events
    if 'RequestNumEvents' in request['Task1']:
        if request['Task1']['RequestNumEvents'] is not None:
            return request['Task1']['RequestNumEvents']
    #if it has an input dataset
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

        if blockWhitelist:
            return dbs3.getEventCountDataSetBlockList(inputDataSet,blockWhitelist)
        if blockBlacklist:
            return dbs3.getEventCountDataset(inputDataSet) - dbs3.getEventCountDataSetBlockList(inputDataSet,blockBlacklist)
        if runWhitelist:
            return dbs3.getEventCountDataSetRunList(inputDataSet, runWhitelist)
        else:
            return dbs3.getEventCountDataset(inputDataSet)
    #TODO what if intermediate steps have filter efficiency?
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





