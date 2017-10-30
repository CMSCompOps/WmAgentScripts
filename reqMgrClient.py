#!/usr/bin/env python
"""
    This client encapsulates several basic queries to request manager.
    This uses ReqMgr rest api through HTTP
    url parameter is normally 'cmsweb.cern.ch'
"""

import copy
import httplib
import json
import optparse
import os
import re
import sys
import time
import urllib

import dbs3Client as dbs3

# default headers for PUT and POST methods
def_headers={"Content-type": "application/json", "Accept": "application/json"}
def_headers1={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}

CERT_FILE = os.getenv('X509_USER_PROXY')
#print CERT_FILE
KEY_FILE = os.getenv('X509_USER_PROXY')
#print KEY_FILE

#CERT_FILE = os.getenv('X509_USER_CERT')
#print CERT_FILE
#KEY_FILE = os.getenv('X509_USER_KEY')
#print KEY_FILE

class Workflow:
    """
    Wraps all information available on ReqMgr
    To avoid querying the same stuff multiple times
    This is useful for closeout script
    """
    def __init__(self, name, url='cmsweb.cern.ch', workflow=None):
        """
        Initialization
        """
        #true if a object for copy was provided
        obj = (workflow is not None) and isinstance(workflow, Workflow)

        self.name = name
        self.url = url
        #from the workflow Info
        #if workflow object was provided, deep copy, otherwise pull info
        if obj:
            self.info = workflow.info
        else:
            self.info = getWorkflowInfo(url, name)

        self.status = self.info['RequestStatus']
        self.type = self.info['RequestType']
        if 'SubRequestType' in self.info:
            self.subType = self.info['SubRequestType']
        else:
            self.subType = None
        #if object was provided no need to pull the info
        if obj:
            self.outputDatasets = workflow.outputDatasets
        else:
            self.outputDatasets = outputdatasetsWorkflow(url, name)

        if 'Teams' in self.info and len(self.info['Teams']) > 0 :
            self.team = self.info['Teams'][0]
        else:
            self.team = 'NoTeam'
        if 'FilterEfficiency' in self.info:
            self.filterEfficiency = float(self.info['FilterEfficiency'])
        self.outEvents = {}
        self.outLumis = {}


    def getInputEvents(self):
        """
        Gets the inputs events of a given workflow
        depending of the kind of workflow, by default gets
        it from the workload cache.
        """
        ev = 0
        if 'TotalInputEvents' in self.info:
            ev = self.info['TotalInputEvents']
        if not ev:
            ev = self.info['RequestNumEvents']/self.info['FilterEfficiency']
        return ev

    def getInputLumis(self):
        """
        Gets the inputs lumis of a given workflow
        depending of the kind of workflow, by default gets
        it from the workload cache.
        """
        if 'TotalInputLumis' in self.info:
            return self.info['TotalInputLumis']
        return 0
    
    def getOutputEvents(self, ds, skipInvalid=False):
        """
        gets the output events on one of the output datasets
        """
        #We store the events to avoid checking them twice
        if ds not in self.outEvents:
            events = dbs3.getEventCountDataSet(ds, skipInvalid)
            self.outEvents[ds] = events
        else:
            events = self.outEvents[ds]
        return events

    def getOutputLumis(self, ds, skipInvalid=False):
        """
        Gets the numer of lumis in an output dataset
        """
        #We store the events to avoid checking them twice
        if ds not in self.outLumis:
            lumis = dbs3.getLumiCountDataSet(ds, skipInvalid)
            self.outLumis[ds] = lumis
        else:
            lumis = self.outLumis[ds]
        return lumis

    def percentageCompletion(self, ds, skipInvalid=False):
        """
        Calculates Percentage of lumis produced for a given workflow
        taking a particular output dataset
        """
        inputEvents = self.getInputLumis()
        outputEvents = self.getOutputLumis(ds, skipInvalid)
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
    def __init__(self, name, url='cmsweb.cern.ch', workflow=None):
        Workflow.__init__(self, name, url, workflow)

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
    def __init__(self, name, url='cmsweb.cern.ch', workflow=None):
        Workflow.__init__(self, name, url, workflow)

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
                , "BlockBlacklist", "LumiList"]
    
    def __init__(self, name, url='cmsweb.cern.ch', workflow=None):
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
        self.inputLumisFromDset = None
    
    def percentageCompletion(self, ds, skipInvalid=False, checkInput=False):
        """
        Calculates the percentage of completion based on lumis
        if checkInput=True, the ammount of lumis is taken from the input
        dataset (take into account the white/blacklist are not calculated
        """
        
        inputEvents = self.getInputLumis(checkInput=checkInput)
        outputEvents = self.getOutputLumis(ds, skipInvalid)
        if inputEvents == 0:
            return 0
        if not outputEvents:
            return 0
        perc = outputEvents/float(inputEvents)
        return perc

    def getInputLumis(self, checkList = False, checkInput=False):
        """
        Checks against lumi list
        """
        if not checkList and not checkInput:
            return Workflow.getInputLumis(self)
        if checkInput:
            #retrieve lumis of the inpu dataset
            return dbs3.getLumiCountDataSet(self.inputDataset)
        if checkList:
            runLumis = self.info['LumiList']
            if runLumis:
                total = 0
                for run, lumiList in runLumis.items():
                    total += sum(l2 - l1 + 1 for l1, l2 in lumiList)
                return total
            return 0

class MonteCarloFromGen(WorkflowWithInput):
    """
    Montecarlo using a GEN dataset as input
    """
    def __init__(self, name, url='cmsweb.cern.ch', workflow=None):
        WorkflowWithInput.__init__(self, name, url, workflow)

class ReReco(WorkflowWithInput):
    """
    """
    def __init__(self, name, url='cmsweb.cern.ch', workflow=None):
        WorkflowWithInput.__init__(self, name, url, workflow)
        
class ReDigi(WorkflowWithInput):
    """
    Using a GEN-SIM dataset as input
    """
    def __init__(self, name, url='cmsweb.cern.ch', workflow=None):
        WorkflowWithInput.__init__(self, name, url, workflow)

class StoreResults(WorkflowWithInput):
    """
    Uses a user dataset as input
    """
    def __init__(self, name, url='cmsweb.cern.ch', workflow=None):
        WorkflowWithInput.__init__(self, name, url, workflow)

class TaskChain(Workflow):
    """
    Chained workflow. several steps
    """
    def __init__(self, name, url='cmsweb.cern.ch', workflow=None):
        Workflow.__init__(self, name, url, workflow)
    
    def getInputDataset(self):
        task1 = self.info['Task1']
        if 'InputDataset' in task1:
            return task1['InputDataset']
        return None
    
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

def createWorkflowObject(name, url='cmsweb.cern.ch'):
    """
    Factory method
    Creates a Workflow object casted to the 
    specific class to its type
    """
    wf = Workflow(name , url)
    if wf.type == "MonteCarlo" and re.search('.*/GEN$', wf.outputDatasets[0]):
        wf = StepZero(name, url, wf)
    elif wf.type == "MonteCarlo":
        wf = MonteCarlo(name, url, wf)
    elif wf.type == "MonteCarloFromGEN":
        wf = MonteCarloFromGen(name, url, wf)
    elif wf.type == "ReDigi":
        wf = ReDigi(name, url, wf)
    elif wf.type == "ReReco":
        wf = ReReco(name, url, wf)
    elif wf.type == "StoreResults":
        wf = StoreResults(name, url, wf)
    elif wf.type == "TaskChain":
        wf = TaskChain(name, url, wf)
    return wf


def requestManagerGet(url, request, retries=4):
    """
    Queries ReqMgr through a HTTP GET method
    in every request manager query 
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url
    retries: number of retries
    """
    conn  =  httplib.HTTPSConnection(url, cert_file = CERT_FILE,
                                            key_file = KEY_FILE)
    headers = {"Accept": "application/json"}
    r1=conn.request("GET",request, headers=headers)
    r2=conn.getresponse()
    request = json.loads(r2.read())  
    #try until no exception
    while 'exception' in request and retries > 0:
        conn  =  httplib.HTTPSConnection(url, cert_file = CERT_FILE,
                                                key_file = KEY_FILE)
        r1=conn.request("GET",request, headers=headers)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        retries-=1
    if 'exception' in request:
        raise Exception('Maximum queries to ReqMgr exceeded',str(request))
    return request

#def _convertToRequestMgrPostCall(url, request, params, head):
#    header = {"Content-type": "application/json", "Accept": "application/json"}
#    if request == '/reqmgr/create/makeSchema':
#        request = "/reqmgr2/data/request"
#        data, status = _post(url, request, params, head=header, encode=json.dumps)
#    else:
#        raise Exception("no correspondonting reqmgr2 call for %s" % request)
#    return data
    
def requestManager1Post(url, request, params, head = def_headers1, nested=False):
    """
    Performs some operation on ReqMgr through
    an HTTP POST method.
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url for the POST method
    params: a dict with the POST parameters
    nested: deep encode a json parameters
    """
    if nested:
        jsonEncodedParams ={}
        for pKey in params:
            jsonEncodedParams[pKey] = json.dumps(params[pKey])
        encodedParams = urllib.urlencode(jsonEncodedParams)
    else:
        encodedParams = urllib.urlencode(params)
    
    data, status = _post(url, request, encodedParams, head, encode=None)
    return data
    
def requestManagerPost(url, request, params, head = def_headers):
    """
    Performs some operation on ReqMgr through
    an HTTP POST method.
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url for the POST method
    params: a dict with the POST parameters
    nested: deep encode a json parameters
    """
    data, status = _post(url, request, params, head, encode=json.dumps)
    return data

def _put(url, request, params, head=def_headers, encode=urllib.urlencode):
    return _httpsRequest("PUT", url, request, params, head, encode)

def _post(url, request, params, head=def_headers, encode=urllib.urlencode):
    return _httpsRequest("POST", url, request, params, head, encode)

def _httpsRequest(verb, url, request, params, head, encode):
    conn  =  httplib.HTTPSConnection(url, cert_file = CERT_FILE,
                                    key_file = KEY_FILE)
    headers = head
    if encode:
        encodedParams = encode(params)
    else:
        encodedParams = params
    conn.request(verb, request, encodedParams, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return (data, response.status)

def requestManager1Put(url, request, params, head = def_headers1):
    """
    Performs some operation on ReqMgr through
    an HTTP PUT method.
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url for the POST method
    params: a dict with the PUT parameters
    head: optional headers param. If not given it takes default value (def_headers)
    """
    
    data, status = _put(url, request, params, head)
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
    data, status = _put(url, request, params, head, encode=json.dumps)
    return data

def getWorkflowWorkload(url, workflow, retries=4):
    """
    Gets the workflow loaded, splitted by lines.
    """
    print "getWorkflowWorkload is Deprecated"
    return None

def getWorkflowInfo(url, workflow):
    """
    Retrieves workflow information
    """
    retries=10000
    while retries:
        try:
            request = requestManagerGet(url,'/reqmgr2/data/request?name='+workflow)
            return request['result'][0][workflow]
        except:
            time.sleep(1)
            retries -=1
            print "beep"
    return None

def isRequestMgr2Request(url, workflow):
    result = getWorkflowInfo(url, workflow)
    return result.get("ReqMgr2Only", False)

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
    results = requestManagerGet(url,'/reqmgr2/data/request?name='+workflow)['result']
    request = results[0][workflow]
    datasets = []
    if "OutputDatasets" in request:
        datasets.extend(request['OutputDatasets'])
        
    if "TaskChain" in request:
        for num in range(request['TaskChain']):
            if"OutputDatasets" in request["Task%i" % (num+1)]:
                datasets.extend(request['OutputDatasets'])
    
    if "StepChain" in request:
        for num in range(request['StepChain']):
            if "OutputDatasets" in request["Step%i" % (num+1)]:
                datasets.extend(request['OutputDatasets'])
    
    return datasets

def getRequestTeam(url, workflow):
    """
    Retrieves the team on which the wf is assigned
    """
    request = getWorkflowInfo(url,workflow)
    if 'Teams' not in request:
        return 'NoTeam'
    teams = request['Teams']
    if len(teams)<1:
        return 'NoTeam'
    else:
        return teams[0]

def getInputEvents(url, workflow):
    """
    Gets the inputs events of a given workflow
    depending of the kind of workflow
    TODO this can be replaced by getting the info from the workload cache
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
            events = dbs3.getEventCountDataSet(inputDataSet)
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
    TODO this can be replaced by getting it from the workload cache
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


def retrieveSchema(workflowName, reqmgrCouchURL = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"):
    """
    Creates the cloned specs for the original request
    Updates parameters
    """
    from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    helper.load(specURL)
    return helper

def getOutputEvents(url, workflow, dataset):
    """
    Gets the output events depending on the type
    of the request
    """
    # request = getWorkflowInfo(url, workflow)
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


def getOutputLumis(url, workflow, dataset, skipInvalid=False):
    """
    Gets the output lumis depending on the type
    of the request
    """
    # request = getWorkflowInfo(url, workflow)
    return dbs3.getLumiCountDataSet(dataset, skipInvalid)
    
def assignWorkflow(url, workflowname, team, parameters ):
    assignWorkflow.errorMessage = ""
    #local import so it doesn't screw with all other stuff
    defaults = copy.deepcopy( assignWorkflow.defaults )
    if 'Team' in parameters: ## this needs to go away with moving away from old assignment to new in all scripts
        team = parameters['Team'] 
    else:
        parameters['Team'] = team 

    #defaults["checkbox"+workflowname] = "checked"
    #defaults["Team"+team] = "checked"
    defaults["RequestName"] = workflowname
    defaults["RequestStatus"] = 'assigned'

    from utils import workflowInfo

    wf = workflowInfo(url, workflowname)

    # set the maxrss watchdog to what is specified in the request
    if 'Memory' in parameters:
        wf.request['Memory'] = parameters['Memory']

    maxRSSs = {}
    chain_type = 'Step' if 'Step1' in wf.request else ('Task' if 'Task1' in wf.request else None)
    if chain_type:
        t_i = 1
        while True:
            t_s = '%s%d'%(chain_type, t_i)
            if not t_s in wf.request: break
            t_n = wf.request[t_s]['%sName'%chain_type]
            if type(wf.request['Memory'])==dict and t_n in wf.request['Memory']:
                maxRSSs[t_n] = int(wf.request['Memory'][t_n])*1024
            elif 'Memory' in wf.request[t_s]:
                maxRSSs[t_n] = int(wf.request[t_s]['Memory'])*1024
            else:
                #nothing partial
                maxRSSs = {}
            t_i+=1
    if maxRSSs and chain_type=='Task':
        defaults['MaxRSS'] = maxRSSs ## set it as a dict
    elif maxRSSs and chain_type=='Step':
        defaults['MaxRSS'] = int(max(maxRSSs.values()))*1024 ## set it to the max value
    else:
        defaults['MaxRSS'] = int(wf.request['Memory'])*1024 ## set it to the indicated value

    defaults.update( parameters )

    if 'Multicore' in parameters:
        pass
    elif 'Multicore' in wf.request and (wf.request['Multicore']>1) and not wf.request['RequestType']=='TaskChain':
        ## hack for multicode assignment

        defaults['Multicore'] = wf.request['Multicore']
    #if ('Multicore' in wf.request and wf.request['Multicore']>1):
    #    defaults['MaxRSS'] = int((wf.request['Memory']*1024+10) * 1.5 * wf.request['Multicore'])
    #    defaults['MaxVSize'] = int(10*defaults['MaxRSS'])
    
    pop_useless = ['AcquisitionEra','ProcessingString']
    for what in pop_useless:
        if defaults[what] == None:
            defaults.pop(what)

    if not set(assignWorkflow.mandatories).issubset( set(parameters.keys())):
        assignWorkflow.errorMessage = "There are missing parameters\n"
        assignWorkflow.errorMessage += str(list(set(assignWorkflow.mandatories) - set(parameters.keys())))
        print assignWorkflow.errorMessage
        return False


    if wf.request['RequestType'] in ['ReDigi','ReReco']:
        defaults['Dashboard'] = 'reprocessing'
    elif 'SubRequestType' in wf.request and wf.request['SubRequestType'] in ['ReDigi']:
        defaults['Dashboard'] = 'reprocessing'
    elif wf.isRelval():
        defaults['Dashboard'] = 'relval'


    if defaults['SiteBlacklist'] and defaults['SiteWhitelist']:
        defaults['SiteWhitelist'] = list(set(defaults['SiteWhitelist']) - set(defaults['SiteBlacklist']))
        defaults['SiteBlacklist'] = []
        if not defaults['SiteWhitelist']:
            assignWorkflow.errorMessage = "Cannot assign with no site whitelist"
            print assignWorkflow.errorMessage
            return False


    for aux in assignWorkflow.auxiliaries:
        if aux in defaults: 
            par = defaults.pop( aux )

            if aux == 'EventsPerJob':
                wf = workflowInfo(url, workflowname)
                t = wf.firstTask()
                par = int(float(par))
                params = wf.getSplittings()[0]
                if par < params['events_per_job']:
                    params.update({"requestName":workflowname,
                                   "splittingTask" : '/%s/%s'%(workflowname,t),
                                   "events_per_job": par,
                                   "splittingAlgo":"EventBased"})
                    print setWorkflowSplitting(url, workflowname, params)
            elif aux == 'EventsPerLumi':
                wf = workflowInfo(url, workflowname)
                t = wf.firstTask()
                params = wf.getSplittings()[0]
                if wf.request['RequestType']  == 'StepChain':
                    print "Ignoring changing eventsperlumi in stepchain"
                    continue
                if params['splittingAlgo'] != 'EventBased': 
                    print "Ignoring changing events per lumi for",params['splittingAlgo']
                    continue
                (_,prim,_,_) = wf.getIO()
                if prim:
                    print "Ignoring changing events per lumi for wf that take input"
                    continue

                if str(par).startswith('x'):
                    multiplier = float(str(par).replace('x',''))
                    par = int(params['events_per_lumi'] * multiplier)
                else:
                    if 'FilterEfficiency' in wf.request and wf.request['FilterEfficiency']:
                        par = int(float(par)/wf.request['FilterEfficiency'])
                    else:
                        par = int(float(str(par)))

                params.update({"requestName":workflowname,
                               "splittingTask" : '/%s/%s'%(workflowname,t),
                               "events_per_lumi": par})
                print setWorkflowSplitting(url, workflowname, params)
            elif aux == 'SplittingAlgorithm':
                wf = workflowInfo(url, workflowname)
                ### do it for all major tasks
                #for (t,params) in wf.getTaskAndSplittings():
                #    params.update({"requestName":workflowname,
                #                   "splittingTask" : '/%s/%s'%(workflowname,t),
                #                   "splittingAlgo" : par})
                #    setWorkflowSplitting(url, workflowname, params)
                t = wf.firstTask()
                params = wf.getSplittings()[0]
                params.update({"requestName":workflowname,
                               "splittingTask" : '/%s/%s'%(workflowname,t),
                               "splittingAlgo" : par})
                #swap values
                if "avg_events_per_job" in params and not "events_per_job" in params:
                    params['events_per_job' ] = params.pop('avg_events_per_job')
                print params
                print setWorkflowSplitting(url, workflowname, params)
            elif aux == 'LumisPerJob': 
                wf = workflowInfo(url, workflowname)
                t = wf.firstTask()
                #params = wf.getSplittings()[0]
                params = {"requestName":workflowname,
                          "splittingTask" : '/%s/%s'%(workflowname,t),
                          "lumis_per_job" : int(par),
                          "halt_job_on_file_boundaries" : True,
                          "splittingAlgo" : "LumiBased"}
                print setWorkflowSplitting(url, workflowname, params)
            else:
                print "No action for ",aux

    if not 'execute' in defaults or not defaults['execute']:
        assignWorkflow.errorMessage = json.dumps( defaults ,indent=2)
        print assignWorkflow.errorMessage
        return False
    else:
        defaults.pop('execute')
        print "These are the parameters to be used"
        print json.dumps( defaults ,indent=2)
        

    res = setWorkflowAssignment(url, workflowname, defaults)
    if res:
        print 'Assigned workflow:',workflowname,'to site:',defaults['SiteWhitelist'],'and team',team
        return True
    else:
        print "error in assigning",workflowname
        assignWorkflow.errorMessage = setWorkflowAssignment.errorMessage
        return False

assignWorkflow.errorMessage = ""
assignWorkflow.defaults= {
        #"action": "Assign",
        "SiteBlacklist": [],
        "TrustSitelists" : False,
        "TrustPUSitelists" : False,
        #"useSiteListAsLocation" : False,
        "UnmergedLFNBase": "/store/unmerged",
        "MinMergeSize": 2147483648,
        "MaxMergeSize": 4294967296,
        "MaxMergeEvents" : 200000, ## shouldn't this be set to infinite ?
        'BlockCloseMaxEvents' : 2000000,
        'BlockCloseMaxWaitTime' : 172800,
        "MaxRSS" : 3000000,
        "MaxVSize": 4394967000,
        "Dashboard": "production",
        "SoftTimeout" : 159600, ## 44H watchdog, regardless of the requested time ...
        "GracePeriod": 300,
        'CustodialSites' : [], ## make a custodial copy of the output there
        "CustodialSubType" : 'Replica', ## move will screw it over ?
        'NonCustodialSites' : [],
        "NonCustodialSubType" : 'Replica', ## that's the default, but let's be sure
        'AutoApproveSubscriptionSites' : [],
        'NonCustodialGroup' : 'DataOps'
        #'Multicore' : 1
        }
assignWorkflow.mandatories = ['Team',
                              'SiteWhitelist',
                              'AcquisitionEra',
                              'ProcessingVersion',
                              'ProcessingString',
                              'MergedLFNBase',
                              
                              #'CustodialSites', ## make a custodial copy of the output there
                              
                              #'SoftTimeout',
                              #'BlockCloseMaxEvents',
                              #'MinMergeSize',
                              #'MaxMergeEvents',
                              #'MaxRSS'
                              ]
assignWorkflow.auxiliaries = [ 'SplittingAlgorithm',
                               'EventsPerJob',
                               'EventsPerLumi',
                               'LumisPerJob',
                               ]

assignWorkflow.keys = assignWorkflow.mandatories+assignWorkflow.defaults.keys() + assignWorkflow.auxiliaries


def changePriorityWorkflow(url, workflowname, priority):
    """
    Change the priority of a workflow
    """
    if isRequestMgr2Request(url, workflowname):
        params = {"RequestPriority" : priority}
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, params)
    else:
        params = {workflowname + ":status": "", workflowname + ":priority": str(priority)}
        data = requestManager1Post(url, "/reqmgr/view/doAdmin", params)

def forceCompleteWorkflow(url, workflowname):
    """
    Moves a workflow from running-closed to force-complete
    """
    if isRequestMgr2Request(url, workflowname):
        params = {"RequestStatus" : "force-complete"}  
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, params)
    else:
        params = {"requestName" : workflowname,"status" : "force-complete"}
        data = requestManager1Put(url,"/reqmgr/reqMgr/request", params)
    return data

def closeOutWorkflow(url, workflowname, cascade=False):
    """
    Closes out a workflow by changing the state to closed-out
    This does not care about cascade workflows
    """
    if isRequestMgr2Request(url, workflowname): 
        params = {"RequestStatus" : "closed-out",
                  "cascade": cascade}
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, params)
        try:
            return None if (json.loads(data)['result'][0][workflowname] == 'OK') else "Error"
        except:
            return "Error"
    else:
        if cascade:
            params = {"requestName" : workflowname,"cascade" : cascade} 
            data = requestManager1Post(url,"/reqmgr/reqMgr/closeout", params)
        else:
            params = {"requestName" : workflowname,"status" : "closed-out"}
            data = requestManager1Put(url,"/reqmgr/reqMgr/request", params)
    return data

def closeOutWorkflowCascade(url, workflowname):
    return closeOutWorkflow(url, workflowname, True)

def announceWorkflow(url, workflowname, cascade=False):
    """
    Sets a workflow state to announced
    This does not care about cascade workflows
    """
    if isRequestMgr2Request(url, workflowname): 
        params = {"RequestStatus" : "announced",
                  "cascade": cascade}
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, params)
    else:
        if cascade:
            params = {"requestName" : workflowname,"cascade" : cascade}
            data = requestManager1Post(url,"/reqmgr/reqMgr/announce", params)
        else:
            params = {"requestName" : workflowname,"status" : "announced"}
            data = requestManager1Put(url,"/reqmgr/reqMgr/request", params)
    return data

def announceWorkflowCascade(url, workflowname):
    return announceWorkflow(url, workflowname,True)


def setWorkflowApproved(url, workflowname):
    """
    Sets a workflow state to assignment-approved
    """
    if isRequestMgr2Request(url, workflowname):
        params = {"RequestStatus" : "assignment-approved"}
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, params)
    else:
        params = {"requestName" : workflowname,"status" : "assignment-approved"}
        data = requestManager1Put(url,"/reqmgr/reqMgr/request", params)
    return data

def setWorkflowForceComplete(url, workflowname):
    return forceCompleteWorkflow(url, workflowname)

def setWorkflowRunning(url, workflowname):
    """
    Sets a workflow state to running
    """
    if isRequestMgr2Request(url, workflowname):
        params = {"RequestStatus" : "running"}
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, params)
    else:
        params = {"requestName" : workflowname,"status" : "running"}
        data = requestManager1Put(url,"/reqmgr/reqMgr/request", params)
    return data

def invalidateWorkflow(url, workflowname, current_status=None, cascade=False):
    if not current_status:
        print "not implemented yet to retrieve the status at that point"
    
    if current_status in ['assignment-approved','new','completed','closed-out','announced','failed']:
        return rejectWorkflow(url, workflowname, cascade)
    elif current_status in['normal-archived']:
        return rejectArchivedWorkflow(url, workflowname)
    elif current_status in ['aborted','rejected','aborted-archived','rejected-archived']:
        print workflowname,"already",current_status
        return True
    else:
        return abortWorkflow(url, workflowname, cascade)

def rejectArchivedWorkflow(url, workflowname): 
    """ 
    Sets a workflowin in rejected-argchived
    """
    if isRequestMgr2Request(url, workflowname):
        params = {"RequestStatus" : "rejected-archived"}
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, params)
    else:
        params = {"requestName" : workflowname,"status" : "rejected-archived"}
        data = requestManager1Put(url,"/reqmgr/reqMgr/request", params)
    return data

def rejectWorkflow(url, workflowname, cascade=False):
    """
    Sets a workflow state to rejected
    """
    if isRequestMgr2Request(url, workflowname):
        params = {"RequestStatus" : "rejected"}
        if cascade: params.update({"cascade": cascade})
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, params)
    else:
        params = {"requestName" : workflowname,"status" : "rejected"}
        data = requestManager1Put(url,"/reqmgr/reqMgr/request", params)
    return data

def abortWorkflow(url, workflowname, cascade=False):
    """
    Sets a workflow state to aborted
    """
    if isRequestMgr2Request(url, workflowname):
        params = {"RequestStatus" : "aborted"}
        if cascade: params.update({"cascade": cascade})
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, params)
    else:
        params = {"requestName" : workflowname,"status" : "aborted"}
        data = requestManager1Put(url,"/reqmgr/reqMgr/request", params)
    return data

def cloneWorkflow(url, workflowname, overrideArgs=None):
    """
    This clones a request
    """
    overrideArgs = overrideArgs or {}
    if isRequestMgr2Request(url, workflowname):
        data = requestManagerPost(url, "/reqmgr2/data/request/clone/%s" % workflowname, overrideArgs)
    else:
        print "cloneWorkflow Does not function in reqmgr1 in this interface. please migrate."
        data = None
    return data

def submitWorkflow(url, schema, reqmgr2=True): ## single switch flip
    """
    This submits a workflow into the ReqMgr, can be used for cloning
    and resubmitting workflows
    url: the instance ued, i.e. 'cmsweb.cern.ch'
    schema: A dictionary with the parameters needed to create
    the workflow
    """
    data = requestManagerPost(url, "/reqmgr2/data/request", schema)
    try:
        newwf = json.loads(data)['result'][0]['request']
        return newwf
    except:
        print "Error in making the request"
        print data
        return None
        

def reqmgr1_to_2_Splitting(params):
    reqmgr2Params = {}
    reqmgr2Params["taskName"] = params['splittingTask']
    reqmgr2Params["splitAlgo"] = params['splittingAlgo']
    del params['splittingTask']
    del params['splittingAlgo']
    del params['requestName']
    reqmgr2Params["splitParams"] = params
    return [reqmgr2Params]

def reqmgr2_to_1_Splitting(params):
    if len(params)>1: print "cannot set multiple splitting in reqmgr1 at once : truncating. please migrate."
    params = params[0]
    reqmgr1Params = {}
    reqmgr1Params["splittingTask"] = params["taskName"]
    reqmgr1Params["splittingAlgo"] = params["splitAlgo"]
    reqmgr1Params.update( params["splitParams"] )
    return reqmgr1Params

def setWorkflowSplitting(url, workflowname, schema):
    """
    This sets the workflow splitting into ReqMgr
    """
    def ifOldSchema(schema):
        return( type(schema)!=list)

    if isRequestMgr2Request(url, workflowname):
        if ifOldSchema(schema):
            print "old splitting format detected : translating. please migrate."
            schema = reqmgr1_to_2_Splitting(schema)
        data = requestManagerPost(url, "/reqmgr2/data/splitting/%s"%workflowname, schema)
    else:
        if not ifOldSchema(schema):
            print "new schema to reqmgr1 detected: translating. please drain."
            schema = reqmgr2_to_1_Splitting(schema)
        data = requestManager1Post(url,"/reqmgr/view/handleSplittingPage", schema)
    #print data
    return data

def reqmgr1_to_2_Assignment( params ):
    teams = []
    for key, value in params.iteritems():
        if isinstance(value, basestring):
            params[key] = value.strip()
        if key.startswith("Team"):
            if key[4:]: teams.append(key[4:])
        if key.startswith("checkbox"):
            requestName = key[8:]
    params["RequestName"] = requestName        
    params["Team"] = teams[0]
    priority = params.get(requestName + ':priority', '')
    if priority != '':
        params['RequestPriority'] = priority
    if params['action'] == 'Assign':
        params["RequestStatus"] = "assigned"
    elif params['action'] == 'Reject':
        params["RequestStatus"] = "rejected"

    params.pop("action", None)
    params.pop("Team%s" % teams[0], None)
    params.pop("checkbox%s" % requestName, None)
    return params

def reqmgr2_to_1_Assignment( params ):
    params['Team'+params["Teams"][0]] = "checked"
    params['checkbox'+params['RequestName']] = "checked"
    priority = params.get('RequestPriority','')
    if priority:
        params[params['RequestName']+':priority'] = priority
    params['action'] = 'Assign'
    return params




def setWorkflowAssignment(url, workflowname, schema):
    """
    This sets the workflow assignment into reqmgr
    """
    setWorkflowAssignment.errorMessage = ""
    def isOldSchema(schema):
        return any([k.startswith('checkbox') for k in schema.keys()])

    if isRequestMgr2Request(url, workflowname):
        if isOldSchema(schema):
            print "old schema detected : translating. please migrate."
            schema = reqmgr1_to_2_Assignment(schema)
        data = requestManagerPut(url,"/reqmgr2/data/request/%s"%workflowname, schema)
        try:
            ok = any(ro[workflowname]=='OK' for ro in json.loads( data )['result'] if workflowname in ro)
            return ok
        except:
            print data
            setWorkflowAssignment.errorMessage = data
            return False
    else:
        if not isOldSchema(schema):
            print "new schema to reqmgr1 detected : translating. please drain."
            schema = reqmgr2_to_1_Assignment(schema)
        data = requestManager1Post(url, "/reqmgr/assign/handleAssignmentPage", schema, nested=True)
        if 'Assigned' in data:
            return True
        else:
            print data
            setWorkflowAssignment.errorMessage = data
            return False
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
            return dbs3.getEventCountDataSet(inputDataSet) - dbs3.getEventCountDataSetBlockList(inputDataSet,blockBlacklist)
        if runWhitelist:
            return dbs3.getEventCountDataSetRunList(inputDataSet, runWhitelist)
        else:
            return dbs3.getEventCountDataSet(inputDataSet)
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





if __name__ == "__main__":
    usage = "usage: reqMgrClient -j <file.json>"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-j','--json',help='A json file with "createRequest" or just a request schema')
    parser.add_option('--testbed', help='Create in testbed', action='store_true', default=False, dest='testbed')
    parser.add_option('--test', help='Just print the schema',default=False,action='store_true')
    parser.add_option('--no_approve',help='In case you do not want the workflow in assignment-approved right away',default=False,action='store_true')
    (options, args) = parser.parse_args()
    
    prod_url = 'cmsweb.cern.ch'
    testbed_url = 'cmsweb-testbed.cern.ch'
    url = testbed_url if options.testbed else prod_url 

    schema = json.loads(open(options.json).read())
    if "createRequest" in schema: schema = schema["createRequest"]
    
    print json.dumps( schema ,indent=2)
    if not options.test:
        new_wf = submitWorkflow(url, schema)
        if not new_wf:
            print "Issue in making the request"
            sys.exit(1)
        print "Created:",new_wf
        if not options.no_approve:
            r = setWorkflowApproved(url, new_wf)
            print r
