#!/usr/bin/env python
"""
    Simple creating of acdc's
    Use: The workflow name and the initial task name.
    It will copy all the original workflow parameters unless specified
"""
import logging
import sys
from optparse import OptionParser
#from reqmgr import ReqMgrClient
logging.basicConfig(level=logging.WARNING)
import reqMgrClient

#class Config:
#    def __init__(self, info):
#        self.requestArgs = info
#        self.requestNames = []
#        self.cert = None
#        self.key = None
#        self.assignRequests = False
#        self.changeSplitting = False
#        self.assignRequest = False

prod_url = 'cmsweb.cern.ch'
testbed_url = 'cmsweb-testbed.cern.ch'

    
#configJson = {"createRequest":{}}
#config = Config(configJson)
##reqMgrClient = ReqMgrClient(url, config)
    

from Unified.recoveror import singleRecovery
from utils import workflowInfo
def makeACDC(url, workflow, task, memory=None):
    initial = workflowInfo(url, workflow)
    task = '/%s/%s'%( workflow, task)
    actions = []
    if memory:
        increment = initial.request['Memory'] - memory
        actions.append( ['mem-%d'% increment] )

    acdc = singleRecovery(url, task, initial.request, actions, do=True)
    if acdc:
        return acdc
    else:
        print "Issue while creating the acdc for",task
        return None

"""
    #original wf info
    wf = reqMgrClient.Workflow(workflow)
    
    schema = {
        "ACDCServer" : "https://cmsweb.cern.ch/couchdb",
        "ACDCDatabase" : "acdcserver",
        "RequestType" : "Resubmission",
        "Group" : "DATAOPS",
        "OriginalRequestName" : wf.name
        }

    #set up acdc stuff
    if "ACDC" in wf.info["RequestString"]:
        ## should do something about counting acdc
        schema["RequestString"] = wf.info["RequestString"]
    else:
        schema["RequestString"] = "ACDC_"+ wf.info["RequestString"]
    
    schema["PrepID"] = wf.info["PrepID"]
    schema["RequestPriority"] = wf.info["RequestPriority"]*10

    schema["InitialTaskPath"] = "/%s/%s"%(wf.name, task)
    schema["TimePerEvent"] = wf.info["TimePerEvent"]
    if memory:
        schema["Memory"] = memory
    else:
        schema["Memory"] = wf.info["Memory"]
    
    schema["SizePerEvent"] = wf.info["SizePerEvent"]

    
    r = reqMgrClient.createRequest(config)
    return r
"""

def main():

    #Create option parser
    usage = "usage: %prog [options] [WORKFLOW] TASK"
    parser = OptionParser(usage=usage)
    parser.add_option("-f","--file", dest="file", default=None,
                        help="Text file or a list of workflows")
    parser.add_option("-t","--task",
                      help="The task to be recovered")

    parser.add_option("-m","--memory", dest="memory", default=None, type=float,
                        help="Memory to override the original request memory")
    parser.add_option("--testbed", default=False, action="store_true")

    (options, args) = parser.parse_args()

    global url
    url = testbed_url if options.testbed else prod_url
    
    wfs = None
    try:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    except:
        wfs = options.file.split(',')

    if not wfs and not options.task:
        parser.error("Provide the Workflow Name and the Task Name")
        sys.exit(1)

    for wfname in wfs:
        r = makeACDC(url, wfname, options.task, options.memory)
        print "Created:"    
        print r

if __name__ == '__main__':
    main()

