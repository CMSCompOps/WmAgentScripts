#!/usr/bin/env python
"""
    Create one acdc for every task that has an ACDC document
    Use: wfname
    It will copy all the original workflow parameters unless specified
"""
import sys
from optparse import OptionParser
import reqMgrClient as rqMgr
from makeACDC import makeACDC

class Config:
    def __init__(self, info):
        self.requestArgs = info
        self.requestNames = []
        self.cert = None
        self.key = None
        self.assignRequests = False
        self.changeSplitting = False
        self.assignRequest = False


def getAcdcTasks(url, workflow):
    #https://cmsweb.cern.ch/couchdb/acdcserver/_design/ACDC/_view/byCollectionName?key=%22fabozzi_Run2015C_25ns-ZeroBias3-05Oct2015_7414_151015_182109_1001%22&include_docs=true&reduce=false
    # ACDC documents
    acdcDocs = rqMgr.requestManagerGet(url,'/couchdb/acdcserver/_design/ACDC/_view/byCollectionName?key="%s"&include_docs=true&reduce=false'%workflow)
    
    # get the tasks
    tasks = set()
    if 'rows' in acdcDocs:
        for doc in acdcDocs['rows'] :
            if 'doc' in doc and 'fileset_name' in doc['doc']:
                task = doc['doc']['fileset_name']
                task = '/'.join(task.split('/')[2:])
                tasks.add(task)
    return tasks

def makeAllACDCs(url, workflow):    
    tasks = getAcdcTasks(url, workflow)
    acdcs = []
    for task in tasks:
        print task
        acdc = makeACDC(url, workflow, task)
        acdcs.append(acdc)
    return acdcs

def main():

    url = 'cmsweb.cern.ch'
    testbed_url = 'https://cmsweb-testbed.cern.ch'
    #url = 'https://alan-cloud1.cern.ch'

    #Create option parser
    usage = "usage: %prog [options] [WORKFLOW]"
    parser = OptionParser(usage=usage)
    parser.add_option("-f","--file", dest="file", default=None,
                        help="Text file of a list of workflows")
    parser.add_option("-m","--memory", dest="memory", default=None,
                        help="Memory to override the original request memory")

    (options, args) = parser.parse_args()
    
    wfs = None
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    elif args:
        wfs = args
    else:
        parser.error("Provide the Workflow Name")
        sys.exit(1)
    
    for wf in wfs:
        acdcs = makeAllACDCs(url, wf)
        print "created acdcs"
        print '\n'.join(acdcs)
        
if __name__ == '__main__':
    main()

