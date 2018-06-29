#!/usr/bin/env python
import reqMgrClient
from optparse import OptionParser
import sys
from pprint import pprint
class Config:
    def __init__(self, info):
        self.requestArgs = info
        self.requestNames = []
        self.requestName = None
        self.cert = None
        self.key = None
        self.assignRequests = False
        self.changeSplitting = True
        self.assignRequest = False

def getEventBasedParams(split):
    params = {
                  "splittingAlgo": "EventBased",
                  "events_per_job": str(split),
                  "events_per_lumi": '200',
                  "timeout": ""
                  }
    return params

def getEventAwareLumiParams(split):
    params = {
                'SplittingAlgo': 'EventAwareLumiBased',
                'avg_events_per_job': split,
                'halt_job_on_file_boundaries_event_aware': 'True',
                'include_parents': 'False',
                'max_events_per_lumi': 20000,
                'splittingAlgo': 'EventAwareLumiBased',
             }

    return params

def getLumiBasedParams(split):
    params = {
              "splittingAlgo": "LumiBased",
              "lumis_per_job": str(split),
              "include_parents": "False",
              "files_per_job": "",
              'halt_job_on_file_boundaries': 'True'
              }
    return params

def getMergeParams(split):
    params = {
            "splittingAlgo": "ParentlessMergeBySize",
            "min_merge_size" : 2147483648,
            "max_merge_size" : 4294967296,
             "max_merge_events" : split,
              "max_wait_time" : 86400
    }
    return params

def changeSplittingWorkflow(url, workflow, split, task, split_type='EventAwareLumi'):
    if split_type == 'EventAwareLumi':
        params = getEventAwareLumiParams(split)
    elif split_type == 'Event':
        params = getEventBasedParams(split)
    elif split_type == 'Lumi':
        params = getLumiBasedParams(split)
    elif split_type == 'Merge':
        params = getMergeParams(split)
    params['requestName'] = workflow
    params['splittingTask'] = '/%s/%s'%(workflow, task)
    
    #pprint(params)
    data = reqMgrClient.setWorkflowSplitting(url, params)
    #TODO validate data
    print data
    
url = 'cmsweb.cern.ch'
def main():
    parser = OptionParser("python %prog [-t TYPE| -e | -l | -a] WORKFLOW TASKPATH VALUE")
    parser.add_option("-t","--type", dest="type", default=None,
                        help="Type of splitting (event, lumi, eventaware or merge), or use the other options")
    parser.add_option("-e",action="store_true", dest="event", default=False,
                        help="Use EventBased splitting")
    parser.add_option("-l", "--lumi",action="store_true", dest="lumi", default=False,
                        help="Use")
    parser.add_option("-a", "--eventaware",action="store_true", dest="event_aware", default=False,
                        help="Use EventAwareLumiBased")
    parser.add_option("-m", "--merge",action="store_true", dest="merge", default=False,
                        help="Splitting for Merge tasks")
    (options, args) = parser.parse_args()
    
    if len(args) != 3:
        parser.error("Provide workflow name, task path and value")
        sys.exit(1)
    
    workflow = args[0]
    task = args[1]
    split = args[2]
    
    if options.type:
        if options.type.lower() == 'event':
            split_type = 'Event'
        elif options.type.lower() == 'lumi':
            split_type = 'Lumi'
        elif options.type.lower() == 'eventaware':
            split_type = 'EventAwareLumi'
        elif options.type.lower() == 'merge':
            split_type = 'Merge'
            
    elif options.event:
        split_type = 'Event'
    elif options.lumi:
        split_type = 'Lumi'
    elif options.event_aware:
        split_type = 'EventAwareLumi'
    elif options.merge:
        split_type = 'Merge'
    else:
        split_type = 'EventAwareLumi'
    
    changeSplittingWorkflow(url, workflow, split, task, split_type)
    
    sys.exit(0)

if __name__ == "__main__":
    main()
