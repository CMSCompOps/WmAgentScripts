#!/usr/bin/env python
from collections import deque
from assignSession import *
from utils import closeoutInfo, reportInfo, reqmgr_url, componentInfo, moduleLock, workflowInfo
from collections import defaultdict
from wtcClient import wtcClient
from JIRAClient import JIRAClient
import optparse
import json

def sum_per_code( task_dict ):
    s = defaultdict(int)
    for code,codes in task_dict.items():
        for site,count in codes.items():
            s[code] += count
    return dict(s)

def main_errors( task_dict ):
    per_code = sorted(sum_per_code(task_dict).items(), key = lambda o:o[1])
    return per_code

def dominant_error(task_dict, fraction = 0.5 ):
    per_code = main_errors( task_dict )
    s = sum([p[1] for p in per_code])
    return (float(per_code[-1][1]) / float(s) > fraction, per_code[-1][0])
        
def majority_of_139_nanoaod(wfi, record, report):
    ## check on the main error, and bypass the request
    return []

def majority_of_X( wfi, report, code):
    for tname,tinfo in report.get('tasks',{}).items():
        terror = tinfo.get('errors',{})
        if terror:
            dominant,main_error = dominant_error( terror, fraction = 0.7 )
            if dominant and main_error == code:
                print "the main errors codes are", per_code
                print per_code
    return []
    
def majority_of_71104(wfi, record, report):
    all_bad = True
    for tname,tinfo in report.get('tasks',{}).items():
        terror = tinfo.get('errors',{})
        if terror:
            dominant,main_error = dominant_error( terror, fraction = 0.7 )
            if dominant and main_error == '71104':
                print "the main errors code is 71104"
            else:
                all_bad = False
    
    if all_bad:
        print "go on with simple acdc?"
    
    return []

def rulor(spec=None, options=None):
    
    mlock = moduleLock()
    if mlock(): return 

    up = componentInfo(soft=['mcm','wtc'])
    if not up.check(): return
    
    if spec:
        wfs = session.query(Workflow).filter(Workflow.status.contains('manual')).filter(Workflow.name.contains(spec)).all()
    else:
        wfs = session.query(Workflow).filter(Workflow.status.contains('manual')).all()
    
    COI = closeoutInfo()
    RI = reportInfo()
    WC = wtcClient()
    JC = JIRAClient()

    ## a list of function with a given trace ( wfi, record, report) => (action dict list)
    rules = [
        majority_of_139_nanoaod,
        majority_of_71104,
    ]

    for wfo in wfs:
        wfi = workflowInfo( reqmgr_url, wfo.name )
        record = COI.get( wfo.name )
        report = RI.get( wfo.name )
        if not record: 
            print "no information to look at"
            continue
        print "close out information as in the assistance page"
        print json.dumps(record, indent=2)
        print "report information as in the unified report"
        print json.dumps(report, indent=2)

        
        ## parse the information and produce an action document
        ### a rule for on-going issue with memory in campaign ...
        acted = False
        for condition in rules:
            acts = condition( wfi, record, report)
            if acts:
                print "list of actions being taken for",wfo.name
                for a in acts:
                    print json.dumps(a, indent=2)
                if not options.test:
                    acted = True
                    WC.set_actions( acts )
                    wfo.status = wfo.status.replace('manual','acting')
                    session.commit()
                break
                
        if acted: continue    
        if "some conditions":
            action_doc =  { 'workflow' : wfo.name,
                            'name' : "a task name",
                            'parameters' : { 'action' : 'acdc',
                                             'memory' : 5000}
                        }
            acted = True
        if acted: continue
        if "majority of 139":
            
            pass
        if acted: continue

if __name__ == "__main__":
    url = reqmgr_url
    
    print "yup"
    parser = optparse.OptionParser()
    parser.add_option('--test',help="Dry run, only show what you want to do", default=False, action="store_true")
    (options,args) = parser.parse_args()
    spec= args[0] if len(args)!=0 else None

    rulor(spec, options)
