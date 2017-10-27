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
from utils import workflowInfo
from collections import defaultdict 

prod_url = 'cmsweb.cern.ch'
testbed_url = 'cmsweb-testbed.cern.ch'

from Unified.recoveror import singleRecovery
from utils import workflowInfo
def makeACDC(**args):
    url = args.get('url')
    wfi = args.get('wfi')
    task = args.get('task')
    initial = wfi
    actions = []
    memory = args.get('memory',None)
    if memory:
        #increment = initial.request['Memory'] - memory
        #actions.append( 'mem-%d'% increment )
        actions.append( 'mem-%s'% memory )
    mcore = args.get('mcore',None)
    if mcore:
        actions.append( 'core-%s'% mcore)
        
    acdc = singleRecovery(url, task, initial.request, actions, do=True)
    if acdc:
        return acdc
    else:
        print "Issue while creating the acdc for",task
        return None

def main():

    #Create option parser
    usage = "usage: %prog (-w workflow|-f filelist) (-t TASK|--all) [--tesbed]"
    parser = OptionParser(usage=usage)
    parser.add_option("-f","--file", dest="file", default=None,
                        help="Text file with a list of workflows")
    parser.add_option("-w","--workflow", default=None,
                      help="Coma separated list of wf to handle")
    parser.add_option("-t","--task", default=None,
                      help="Coma separated task to be recovered")
    parser.add_option("-p","--path", default=None,
                      help="Coma separated list of paths to recover")
    parser.add_option("-a","--all",
                      help="Make acdc for all tasks to be recovered",default=False, action='store_true')
    parser.add_option("-m","--memory", dest="memory", default=None, type=int,
                        help="Memory to override the original request memory")
    parser.add_option("-c","--mcore", dest="mcore", default=None,
                      help="Multicore to override the original request multicore")
    parser.add_option("--testbed", default=False, action="store_true")

    (options, args) = parser.parse_args()

    global url
    url = testbed_url if options.testbed else prod_url

    if options.all : options.task = 'all'

    if not options.task:
        parser.error("Provide the -t Task Name or --all")
        sys.exit(1)

    if not ((options.workflow) or (options.path) or (options.file)):
        parser.error("Provide the -w Workflow Name or the -p path or the -f workflow filelist")
        sys.exit(1)
    
    wfs = None
    wf_and_task = defaultdict(set)
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    elif options.workflow:
        wfs = options.workflow.split(',')
    elif options.path:
        ## self contained
        paths = options.path.split(',')
        for p in paths:
            _,wf,t = p.split('/',2)
            wf_and_task[wf].add('/%s/%s'%(wf,t))
    else:
        parser.error("Either provide a -f filelist or a -w workflow or -p path")
        sys.exit(1)

    if not wf_and_task:
        if options.task == 'all':
            for wfname in wfs: 
                wf_and_task[wfname] = None
        else:
            for wfname in wfs: 
                wf_and_task[wfname].update( [('/%s/%s'%(wfname,task)).replace('//','/') for task in options.task.split(',')] )

    if not wf_and_task:
        parser.error("Provide the -w Workflow Name and the -t Task Name or --all")
        sys.exit(1)        

    
    for wfname,tasks in wf_and_task.items():
        wfi = workflowInfo(url, wfname)
        if tasks == None:
            where,how_much,how_much_where = wfi.getRecoveryInfo()
            tasks = sorted(how_much.keys())
        else:
            tasks = sorted(tasks)

        created = {}
        print "Workflow:",wfname
        print "Tasks:",tasks
        for task in tasks:
            r = makeACDC(url=url, wfi=wfi, task=task,
                         memory = options.memory,
                         mcore = options.mcore) 
            if not r: 
                print "Error in creating ACDC for",task,"on",wfname
                break
            created[task] = r
        if len(created)!=len(tasks):
            print "Error in creating all required ACDCs"
            sys.exit(1)
        print "Created:"
        for task in created:
            print created[task],"for",task

if __name__ == '__main__':
    main()

