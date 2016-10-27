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

prod_url = 'cmsweb.cern.ch'
testbed_url = 'cmsweb-testbed.cern.ch'

from Unified.recoveror import singleRecovery
from utils import workflowInfo
def makeACDC(url, wfi, task, memory=None):
    initial = wfi
    #task = '/%s/%s'%( workflow, task)
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
    parser.add_option("-a","--all",
                      help="Make acdc for all tasks to be recovered",default=False, action='store_true')
    parser.add_option("-m","--memory", dest="memory", default=None, type=float,
                        help="Memory to override the original request memory")
    parser.add_option("--testbed", default=False, action="store_true")

    (options, args) = parser.parse_args()

    global url
    url = testbed_url if options.testbed else prod_url
    
    wfs = None
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    elif options.workflow:
        wfs = options.workflow.split(',')
    else:
        parser.error("Either provide a -f filelist or a -w workflow")
        sys.exit(1)

    if (not wfs) or (not options.task and not options.all):
        parser.error("Provide the -w Workflow Name and the -t Task Name or --all")
        sys.exit(1)

    for wfname in wfs:
        wfi = workflowInfo(url, wfname)
        if options.task == 'all' or options.all:
            where,how_much,how_much_where = wfi.getRecoveryInfo()
            tasks = sorted(how_much.keys())
        else:
            tasks = [('/%s/%s'%(wfname,task)).replace('//','/') for task in options.task.split(',')]

        created = {}
        print "Workflow:",wfname
        print "Tasks:",tasks
        for task in tasks:
            r = makeACDC(url, wfi, task, options.memory) 
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

