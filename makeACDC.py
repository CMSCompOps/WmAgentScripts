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
    usage = "usage: %prog [options] [WORKFLOW] TASK"
    parser = OptionParser(usage=usage)
    parser.add_option("-f","--file", dest="file", default=None,
                        help="Text file or a list of workflows")
    parser.add_option("-t","--task",
                      help="The task to be recovered")
    parser.add_option("-a","--all",
                      help="Make acdc for all tasks to be recovered",default=False, action='store_true')
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
        wfi = workflowInfo(url, wfname)
        if options.task == 'all' or options.all:
            where,how_much,how_much_where = wfi.getRecoveryInfo()
            tasks = sorted(how_much.keys())
            created = []
            print tasks
            for task in tasks:
                r = makeACDC(url, wfi, task, options.memory) 
                if not r: 
                    print "Error in creating ACDC for",task,"on",wfname
                    break
                created.append( r )
            if len(created)!=len(tasks):
                print "Error in creating all required ACDCs"
                sys.exit(1)
            print "Created:"
            print '\n'.join(sorted(created))
        else:
            r = makeACDC(url, wfi, options.task, options.memory)
            if not r:
                print "Error in creating ACDC"
                sys.exit(1)
            print "Created:" 
            print r

if __name__ == '__main__':
    main()

