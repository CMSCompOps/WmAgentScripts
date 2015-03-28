#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import reqMgrClient, dbs3Client
from optparse import OptionParser
from pprint import pprint
import math
"""
    Show the summary of a workflow
    
"""
url = 'cmsweb.cern.ch'

def human(n):
    """
    Format a number in a easy reading way
    """
    if n<1000:
        return "%s" % n
    elif n>=1000 and n<1000000:
        order = 1
    elif n>=1000000 and n<1000000000:
        order = 2
    else:
        order = 3

    norm = pow(10,3*order)
    value = float(n)/norm
    letter = {1:'k',2:'M',3:'G'}
    return ("%.1f%s" % (value,letter[order])).replace(".0", "")

def printTaskChain(workflow, options):
    tasks = workflow.cache["TaskChain"]
    print "Tasks:", tasks
    #pprint(workflow.cache)
    #print "-"*120
    #pprint(workflow.info)

    printException(workflow.cache, 'MCPileup')
    #printException(workflow.cache, 'EnableHarvesting')
    #printException(workflow.cache, 'EnableDQMHarvest')
    printException(workflow.cache, 'ProcScenario')
    printException(workflow.cache, 'DQMUploadUrl')
    printException(workflow.cache, 'DQMConfigCacheID')
    printException(workflow.cache, 'AcquisitionEra')

    
    tasksByName = {}    
    #calculate event and jobs traversing the task graph
    for i in range(1,int(tasks)+1):
        task = "Task"+str(i)
        tcache = workflow.cache[task]
        tinfo = workflow.info[task]
        #add to the list by name
        tasksByName[tcache["TaskName"]] = tcache
        #first task - no input
        if "InputTask" not in tinfo:
            if "TotalInputEvents" in workflow.cache: 
                inEv = int(workflow.cache["TotalInputEvents"])
            else:
                inEv = int(tcache["RequestNumEvents"])
            filEff = float(tcache["FilterEfficiency"])
            outEv = inEv
            tEv = tcache["TimePerEvent"]
            jobs = math.ceil(inEv*tEv/filEff/3600.0/8.0)
            tcache["TaskInputEvents"] = inEv
            tcache["TaskJobs"] = int(jobs)
            tcache["TaskOutputEvents"] = outEv
        #any other task that depends on the output of another
        else:
            #get predecessor
            pred = tasksByName[tinfo["InputTask"]]
            inEv = int(pred["TaskOutputEvents"])
            filEff = float(tcache["FilterEfficiency"])
            outEv = inEv*filEff
            tEv = tcache["TimePerEvent"]
            jobs = math.ceil(inEv*tEv/3600.0/8.0)
            tcache["TaskInputEvents"] = inEv
            tcache["TaskJobs"] = int(jobs)
            tcache["TaskOutputEvents"] = outEv
        
        print task,":", tcache["TaskName"]
        print "  Events:", human(tcache["TaskOutputEvents"])
        print "  Jobs  ~", human(tcache["TaskJobs"])
        printTaskException(tcache, 'MCPileup')
        printTaskException(tcache, 'InputDataset')
        printTaskException(tcache, 'PrimaryDataset')
        printTaskException(tcache, 'RequestNumEvents')
        printTaskException(tcache, 'GlobalTag')
        printTaskException(tcache, 'AcquisitionEra')
        printTaskException(tcache, 'ProcessingString')
        #printTaskException(tcache, 'RunWhitelist')
        printTaskException(tcache, 'SplittingArguments')
        printTaskException(tcache, 'InputTask')
        print ""

def printException(request, keyDic):
    try:
        result = str(request[keyDic])
        print ""+keyDic+": "+result
    except KeyError:
        pass
        #print ""

def printTaskException(taskdic, keyDic):
    try:
        result = str(taskdic[keyDic])
        print "  %s: %r" % (keyDic, result) 
    except KeyError:
        pass
        #print ""
       

def main():
    usage = "usage: %prog [options] workflow"
    parser = OptionParser(usage=usage)
    parser.add_option("-v","--verbose",action="store_true", dest="verbose", default=False,
                        help="Show detailed info")
    parser.add_option("-f","--file",action="store_true", dest="fileName", default=None,
                        help="Text file with several workflows")


    (options, args) = parser.parse_args()
    if len(args) != 1 and options.fileName is None:
        parser.error("Provide the workflow name or a file")
        sys.exit(1)
    if options.fileName is None:
        workflows = [args[0]]
    else:
        workflows = [l.strip() for l in open(options.fileName) if l.strip()]

    for wf in workflows:
        print wf
        workflow = reqMgrClient.Workflow(wf, url)        
        #by type
        print "Type", workflow.type
        if workflow.type == 'TaskChain':
            printTaskChain(workflow, options)
        else:
            print "There is nothing interesting about this workflow!"

if __name__ == "__main__":
    main()

