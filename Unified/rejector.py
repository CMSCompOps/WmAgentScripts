#!/usr/bin/env python
from assignSession import *
import reqMgrClient
from utils import workflowInfo, setDatasetStatus, invalidate
from utils import componentInfo, reqmgr_url, getWorkflowById
from utils import componentInfo, getWorkflowById, sendLog, batchInfo
import optparse
import json
import re
import os
import time
import getpass
username = getpass.getuser()

def rejector(url, specific, options=None):
    
    if options.test:
        print "Test mode - no changes propagate to the production system"

    if not componentInfo(soft=['wtc','jira']).check(): return

    if specific and specific.startswith('/'):
        ## this is for a dataset
        print setDatasetStatus(specific, 'INVALID')
        return

    if options.filelist:
        wfs = []
        for line in filter(None, open(options.filelist).read().split('\n')):
            print line
            wfs.extend( session.query(Workflow).filter(Workflow.name.contains(line)).all())
    elif specific:
        wfs = session.query(Workflow).filter(Workflow.name.contains(specific)).all()
        if not wfs:
            batches = batchInfo().content()
            for bname in batches:
                if specific == bname:
                    for pid in batches[bname]:
                        b_wfs = getWorkflowById(url, pid)
                        for wf in b_wfs:
                            wfs.append( session.query(Workflow).filter(Workflow.name == wf).first())
                    break
    else:
        wfs = session.query(Workflow).filter(Workflow.status == 'assistance-clone').all()
        #wfs.extend( session.query(Workflow).filter(Workflow.status == 'assistance-reject').all())
        ## be careful then on clone case by case
        options.clone = True
        print "not supposed to function yet"
        return 

    print len(wfs),"to reject"

    if len(wfs)>1:
        print "\n".join( [wfo.name for wfo in wfs] )
        answer = raw_input('Reject these')
        if not answer.lower() in ['y','yes']:
            return
        
    for wfo in wfs:
        #wfo = session.query(Workflow).filter(Workflow.name == specific).first()
        if not wfo:
            print "cannot reject",spec
            return
        wfi = workflowInfo(url, wfo.name)

        comment=""
        if options.comments: comment = ", reason: "+options.comments
        if options.test:
            if options.keep: 
                print 'invalidating the workflow by unified operator {}{}'.format(username, comment)
            else:
                print 'invalidating the workflow and outputs by unified operator {}{}'.format(username, comment)
            results = [True]
        else:
            if options.keep: 
                wfi.sendLog('rejector','invalidating the workflow by unified operator {}{}'.format(username, comment))
            else:
                wfi.sendLog('rejector','invalidating the workflow and outputs by unified operator {}{}'.format(username, comment))

            results = invalidate(url, wfi, only_resub=True, with_output= (not options.keep))

        if all(results):
            print wfo.name,"rejected"
            if options and options.clone:
                if not options.test:
                    wfo.status = 'trouble'
                    session.commit()                
                schema = wfi.getSchema()
                if options.test:
                    print "Original schema"
                    print json.dumps( schema, indent=2 )
                schema['Requestor'] = os.getenv('USER')
                schema['Group'] = 'DATAOPS'
                schema['OriginalRequestName'] = wfo.name
                if 'ProcessingVersion' in schema:
                    schema['ProcessingVersion'] = int(schema['ProcessingVersion'])+1 ## dubious str->int conversion
                else:
                    schema['ProcessingVersion']=2
                for k in schema.keys():
                    if k.startswith('Team'):
                        schema.pop(k)
                    if k.startswith('checkbox'):
                        schema.pop(k)

                ## a few tampering of the original request
                if options.Memory:
                    if schema['RequestType'] == 'TaskChain':
                        it=1
                        while True:
                            t = 'Task%d'%it
                            it+=1
                            if t in schema:
                                schema[t]['Memory'] = options.Memory
                            else:
                                break
                    else:
                        schema['Memory'] = options.Memory

                if options.short_task and schema['RequestType'] == 'TaskChain':
                    translate = {}
                    it = 1
                    while True:
                        tt = 'Task%d'% it
                        if tt in schema:
                            tname = schema[tt]['TaskName']
                            ntname = 'T%d'%it
                            translate[tname] = ntname
                            it+=1
                            schema[tt]['TaskName'] = ntname
                            if 'InputTask' in schema[tt]:
                                itname = schema[tt]['InputTask']
                                schema[tt]['InputTask'] = translate[itname]
                        else:
                            break
                    for k in schema.get('ProcessingString',{}).keys():
                        schema['ProcessingString'][translate[k]] = schema['ProcessingString'].pop(k)
                    for k in schema.get('AcquisitionEra',{}).keys():
                        schema['AcquisitionEra'][translate[k]] = schema['AcquisitionEra'].pop(k)

                        
                if options.Multicore:
                    ## to do : set it properly in taskchains
                    if schema['RequestType'] == 'TaskChain':
                        tasks,set_to = options.Multicore.split(':') if ':' in options.Multicore else ("",options.Multicore)
                        set_to = int(set_to)
                        tasks = tasks.split(',') if tasks else ['Task1']
                        it = 1 
                        while True:
                            tt = 'Task%d'% it
                            it+=1
                            if tt in schema:
                                tname = schema[tt]['TaskName']
                                if tname in tasks or tt in tasks:
                                    mem = schema[tt]['Memory']
                                    mcore = schema[tt].get('Multicore',1)
                                    factor = (set_to / float(mcore))
                                    fraction_constant = 0.4
                                    mem_per_core_c = int((1-fraction_constant) * mem / float(mcore))
                                    print "mem per core", mem_per_core_c
                                    print "base mem", mem
                                    ## adjusting the parameter in the clone
                                    schema[tt]['Memory'] += (set_to-mcore)*mem_per_core_c
                                    schema[tt]['Multicore'] = set_to
                                    schema[tt]['TimePerEvent'] /= factor
                            else:
                                break
                    else:
                        schema['Multicore'] = options.Multicore
                if options.deterministic:
                    if schema['RequestType'] == 'TaskChain':
                        schema['Task1']['DeterministicPileup']  = True
                if options.EventsPerJob:
                    if schema['RequestType'] == 'TaskChain':
                        schema['Task1']['EventsPerJob'] = options.EventsPerJob
                    else:
                        schema['EventsPerJob'] = options.EventsPerJob
                if options.EventAwareLumiBased:
                    schema['SplittingAlgo'] = 'EventAwareLumiBased'
                if options.TimePerEvent:
                    schema['TimePerEvent'] = options.TimePerEvent

                if options.ProcessingString:
                    schema['ProcessingString'] = options.ProcessingString
                if options.AcquisitionEra:
                    schema['AcquisitionEra'] = options.AcquisitionEra
                if options.runs:
                    schema['RunWhitelist'] = map(int,options.runs.split(','))
                if options.PrepID:
                    schema['PrepID'] =options.PrepID

                if schema['RequestType'] == 'TaskChain' and options.no_output:
                    ntask = schema['TaskChain']
                    for it in range(1,ntask-1):
                        schema['Task%d'%it]['KeepOutput'] = False
                    schema['TaskChain'] = ntask-1
                    schema.pop('Task%d'%ntask)

                if options.priority:
                    schema['RequestPriority'] = options.priority

                ## update to the current priority
                schema['RequestPriority'] = wfi.request['RequestPriority']

                ## drop shit on the way to reqmgr2
                schema = reqMgrClient.purgeClonedSchema( schema )

                print "submitting"
                if (options.to_stepchain and (schema['RequestType']=='TaskChain')):
                    ## transform the schema into StepChain schema
                    print "Transforming a TaskChain into a StepChain"
                    mcore = 0
                    mem = 0
                    schema['RequestType'] = 'StepChain'
                    schema['StepChain'] = schema.pop('TaskChain')
                    schema['SizePerEvent'] = 0
                    schema['TimePerEvent'] = 0
                    step=1
                    s_n = {}
                    while True:
                        if 'Task%d'%step in schema:
                            sname = 'Step%d'%step
                            schema[sname] = schema.pop('Task%d'%step)
                            if 'Multicore' in schema[sname] and schema[sname]['Multicore']==1:
                                # enforce single-core mode assuming that all Tasks with
                                # Multicore=1 are not thread-safe
                                tmcore = schema[sname]['Multicore']
                            else:
                                # remove explicit assignment of the number of cores
                                tmcore = schema[sname].pop('Multicore')
                            tmem = schema[sname].pop('Memory')
                            if mcore and tmcore != mcore:
                                if options.test:
                                    print 'the conversion of %s to stepchain encoutered different value of Multicore %d != %d' % (wfo.name, tmcore, mcore)
                                else:
                                    wfi.sendLog('rejector','the conversion to stepchain encoutered different value of Multicore %d != %d'%( tmcore, mcore))
                                    sendLog('rejector','the conversion of %s to stepchain encoutered different value of Multicore %d != %d'%( wfo.name, tmcore, mcore))
                            mcore = max(mcore, tmcore)
                            mem = max(mem, tmem)
                            schema[sname]['StepName'] = schema[sname].pop('TaskName')
                            s_n[ schema[sname]['StepName'] ] = sname
                            if 'InputTask' in schema[sname]:
                                schema[sname]['InputStep'] = schema[sname].pop('InputTask')
                            eff = 1.
                            up_s = sname
                            while True:
                                ## climb up a step. supposedely already all converted
                                up_s = s_n.get(schema[up_s].get('InputStep',None),None)
                                if up_s:
                                    ## multiply with the efficiency
                                    eff *= schema[up_s].get('FilterEfficiency',1.)
                                else:
                                    ## or stop there
                                    break

                            if not 'KeepOutput' in schema[sname]:
                                ## this is a weird translation capability. Absence of keepoutput in step means : keep the output. while in TaskChain absence means : drop
                                schema[sname]['KeepOutput'] = False
                            schema['TimePerEvent'] += eff*schema[sname].pop('TimePerEvent')
                            schema['SizePerEvent'] += eff*schema[sname].pop('SizePerEvent')
                            step+=1
                        else:
                            break
                    schema['Multicore'] = mcore
                    schema['Memory'] = mem
                print "New request schema"
                print json.dumps( schema, indent=2 )
                if not options.test:
                    newWorkflow = reqMgrClient.submitWorkflow(url, schema)
                    if not newWorkflow:
                        msg = "Error in cloning {}".format(wfo.name)
                        print(msg)
                        wfi.sendLog('rejector',msg)

                        # Get the error message
                        time.sleep(5)
                        data = reqMgrClient.requestManagerPost(url, "/reqmgr2/data/request", schema)
                        wfi.sendLog('rejector',data)

                        print json.dumps( schema, indent=2 )
                        return 
                    print newWorkflow

                    data = reqMgrClient.setWorkflowApproved(url, newWorkflow)
                    print data
                    wfi.sendLog('rejector','Cloned into %s by unified operator %s'%( newWorkflow, comment ))
                    #wfi.notifyRequestor('Cloned into %s by unified operator %s'%( newWorkflow, comment ),do_batch=False)
            else:
                if options.test:
                    print 'Rejected by unified operator %s'%( comment )
                else:
                    wfo.status = 'trouble' if options.set_trouble else 'forget' 
                    wfi.notifyRequestor('Rejected by unified operator %s'%( comment ),do_batch=False)
                    session.commit()

        else:
            msg = "Error in rejecting {}: {}".format(wfo.name, results)
            print msg
            if not options.test:
                wfi.sendLog('rejector', msg)

if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
    parser.add_option('-t', '--test', help="test mode - no changes are made", default=False, action="store_true")
    parser.add_option('-m','--manual', help='Manual assignment, bypassing lock check',action='store_true',dest='manual',default=False)
    parser.add_option('-c','--clone',help="clone the workflow",default=False,action="store_true")
    parser.add_option('--comments', help="Give a comment to the clone",default="")
    parser.add_option('-k','--keep',help="keep the outpuy in current status", default=False,action="store_true")
    parser.add_option('--set_trouble',help="When rejecting but the status should be trouble instead of forget", default=False,action="store_true")
    ## options for cloning
    parser.add_option('--Memory',help="memory parameter of the clone", default=0, type=int)
    parser.add_option('--Multicore',help="Set the number of core in the clone", default=None)
    parser.add_option('--ProcessingString',help="change the proc string", default=None)
    parser.add_option('--AcquisitionEra',help="change the acq era", default=None)
    parser.add_option('--PrepID',help='change the prepid',default=None)
    parser.add_option('--EventsPerJob', help="set the events/job on the clone", default=0, type=int)
    parser.add_option('--priority', help="Change the priority", default=0, type=int)
    parser.add_option('--EventAwareLumiBased', help="set the splitting algo of the clone", default=False, action='store_true')
    parser.add_option('--TimePerEvent', help="set the time/event on the clone", default=0, type=float)
    parser.add_option('--filelist',help='a file with a list of workflows',default=None)
    parser.add_option('--no_output',help='keep only the output of the last task of TaskChain',default=False,action='store_true')
    parser.add_option('--deterministic',help='set the splitting to deterministic in the clone',default=False,action='store_true')
    parser.add_option('--runs',help='set the run whitelist in the clone',default=None)
    parser.add_option('--short_task', help='Reduce the TaskName to a minimal value', default=False,action='store_true')
    parser.add_option('-s','--to_stepchain',help='transform a TaskChain into StepChain',default=False,action='store_true')
    (options,args) = parser.parse_args()

    spec=None
    if len(args):
        spec = args[0]
    

    rejector(url, spec, options)
