#!/usr/bin/env python
from assignSession import *
import sys
import reqMgrClient
from utils import workflowInfo, setDatasetStatus
from utils import componentInfo, reqmgr_url, getWorkflowById
from utils import componentInfo, getWorkflowById, sendLog
import optparse
import json
import re
import os

def rejector(url, specific, options=None):

    #use_mcm = True
    #up = componentInfo(mcm=use_mcm, soft=['mcm'])
    up = componentInfo()
    if not up.check(): return
    #use_mcm = up.status['mcm']
    #mcm = McMClient(dev=False) if use_mcm else None

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
            batches = json.loads(open('batches.json').read())
            for bname in batches:
                if specific == bname:
                    for wf in batches[bname]:
                        wfs.append( session.query(Workflow).filter(Workflow.name == wf).first())
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
        results=[]
        wfi = workflowInfo(url, wfo.name)

        datasets = set(wfi.request['OutputDatasets'])
        reqMgrClient.invalidateWorkflow(url, wfo.name, current_status=wfi.request['RequestStatus'])

        comment=""
        if options.comments: comment = ", reason: "+options.comments
        wfi.sendLog('rejector','invalidating the workflow by unified operator%s'%comment)
        ## need to find the whole familly and reject the whole gang
        familly = getWorkflowById( url, wfi.request['PrepID'] , details=True)
        for fwl in familly:
            if fwl['RequestDate'] < wfi.request['RequestDate']: continue
            if fwl['RequestType']!='Resubmission': continue
            ## does not work on second order acd
            if 'OriginalRequestName' in fwl and fwl['OriginalRequestName'] != wfi.request['RequestName']: continue
            print "rejecting",fwl['RequestName']
            reqMgrClient.invalidateWorkflow(url, fwl['RequestName'], current_status=fwl['RequestStatus'], cascade=False)
            datasets.update( fwl['OutputDatasets'] )

        for dataset in datasets:
            if options.keep:
                print "keeping",dataset,"in its current status"
            else:
                results.append( setDatasetStatus(dataset, 'INVALID') )
                pass


        if all(map(lambda result : result in ['None',None,True],results)):
            print wfo.name,"and",datasets,"are rejected"
            if options and options.clone:
                wfo.status = 'trouble'
                session.commit()                
                schema = wfi.getSchema()
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

                ## update to the current priority
                schema['RequestPriority'] = wfi.request['RequestPriority']

                ## drop shit on the way to reqmgr2
                paramBlacklist = ['BlockCloseMaxEvents', 'BlockCloseMaxFiles', 'BlockCloseMaxSize', 'BlockCloseMaxWaitTime',
                                  'CouchWorkloadDBName', 'CustodialGroup', 'CustodialSubType', 'Dashboard',
                                  'GracePeriod', 'HardTimeout', 'InitialPriority', 'inputMode', 'MaxMergeEvents', 'MaxMergeSize',
                                  'MaxRSS', 'MaxVSize', 'MinMergeSize', 'NonCustodialGroup', 'NonCustodialSubType',
                                  'OutputDatasets', 'ReqMgr2Only', 'RequestDate' 'RequestorDN', 'RequestName', 'RequestStatus',
                                  'RequestTransition', 'RequestWorkflow', 'SiteWhitelist', 'SoftTimeout', 'SoftwareVersions',
                                  'SubscriptionPriority', 'Team', 'timeStamp', 'TrustSitelists', 'TrustPUSitelists',
                                  'TotalEstimatedJobs', 'TotalInputEvents', 'TotalInputLumis', 'TotalInputFiles',
                                  ## and the new parameter validation scheme
                                  'DN', 'AutoApproveSubscriptionSites', 'NonCustodialSites', 'CustodialSites', 
                                  'OriginalRequestName', 'IgnoredOutputModules', 'OutputModulesLFNBases', 'SiteBlacklist', 'AllowOpportunistic', '_id',
                                  'min_merge_size', 'events_per_lumi', 'max_merge_size', 'max_events_per_lumi', 'max_merge_events', 'max_wait_time', 'events_per_job']
                for p in paramBlacklist:
                    if p in schema:
                        schema.pop( p )
                        #pass
                print "submitting"
                if (options.to_stepchain and (schema['RequestType']=='TaskChain')):
                    ## transform the schema into StepChain schema
                    print "Transforming a TaskChain into a StepChain"
                    schema['RequestType'] = 'StepChain'
                    schema['StepChain'] = schema.pop('TaskChain')
                    schema['SizePerEvent'] = 0
                    schema['TimePerEvent'] = 0
                    step=1
                    while True:
                        if 'Task%d'%step in schema:
                            schema['Step%d'%step] = schema.pop('Task%d'%step)
                            schema['TimePerEvent'] += schema['Step%d'%step].pop('TimePerEvent')
                            #schema['SizePerEvent'] = max(schema['SizePerEvent'], schema['Step%d'%step].pop('SizePerEvent'))
                            schema['SizePerEvent'] += schema['Step%d'%step].pop('SizePerEvent')
                            schema['Step%d'%step]['StepName'] = schema['Step%d'%step].pop('TaskName')
                            if 'InputTask' in schema['Step%d'%step]:
                                schema['Step%d'%step]['InputStep'] = schema['Step%d'%step].pop('InputTask')
                            if not 'KeepOutput' in schema['Step%d'%step]:
                                ## this is a weird translation capability. Absence of keepoutput in step means : keep the output. while in TaskChain absence means : drop
                                schema['Step%d'%step]['KeepOutput'] = False
                            step+=1
                        else:
                            break


                print json.dumps( schema, indent=2 )
                newWorkflow = reqMgrClient.submitWorkflow(url, schema)
                if not newWorkflow:
                    print "error in cloning",wfo.name
                    print json.dumps( schema, indent=2 )
                    return 
                print newWorkflow

                data = reqMgrClient.setWorkflowApproved(url, newWorkflow)
                print data
                wfi.sendLog('rejector','Cloned into %s by unified operator %s'%( newWorkflow, comment ))
                wfi.notifyRequestor('Cloned into %s by unified operator %s'%( newWorkflow, comment ),do_batch=False)
            else:
                wfo.status = 'trouble' if options.set_trouble else 'forget' 
                wfi.notifyRequestor('Rejected by unified operator %s'%( comment ),do_batch=False)
                session.commit()

        else:
            print "error in rejecting",wfo.name,results

if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
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
    parser.add_option('--EventAwareLumiBased', help="set the splitting algo of the clone", default=False, action='store_true')
    parser.add_option('--TimePerEvent', help="set the time/event on the clone", default=0, type=float)
    parser.add_option('--filelist',help='a file with a list of workflows',default=None)
    parser.add_option('--no_output',help='keep only the output of the last task of TaskChain',default=False,action='store_true')
    parser.add_option('--deterministic',help='set the splitting to deterministic in the clone',default=False,action='store_true')
    parser.add_option('--runs',help='set the run whitelist in the clone',default=None)
    parser.add_option('-s','--to_stepchain',help='transform a TaskChain into StepChain',default=False,action='store_true')
    (options,args) = parser.parse_args()

    spec=None
    if len(args):
        spec = args[0]
    

    rejector(url, spec, options)
