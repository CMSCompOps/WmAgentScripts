#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo,\
                  getWorkflows, \
                  componentInfo, \
                  reqmgr_url, \
                  campaignInfo, \
                  unifiedConfiguration, \
                  sendLog
import json
import os
import optparse
from collections import defaultdict
import random


def equalizor(url , specific = None, options=None):

    up = componentInfo(soft=['mcm','wtc','jira']) 

    if not specific:
        if not up.check(): return # Only check component when running cron job with everything
        workflows = getWorkflows(url, status='running-closed', details=True)
        workflows.extend(getWorkflows(url, status='running-open', details=True))

    UC = unifiedConfiguration()
    CI = campaignInfo()

    memory_correction = UC.get('memory_correction')

    stats_to_go = UC.get('tune_min_stats')
    def getPerf( task , stats_to_go, original_ncore=1):
        task = task.split('/')[1]+'/'+task.split('/')[-1]

        print "#"*10,"input read performance","#"*10
        failed_out = (None,None,None,None)
        try:
            u = 'http://cms-gwmsmon.cern.ch/prodview/json/historynew/highio720/%s'%task
            print u
            io_data = json.loads(os.popen('curl -s --retry 5 %s'%u).read())
        except Exception as e:
            print "No good io data"
            print str(e)
            io_data = None

        read_need = None
        binned_io = defaultdict( lambda : defaultdict(float))
        denom = 'CoreHr' # 'ReadTimeHrs'
        if io_data and 'aggregations' in io_data and io_data["aggregations"]["2"]["buckets"]:
            buckets = io_data["aggregations"]["2"]["buckets"][0].get('RequestCpus',{}).get('buckets',[])
            for bucket in buckets:
                ncore = bucket['key']
                igb = bucket.get('InputGB',{}).get('value',0)
                rth = bucket.get('ReadTimeHrs',{}).get('value',0)
                ch = bucket.get('CoreHr',{}).get('value',0)
                d = bucket.get(denom,{}).get('value',0)
                print ncore,"read",igb,"spend",rth,"reading",ch,"running"
                if d:
                    binned_io[ncore] = (igb *1024.*1024.) / (d*60*60) ## MB/s

        if binned_io:
            if denom == 'CoreHr':
                per_core_io = [ v for k,v in binned_io.items()]
            else:
                per_core_io = [ v/k for k,v in binned_io.items()]                
            per_core_io = max( per_core_io )
            print "binned I/O",dict(binned_io)
            read_need = int(per_core_io)

        print "#"*10,"memory usage performance","#"*10
        inflate_memory = 1.2
        try:
            u = 'http://cms-gwmsmon.cern.ch/prodview/json/historynew/memorycpu720/%s/success'%task
            print u
            perf_data = json.loads(os.popen('curl -s --retry 5 %s'%u).read())
        except Exception as e:
            print str(e)
            return failed_out
        binned_memory = defaultdict( lambda : defaultdict(float))
        buckets = filter(lambda i:i['key']!=0,perf_data['aggregations']["2"]["buckets"]) if 'aggregations' in perf_data else [] ## fail safe on ES missing data
        for bucket in buckets:
            sub_buckets = filter(lambda i:i['key']!=0, bucket["3"]["buckets"])
            for sub_bucket in sub_buckets:
                memory = int(float(bucket["key"])*inflate_memory)
                ncore = int(sub_bucket["key"])
                binned_memory[ncore][memory] += sub_bucket["doc_count"]

        memory_percentil = 90

        def weighted_percentile( values , bins , percentile):
            ## values are the count
            ## bins are the memory values
            cumsum = [ sum(values[:i+1]) for i in range(len(values)) ] 
            above = (cumsum[-1]*percentile/100.)
            index = 0
            for i in range(len(cumsum)): 
                if cumsum[i]>above: 
                    index=i
                    break
            if index==0: return bins[index]
            else:
                ## do the linear interpolation to the previous bin
                x1=bins[index-1]
                x2=bins[index]
                y1=cumsum[index-1]
                y2=cumsum[index]
                interp = x1+(x2-x1)/(y2-y1)*(above-y1)
                return interp
                
        ## now you have the binned memory binned_memory[cores][mem] = population
        percentiles = defaultdict(float)
        
        for core_count in binned_memory:
            bins = sorted(binned_memory[core_count].keys())
            values = [binned_memory[core_count][k] for k in bins]
            if sum(values) < stats_to_go: 
                print "not enough stats to go for core-binned mem usage",sum(values),"<",stats_to_go,"at cores=",core_count
                continue
            avg = 'N/A'
            if sum(values):
                avg = sum([(a*w) for (a,w) in zip(bins,values)]) / sum( values )
            print "Getting information at",core_count, avg
            print bins
            print values
            if values:
                percentiles[core_count] = weighted_percentile( values, bins, memory_percentil)
        
        slopes = []
        lever = []
        baseline = percentiles[original_ncore] if original_ncore in percentiles else None
        if baseline:
            for ncore,v in percentiles.items():
                if ncore == original_ncore: continue
                s = (v-baseline) / float((ncore - original_ncore))
                slopes.append( s )
                lever.append( abs(ncore - original_ncore) )
            baseline = int(baseline)
        slope = max(0,int(sum([l*v for (l,v) in zip(lever,slopes)])/sum(lever))) if slopes else None
        print "From multiple memory points",baseline,"MB baseline at",original_ncore,"cores, and",slope,"per thread"
        print slopes
        print lever
        
        b_m = None
        if baseline: 
            b_m = baseline*1.1 ## put 10% on top for safety

        print "#"*10,"total core-hour performance","#"*10

        time_percentil = 95
        backup_time_percentil = 99
        try:
            ## the returned value is the commitedcorehours ~ walltime * 4
            u = 'http://cms-gwmsmon.cern.ch/prodview/json/historynew/percentileruntime720/%s'%task
            print u
            percentile_data = json.loads(os.popen('curl -s --retry 5 %s'%u).read())
        except Exception as e:
            print str(e)
            return failed_out
        
        p_t = percentile_data['aggregations']["2"]["values"].get("%.1f"%time_percentil,None) if 'aggregations' in percentile_data else None
        bck_p_t = percentile_data['aggregations']["2"]["values"].get("%.1f"%backup_time_percentil,None) if 'aggregations' in percentile_data else None
        if p_t=="NaN":p_t=None
        if bck_p_t=="NaN":p_t=None
        
        if p_t: p_t*=60. ## convert in mins
        if bck_p_t: bck_p_t*=60 ## convert in min
        w_t = 0
        if "hits" in percentile_data and "total" in percentile_data["hits"]:
            w_t = percentile_data["hits"]["total"]
        
        b_t = None
        used_percentil = time_percentil
        if w_t > stats_to_go and p_t:
            if bck_p_t and bck_p_t > 2*p_t:
                print backup_time_percentil,"percentil is giving much better job runtime measurements"
                used_percentil = backup_time_percentil
                b_t = int(bck_p_t)
            else:
                b_t = int(p_t)
        else:
            print "not enough stats for time",w_t,"<",stats_to_go,"value is",p_t

        print "%s%% of the jobs are running for a total core-min under %s [min]"%( used_percentil, b_t)
        print "#"*30
        return (b_m,slope,b_t, read_need)
        
    def getcampaign( task , req=None):
        taskname = task.pathName.split('/')[-1]
        if req:
            c = req.getCampaignPerTask( taskname )
            if c: return c

        try:
            if hasattr( task, 'prepID'):
                return task.prepID.split('-')[1]
            elif taskname.count('-')>=1:
                return taskname.split('-')[1]
            else:
                return None
        except Exception as e :
            print "Inconsistent prepid very likely"
            print str(e)
            return None

    tune_performance = []

    if specific and ":" in specific:
        specific,specific_task = specific.split(':')

    if specific:
        wfs = session.query(Workflow).filter(Workflow.name.contains(specific)).all()
    else:
        wfs = session.query(Workflow).filter(Workflow.status == 'away').all()

    warning_short_time = UC.get('warning_short_time')
    warning_long_time = UC.get('warning_long_time')
    warning_mem = UC.get('warning_mem')

    short_tasks = set()
    long_tasks = set()
    hungry_tasks = set()
    bad_hungry_tasks = set()
    performance = {}

    random.shuffle( wfs )
    for wfo in wfs:
        if not wfo.status in ['away']: continue

        if specific and not specific in wfo.name: 
            continue
        if specific:
            wfi = workflowInfo(url, wfo.name)
        else:
            cached = filter(lambda d : d['RequestName']==wfo.name, workflows)
            if not cached : continue
            wfi = workflowInfo(url, wfo.name, request = cached[0])

        if wfi.isRelval(): continue

        if not wfi.request['RequestStatus'] in ['running-open','running-closed'] and not specific: continue

        tasks_and_campaigns = []
        for task in wfi.getWorkTasks():
            tasks_and_campaigns.append( (task, getcampaign(task, wfi) ) )

        # now parse this for action
        for i_task,(task,campaign) in enumerate(tasks_and_campaigns):
            taskname = task.pathName.split('/')[-1]
            print taskname,campaign


            tune = CI.get(campaign,'tune',options.tune)
            if tune and not campaign in tune_performance:
                tune_performance.append( campaign )

            # get the task performance, for further massaging.
            if campaign in tune_performance or options.tune:
                print "performance",task.taskType,task.pathName
                if task.taskType in ['Processing','Production']:
                    mcore = wfi.getCorePerTask( taskname )
                    set_memory,set_slope,set_time,set_io = getPerf( task.pathName , stats_to_go, original_ncore = mcore)

                    ## get values from gmwsmon
                    performance[task.pathName] = {}
                    if set_slope:
                        if set_memory:
                            ## make sure it cannot go to zero
                            max_mem_per_core = int(set_memory / float(mcore))                            
                            set_slope = min( set_slope, max_mem_per_core) 
                        performance[task.pathName]['slope']=set_slope
                    mem = wfi.getMemoryPerTask( taskname )
                    print taskname,mem
                    for key,add_hoc_mem in memory_correction.items():
                        if key in taskname and mem > add_hoc_mem and (set_memory==None or set_memory > add_hoc_mem):
                            print "overiding",set_memory,"to",add_hoc_mem,"by virtue of add-hoc memory_correction",key
                            set_memory = min( add_hoc_mem, set_memory) if set_memory else add_hoc_mem

                    if set_memory:
                        set_memory =  min(set_memory, 20000) ## no bigger than 20G
                        set_memory =  max(set_memory, mcore*1000) ## do not go too low. not less than 1G/core.
                        print "truly setting memory to",set_memory
                        performance[task.pathName]['memory']= set_memory

                    if set_time:
                        if set_time > warning_long_time*mcore:
                            print "WHAT IS THIS TASK",task.pathName,"WITH",set_time/mcore,"large runtime"
                            wfi.sendLog('equalizor','WARNING the task %s was found to run long jobs  of %d [h] %d [mins] at original %d cores setting'%( taskname, divmod(set_time / mcore, 60)[0], divmod(set_time / mcore,60)[1] , mcore))
                            long_tasks.add( (task.pathName, set_time / mcore, mcore) )
                            
                        set_time =  min(set_time, int(warning_long_time*mcore)) ## max to 24H per mcore
                        set_time =  max(set_time, int(30.*mcore)) ## min to 30min per core
                        performance[task.pathName]['time'] = set_time 
                    if set_io:
                        performance[task.pathName]['read'] = set_io

                    ##make up some warnings
                    if set_time and (set_time / mcore) < warning_short_time: ## looks like short jobs all around
                        print "WHAT IS THIS TASK",task.pathName,"WITH",set_time/mcore,"small runtime"
                        wfi.sendLog('equalizor','The task %s was found to run short jobs of %.2f [mins] at original %d cores setting'%( taskname, set_time / mcore , mcore))
                        short_tasks.add( (task.pathName, set_time / mcore, mcore) )

                    if mem and ((mem > warning_mem*mcore) if wfi.request['RequestType'] != 'StepChain' else (mem > warning_mem*wfi.getMulticore())):
                        print "WHAT IS THIS TASK",task.pathName,"WITH",mem,"memory requirement at",mcore,"cores"
                        wfi.sendLog('equalizor','The task %s was found to be confiugred with %d MB over %d MB/core at %d cores'%( taskname, mem, warning_mem, mcore))
                        bad_hungry_tasks.add( (task.pathName, mem, mcore ) )

                    if set_memory and (set_memory > warning_mem*mcore):
                        print "WHAT IS THIS TASK",task.pathName,"WITH",set_memory,"memory requirement at",mcore,"cores"
                        wfi.sendLog('equalizor','The task %s was found to run jobs using %d MB over %d MB/core at %d cores'%( taskname, set_memory, warning_mem, mcore))

                        hungry_tasks.add( (task.pathName, set_memory, mcore) )
                        
                    wfi.sendLog('equalizor',"""Performance tuning of task %s
                                                                          %s MB base memory at %d core
                                                                          %s MB per thread
                                                                          %s min assuming runing 1-thread
                                                                          %s KBs read estimated per thread
                                                                          """
                                                                          %( taskname,
                                                                          set_memory, mcore,
                                                                          set_slope,
                                                                          set_time,
                                                                          set_io ))

    if short_tasks:
        sendLog('equalizor','These tasks are running very short jobs. Shorter than %d [min] \n %s'%(warning_short_time,'\n'.join( ["%s, %.f [mins] observed time at original %d core count"%(a,b,c) for (a,b,c) in sorted( short_tasks )] )), level='critical')

    if long_tasks:
        sendLog('equalizor','These tasks are running very long jobs. Longer than %d [min] per core.\n %s'%(warning_long_time,'\n'.join( ["%s, %.f [h] %.f [mins] observed time at original %d core count"%(a,divmod(b,60)[0], divmod(b,60)[1],c) for (a,b,c) in sorted( long_tasks )] )), level='critical')

    if hungry_tasks:
        sendLog('equalizor','These tasks are running with more memory than usual\n %s'%( '\n'.join( ["%s, %.f [MB] observed concumption at original %d core count"%(a,b,c) for (a,b,c) in sorted( hungry_tasks )] )), level='critical')

    if bad_hungry_tasks:
        sendLog('equalizor','These tasks are configured with more memory than usual\n %s'%( '\n'.join( ["%s, %.f [MB] submitted requirement at original %d core count"%(a,b,c) for (a,b,c) in sorted( bad_hungry_tasks )] )), level='critical')


if __name__ == "__main__":
    url = reqmgr_url
    parser = optparse.OptionParser()
    parser.add_option('--tune',help='Enable performance tuning', default=False, action='store_true')
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    equalizor(url, spec, options=options)
