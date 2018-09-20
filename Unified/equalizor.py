#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, getWorkflows, global_SI, sendEmail, componentInfo, getDatasetPresence, monitor_dir, monitor_pub_dir, reqmgr_url, campaignInfo, unifiedConfiguration, sendLog, do_html_in_each_module, base_eos_dir, eosRead, eosFile, agent_speed_draining
import reqMgrClient
import json
import os
import time
import random
import optparse
from collections import defaultdict
import random
import copy 
import itertools
from htmlor import htmlor
import math

def equalizor(url , specific = None, options=None):

    up = componentInfo(soft=['mcm','wtc']) 
    if not up.check(): return 

    if not specific:
        workflows = getWorkflows(url, status='running-closed', details=True)
        workflows.extend(getWorkflows(url, status='running-open', details=True))

    ## start from scratch
    modifications = defaultdict(dict)
    ## define regionality site => fallback allowed. feed on an ssb metric ??
    mapping = defaultdict(list)
    reversed_mapping = defaultdict(list)
    regions = defaultdict(list)

    UC = unifiedConfiguration()
    over_rides = []
    use_T0 = ('T0_CH_CERN' in UC.get("site_for_overflow"))
    if options.t0: use_T0 = True
    if use_T0: over_rides.append('T0_CH_CERN')

    use_HLT = ('T2_CH_CERN_HLT' in UC.get("site_for_overflow"))
    if options.hlt: use_HLT = True
    if use_HLT: over_rides.append('T2_CH_CERN_HLT')
    
    use_CSCS = ('T0_CH_CSCS_HPC' in UC.get("site_for_overflow"))
    if options.cscs: use_CSCS = True
    if use_CSCS: over_rides.append('T0_CH_CSCS_HPC')
    

    SI = global_SI( over_rides )
    print sorted(SI.all_sites)
    print sorted(SI.sites_T0s)

    CI = campaignInfo()
    
    #sites_to_consider = SI.all_sites
    sites_to_consider = SI.sites_ready
    for site in sites_to_consider:
        region = site.split('_')[1]
        if not region in ['US'
                          ,'DE','IT','FR',
                          'ES',
                          'UK',
                          'RU'### latest addition
                          ]: continue
        regions[region] = [region] 

    def site_in_depletion(s):
        return True
        if s in SI.sites_pressure:
            (m, r, pressure) = SI.sites_pressure[s]
            if float(m) < float(r):
                print s,m,r,"lacking pressure"
                return True
            else:
                print s,m,r,"pressure"
                pass
                
        return False

    for site in sites_to_consider:
        region = site.split('_')[1]
        ## fallback to the region, to site with on-going low pressure
        mapping[site] = [fb for fb in sites_to_consider if any([('_%s_'%(reg) in fb and fb!=site and site_in_depletion(fb))for reg in regions[region]]) ]
    

    for site in sites_to_consider:
        if site.split('_')[1] == 'US': ## to all site in the US
            ## add NERSC 
            mapping[site].append('T3_US_NERSC')
            #mapping[site].append('T3_US_TACC')
            #mapping[site].append('T3_US_PSC')
            ## add OSG            
            mapping[site].append('T3_US_OSG')
            #mapping[site].append('T3_US_Colorado')
            pass

    if use_HLT:
        mapping['T2_CH_CERN'].append('T2_CH_CERN_HLT')

    if use_T0:
        ## who can read from T0
        mapping['T2_CH_CERN'].append('T0_CH_CERN')
        mapping['T1_IT_CNAF'].append('T0_CH_CERN')
        mapping['T1_FR_CCIN2P3'].append('T0_CH_CERN')
        mapping['T1_DE_KIT'].append('T0_CH_CERN')

    if use_CSCS: 
	## analog config to T0: 
	mapping['T2_CH_CERN'].append('T0_CH_CSCS_HPC')
        mapping['T1_IT_CNAF'].append('T0_CH_CSCS_HPC')
        mapping['T1_FR_CCIN2P3'].append('T0_CH_CSCS_HPC')
        mapping['T1_DE_KIT'].append('T0_CH_CSCS_HPC')

    ## temptatively
    mapping['T0_CH_CERN'].append( 'T2_CH_CERN' )

    ## all europ can read from CERN
    for reg in ['IT','DE','UK','FR','BE','ES']:
        mapping['T2_CH_CERN'].extend([fb for fb in sites_to_consider if '_%s_'%reg in fb])
        pass

    ## all europ T1 among each others
    europ_t1 = [site for site in sites_to_consider if site.startswith('T1') and any([reg in site for reg in ['IT','DE','UK','FR','ES','RU']])]
    #print europ_t1
    for one in europ_t1:
        for two in europ_t1:
            if one==two: continue
            mapping[one].append(two)
            pass
        ## all EU T1 can read from T0
        mapping['T0_CH_CERN'].append( one )
        
    mapping['T0_CH_CERN'].append( 'T1_US_FNAL' )
    #mapping['T1_IT_CNAF'].append( 'T1_US_FNAL' )
    #mapping['T1_IT_CNAF'].extend( [site for site in SI.sites_ready if '_US_' in site] ) ## all US can read from CNAF
    mapping['T1_IT_CNAF'].append( 'T2_CH_CERN' )
    mapping['T1_DE_KIT'].append( 'T2_CH_CERN' )
    mapping['T2_CH_CERN'].append( 'T1_IT_CNAF' )
    mapping['T2_CH_CERN'].append( 'T1_US_FNAL' )
    mapping['T2_CH_CERN'].append('T3_CH_CERN_HelixNebula')
    mapping['T2_CH_CERN'].append('T3_CH_CERN_HelixNebula_REHA')
    

    for site in sites_to_consider:
        if '_US_' in site:
            mapping[site].append('T2_CH_CERN')
    ## make them appear as OK to use
    force_sites = []

    ## overflow CERN to underutilized T1s
    upcoming = json.loads( eosRead('%s/GQ.json'%monitor_dir) )
    for possible in SI.sites_T1s:
        if not possible in upcoming:
            mapping['T2_CH_CERN'].append(possible)
            pass

    take_site_out = UC.get('site_out_of_overflow')

    for site,fallbacks in mapping.items():
        mapping[site] = list(set(fallbacks))
    

    ### mapping is a dictionnary where
    # key can read from site in values. 
    ### reverserd mapping is a dictionnary where
    # key can be read by site in values.
    ## create the reverse mapping for the condor module
    for site,fallbacks in mapping.items():
        if site in take_site_out:
            mapping.pop(site)
            continue
        for fb in fallbacks:
            if fb == site: 
                ## remove self
                mapping[site].remove(fb)
                continue
            if fb in take_site_out:
                ## remove those to be removed
                mapping[site].remove(fb)
                continue
            if not site in reversed_mapping[fb]:
                reversed_mapping[fb].append(site)

    #for site in mapping.keys():
    #    mapping[site] = list(set(mapping[site]))

    ## this is the fallback mapping
    #print "Direct mapping : site => overflow"
    #print json.dumps( mapping, indent=2)
    #print "Reverse mapping : dest <= from origin"
    #print json.dumps( reversed_mapping, indent=2)

    altered_tasks = set()

    def running_idle( wfi , task_name):
        gmon = wfi.getGlideMon()
        #print gmon
        if not gmon: return (0,0)
        if not task_name in gmon: return (0,0)
        return (gmon[task_name]['Running'], gmon[task_name]['Idle'])

    def needs_action( wfi, task, min_idled = 100):
        task_name = task.pathName.split('/')[-1]
        running, idled = running_idle( wfi, task_name)
        go = True
        if not idled and not running : 
            go = False
        if idled < 100: 
            go = False
        if (not running and idled) or (running and (idled / float(running) > needs_action.pressure)):
            go = True
        else:
            go = False
        return go, task_name, running, idled
    needs_action.pressure = UC.get('overflow_pressure')

    mem_quanta = UC.get('mem_quanta') #MB
    time_quanta = UC.get('time_quanta') # min
    slope_quanta = UC.get('slope_quanta') #MB
    read_quanta = UC.get('read_quanta') #kB/min or something

    memory_correction = UC.get('memory_correction')

    def quantize( value, quanta ):
        N = int(math.ceil(float(value) / quanta))
        return (N)*quanta
    def s_quantize( value, quanta):
        s = str(quantize( value, quanta ))
        print value,"==quantized(",quanta,")==>",s
        return s
        #return str(quantize( value, quanta ))

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
            #return failed_out

        read_need = None
        binned_io = defaultdict( lambda : defaultdict(float))
        inputGB = None
        denom = 'CoreHr' # 'ReadTimeHrs'
        if io_data and 'aggregations' in io_data and io_data["aggregations"]["2"]["buckets"]:
            inputGB=io_data["aggregations"]["2"]["buckets"][0].get('InputGB',{}).get('value',None)
            buckets = io_data["aggregations"]["2"]["buckets"][0].get('RequestCpus',{}).get('buckets',[])
            for bucket in buckets:
                ncore = bucket['key']
                igb = bucket.get('InputGB',{}).get('value',0)
                rth = bucket.get('ReadTimeHrs',{}).get('value',0)
                ch = bucket.get('CoreHr',{}).get('value',0)
                ## do the math
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
            #per_core_io = sum( per_core_io ) / float(len(per_core_io))
            print "binned I/O",dict(binned_io)
            read_need = int(per_core_io)

        print "#"*10,"memory usage performance","#"*10
        inflate_memory = 1.2
        try:
            #u = 'http://cms-gwmsmon.cern.ch/prodview/json/historynew/memorycpu720/%s/success'%task
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
                #interp = y1+(y2-y1)/(x2-x1)*(above-x1)
                interp = x1+(x2-x1)/(y2-y1)*(above-y1)
                return interp
                
        ## now you have the binned memory binned_memory[cores][mem] = population
        #print json.dumps( binned_memory , indent=2 )
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

        ## do the fit per core
        #print json.dumps( percentiles , indent=2 )
        
        slopes = []
        lever = []
        #print original_ncore
        #print percentiles
        baseline = percentiles[original_ncore] if original_ncore in percentiles else None
        if baseline:
            for ncore,v in percentiles.items():
                if ncore == original_ncore: continue
                s = (v-baseline) / float((ncore - original_ncore))
                slopes.append( s )
                lever.append( abs(ncore - original_ncore) )
            baseline = int(baseline)
        #slope = max(0,int(sum(slopes) / len(slopes))) if slopes else None
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
    def close( interface ):
        eosFile('%s/equalizor.json.new'%monitor_pub_dir,'w').write( json.dumps( interface, indent=2)).close()
        os.system('mv %s/equalizor.json.new %s/equalizor.json'%(monitor_pub_dir,monitor_pub_dir))
        os.system('cp %s/equalizor.json %s/logs/equalizor/equalizor.%s.json'%(monitor_pub_dir,monitor_dir,time.mktime(time.gmtime())))
        ## move it where people use to see it ## should go away at some point
        os.system('cp %s/equalizor.json /afs/cern.ch/user/c/cmst2/www/unified/.'%( monitor_pub_dir ))

    interface = {
        'mapping' : mapping,
        'reversed_mapping' : reversed_mapping,
        'modifications' : {},
        'time' : {},
        'memory' : {},
        'slope' : {},
        'read' : {},
        'hold_site' : [],
        'release_site' : [],
        'hold': {},
        'release' : {},
        'resizing' : {},
        'highprio' : [],
        'speed_drain' : []
        }
    if options.augment or options.remove:
        previous = json.loads( eosRead('%s/equalizor.json'%monitor_pub_dir))
        interface['modifications'] = previous.get('modifications',{})
        interface['memory'] = previous.get('memory',{})
        interface['time'] = previous.get('time',{})
        interface['slope'] = previous.get('slope', {})
        interface['read'] = previous.get('read',{})
        interface['hold_site'] = previous.get('hold_site',[])
        interface['release_site'] = previous.get('release_site',[])
        interface['hold'] = previous.get('hold',{})
        interface['release'] = previous.get('release',{})
        interface['highprio'] = previous.get('highprio',[])
        interface['resizing'] = previous.get('resizing',{})
        interface['speed_drain'] = previous.get('speed_drain',[])
        
    if options.remove:
        if specific in interface['modifications']:
            print "poping",specific
            interface['modifications'].pop(specific)
            close( interface )
        return 


    PU_locations = {}
    PU_overflow = {}
    PRIM_overflow = {}
    PREMIX_overflow = {}
    LHE_overflow = {}
    tune_performance = []

    pending_HLT = 0
    max_HLT = 60000
    pending_T0 = 0
    max_T0 = 60000
    try:
        gmon = json.loads(os.popen('curl -s http://cms-gwmsmon.cern.ch/prodview/json/T2_CH_CERN_HLT').read())
        pending_HLT += gmon["Running"]
        pending_HLT += gmon["MatchingIdle"]
    except:
        pass

    restricting_to_ready = [
                            ]
    
    remove_from = {
        #'cerminar_Run2016B-v1-BTagCSV-23Sep2016_8020_160923_163224_2174' : ['T2_CH_CERN_HLT']
        }

    add_to = {
        #'pdmvserv_EXO-RunIISpring16MiniAODv2-05060_00552_v0__161001_151813_7925' : ['T3_US_OSG'],
        #'cerminar_Run2016C-v2-SingleElectron-23Sep2016_8020_160923_182146_3498' : ['T3_US_NERSC'],
        #'cerminar_Run2016C-v2-Tau-23Sep2016_8020_160923_182336_5649' : ['T3_US_NERSC'],
        }

    perf_per_config = defaultdict(dict)
    for k,v in json.loads(eosRead('%s/perf_per_config.json' % base_eos_dir)).items(): perf_per_config[k] = v

    stay_within_site_whitelist = False
    specific_task=None
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
    resizing = {}
    no_routing = [
        ]
    random.shuffle( wfs )
    for wfo in wfs:
        if not wfo.status in ['away']: continue
        if wfo.name in no_routing and not options.augment:
            continue

        if specific and not specific in wfo.name: 
            continue
        if specific:
            wfi = workflowInfo(url, wfo.name)
        else:
            cached = filter(lambda d : d['RequestName']==wfo.name, workflows)
            if not cached : continue
            wfi = workflowInfo(url, wfo.name, request = cached[0])

        if wfi.isRelval(): continue

        ## only running-* should get re-routed, unless done by hand
        if not wfi.request['RequestStatus'] in ['running-open','running-closed'] and not specific: continue

        is_chain = (wfi.request['RequestType'] in ['TaskChain','StepChain'])
        tasks_and_campaigns = []
        for task in wfi.getWorkTasks():
            tasks_and_campaigns.append( (task, getcampaign(task, wfi) ) )
        
        
        lhe,prim,_,sec,sites_allowed = wfi.getSiteWhiteList()#getIO()
        sites_allowed_se = set()
        for s in sites_allowed: sites_allowed_se.update( SI.CE_to_SEs(s) )
        ncores = wfi.getMulticore()
        mem = wfi.getMemory()
        memory_allowed = SI.sitesByMemory( mem , maxCore=ncores)

        if not lhe and not prim and not sec and not wfi.isRelval():
            ## no input at all: go for OSG!!!
            print "adding", wfo.name, " addhoc for OSG and no task of the workflow requires any input"
            add_to[wfo.name] = ['T3_US_OSG']
            add_to[wfo.name] = ['T3_US_Colorado']
            add_to[wfo.name] = ['T3_US_NERSC']
            add_to[wfo.name] = ['T3_US_PSC']
            add_to[wfo.name] = ['T3_US_TACC']
            add_to[wfo.name] = ['T3_CH_CERN_HelixNebula']

        ## check needs override
        needs_overide = False
        if not needs_overide and  options.augment: needs_overide=True

        def overide_from_agent( wfi, needs_overide):
            bad_agents = []#'http://cmssrv219.fnal.gov:5984']
            if not bad_agents: return needs_overide
            if needs_overide: return True
            agents = wfi.getAgents()

            wqss = ['Running','Acquired']
            if any([agent in agents.get(wqs,{}).keys() for wqs,agent in itertools.product( wqss, bad_agents)]):
                print "overriding the need for bad agent"
                needs_overide = True
            return needs_overide

        good_ces = set()
        for s in SI.sites_ready:
            good_ces.update( SI.CE_to_SEs( s ))

        configcache = wfi.getConfigCacheID()
        ## now parse this for action
        for i_task,(task,campaign) in enumerate(tasks_and_campaigns):
            taskname = task.pathName.split('/')[-1]
            print taskname,campaign
            if options.augment:
                print task.pathName
                print campaign

            resize = CI.get(campaign,'resize',{})


            if resize and type(resize)==dict:# and not is_chain:
                print "adding",task.pathName,"in resizing"
                resizing[task.pathName] = copy.deepcopy(resize)
            elif resize and resize=='auto':
                ## can we add this tuning add-hoc by assuming Memory = a + Ncore*b, where a is a fraction of Memory ?
                mcore = wfi.getCorePerTask( taskname )
                if mcore>=UC.get('min_core_for_resize'):
                    mem = wfi.getMemoryPerTask( taskname )
                    fraction_constant = 0.4
                    min_mem_per_core = 10 ## essentially no min
                    print "task param", mem,mcore
                    max_mem_per_core = int(mem/float(mcore))
                    mem_per_core_c = int((1-fraction_constant) * mem / float(mcore))
                    mem_per_core = max(mem_per_core_c, min_mem_per_core)
                    mem_per_core = min(mem_per_core, max_mem_per_core)
                    min_core = max(int(mcore/3.), min(mcore,3)) 
                    max_core = min(int(2*mcore)+2, 15)
                    wfi.sendLog('equalizor', "Adding %s in resizing, calculating %d < %d < %d MB, using %d to %d cores"%(
                        task.pathName,
                        min_mem_per_core, mem_per_core_c, max_mem_per_core,
                        min_core,max_core))
                    #print "adding", task.pathName,"in resizing, calculating",mem_per_core_c,
                    #"MB per thread, minimum",
                    #min_mem_per_core,"MB, using from",min_core,"to",max_core,"threads"

                    resizing[task.pathName] = { "minCores":min_core, "maxCores": max_core, "memoryPerThread": quantize(mem_per_core, slope_quanta)}
                else:
                    print "do not start resizing a task that was set <",UC.get('min_core_for_resize')

            if task.pathName in resizing:
                addhoc_resize = ['HIG-RunIIFall17GS-00004']
                for k_resize in addhoc_resize:
                    if k_resize in taskname:
                        resizing[task.pathName]["minCores"]=1
                        resizing[task.pathName]["maxCores"]=1

            tune = CI.get(campaign,'tune',options.tune)
            if tune and not campaign in tune_performance:
                tune_performance.append( campaign )

            overflow = CI.get(campaign,'overflow',{})
            if overflow:
                if "PRIM" in overflow and not campaign in PRIM_overflow:
                    PRIM_overflow[campaign] = copy.deepcopy(overflow['PRIM'])
                    print "adding",campaign,"to PRIM overflow"
                if "PREMIX" in overflow and not campaign in PREMIX_overflow:
                    PREMIX_overflow[campaign] = copy.deepcopy(overflow['PREMIX'])
                    print "adding",campaign,"to PREMIX overflow"
                if "PU" in overflow and not campaign in PU_overflow:
                    PU_overflow[campaign] = copy.deepcopy(overflow['PU'])
                    print "adding",campaign,"to PU overflow rules"
                if "LHE" in overflow and not campaign in LHE_overflow:
                    site_list = overflow['LHE'].get('site_list',"")
                    if site_list:
                        if type(site_list)==list:
                            LHE_overflow[campaign] = site_list
                        else:
                            print site_list
                            if hasattr(SI,site_list):
                                LHE_overflow[campaign] = copy.deepcopy( getattr(SI,site_list) )
                            else:
                                LHE_overflow[campaign] = site_list.split(',')
                    print "adding",campaign,"to light input overflow rules",sorted(LHE_overflow[campaign])


            ### get the task performance, for further massaging.
            if campaign in tune_performance or options.tune:
                print "performance",task.taskType,task.pathName
                if task.taskType in ['Processing','Production']:
                    mcore = wfi.getCorePerTask( taskname )
                    set_memory,set_slope,set_time,set_io = getPerf( task.pathName , stats_to_go, original_ncore = mcore)

                    ## get values from gmwsmon
                    # massage the values : 95% percentile
                    performance[task.pathName] = {}
                    if set_slope:
                        if set_memory:
                            ## make sure it cannot go to zero
                            max_mem_per_core = int(set_memory / float(mcore))                            
                            set_slope = min( set_slope, max_mem_per_core) 
                        performance[task.pathName]['slope']=set_slope
                        if task.pathName in resizing and "memoryPerThread" in resizing[task.pathName]:
                            resizing[task.pathName]["memoryPerThread"] = quantize(set_slope, slope_quanta)
                        perf_per_config[configcache.get( taskname , 'N/A')]['slope'] = set_slope
                    mem = wfi.getMemoryPerTask( taskname )
                    print taskname,mem
                    for key,add_hoc_mem in memory_correction.items():
                        if key in taskname and mem > add_hoc_mem and (set_memory==None or set_memory > add_hoc_mem):
                            print "overiding",set_memory,"to",add_hoc_mem,"by virtue of add-hoc memory_correction",key
                            set_memory = min( add_hoc_mem, set_memory) if set_memory else add_hoc_mem

                    if set_memory:
                        set_memory =  min(set_memory, 20000) ## no bigger than 20G
                        set_memory =  max(set_memory, mcore*1000) ## do not go too low. not less than 1G/core.
                        print "trully setting memory to",set_memory
                        performance[task.pathName]['memory']= set_memory
                        perf_per_config[configcache.get( taskname , 'N/A')]['memory'] = set_memory

                    if set_time:
                        if set_time > warning_long_time*mcore:
                            print "WHAT IS THIS TASK",task.pathName,"WITH",set_time/mcore,"large runtime"
                            wfi.sendLog('equalizor','WARNING the task %s was found to run long jobs  of %d [h] %d [mins] at original %d cores setting'%( taskname, divmod(set_time / mcore, 60)[0], divmod(set_time / mcore,60)[1] , mcore))
                            long_tasks.add( (task.pathName, set_time / mcore, mcore) )
                            
                        set_time =  min(set_time, int(warning_long_time*mcore)) ## max to 24H per mcore
                        set_time =  max(set_time, int(30.*mcore)) ## min to 30min per core
                        performance[task.pathName]['time'] = set_time 
                        perf_per_config[configcache.get( taskname , 'N/A')]['time'] = set_time
                    if set_io:
                        performance[task.pathName]['read'] = set_io
                        perf_per_config[configcache.get( taskname , 'N/A')]['read'] = set_io

                    ##make up some warnings
                    if set_time and (set_time / mcore) < warning_short_time: ## looks like short jobs all around
                        print "WHAT IS THIS TASK",task.pathName,"WITH",set_time/mcore,"small runtime"
                        wfi.sendLog('equalizor','The task %s was found to run short jobs of %.2f [mins] at original %d cores setting'%( taskname, set_time / mcore , mcore))
                        short_tasks.add( (task.pathName, set_time / mcore, mcore) )

                    if mem and (mem > warning_mem*mcore):
                        print "WHAT IS THIS TASK",task.pathName,"WITH",mem,"memory requirement at",mcore,"cores"
                        wfi.sendLog('equalizor','The task %s was found to be confiugred with %d GB over %d GB/core at %d cores'%( taskname, mem, warning_mem, mcore))
                        bad_hungry_tasks.add( (task.pathName, mem, mcore ) )

                    if set_memory and (set_memory > warning_mem*mcore):
                        print "WHAT IS THIS TASK",task.pathName,"WITH",set_memory,"memory requirement at",mcore,"cores"
                        wfi.sendLog('equalizor','The task %s was found to run jobs using %d GB over %d GB/core at %d cores'%( taskname, set_memory, warning_mem, mcore))

                        hungry_tasks.add( (task.pathName, set_memory, mcore) )
                        
                    wfi.sendLog('equalizor',"""Performance tuning of task %s
%s GB base memory at %d core
%s GB per thread
%s min assuming runing 1-thread
%s KBs read estimated per thread
"""%( taskname, 
      set_memory, mcore,
      set_slope,
      set_time,
      set_io ))
                    
            ## rule to remove from the site whitelist site that do not look ready for unified (local banning)
            if wfo.name in restricting_to_ready:
                if task.taskType in ['Production']:
                    new_list = list(set(SI.sites_ready)&set(wfi.request['SiteWhitelist']))
                    modifications[wfo.name][task.pathName] = { "ReplaceSiteWhitelist" : new_list }

            if campaign in PREMIX_overflow:
                ## figure out secondary location and neighbors
                ## figure out primary presence and neighbors
                ## do the intersection and add if in need.
                needs, task_name, running, idled = needs_action(wfi, task)
                restrict_to_allowed = not ((campaign in tune_performance) or (CI.get(campaign,'resize',False)) )

                if options.augment:
                    print "\t",task.pathName
                if False and is_chain and task.pathName.endswith('_1') and not options.augment:
                    print i_task,"in chain prevents overflowing"
                    needs = False
                    

                if task.taskType in ['Processing','Production'] and needs:
                    secondary_locations = set(SI.sites_ready + force_sites)
                    for s in sec:
                        if not s in PU_locations:
                            presence = getDatasetPresence( url, s)
                            one_secondary_locations = [site for (site,(there,frac)) in presence.items() if frac>98.]
                            PU_locations[s] = one_secondary_locations
                        print "secondary is at",sorted(PU_locations[s])
                        secondary_locations = set([SI.SE_to_CE(site) for site in PU_locations[s]]) & secondary_locations
                    aaa_sec_grid = set(secondary_locations)
                    for site in sorted(aaa_sec_grid):
                        aaa_sec_grid.update( mapping.get(site, []) )
                    
                    print len(prim)
                    if len(prim):    
                        dataset = list(prim)[0]
                        all_blocks,blocks = wfi.getActiveBlocks()
                        count_all = sum([len(v) for k,v in all_blocks.items()])
                        presence = getDatasetPresence(url, dataset, only_blocks=blocks )
                        aaa_prim_grid = set([SI.SE_to_CE(site) for site in presence.keys()])
                        for site in sorted(aaa_prim_grid):
                            aaa_prim_grid.update( mapping.get(site, []) )

                        print sorted(aaa_prim_grid),"around primary location",sorted(presence.keys())
                        print sorted(aaa_sec_grid),"aroudn secondary location",sorted(secondary_locations)
                        ## intersect
                        aaa_grid = aaa_sec_grid & aaa_prim_grid
                        if restrict_to_allowed:
                            aaa_grid = aaa_grid & set(memory_allowed)
                    else:
                        print "premix overflow from a taskchain"
                        if restrict_to_allowed:
                            aaa_grid = set(memory_allowed) & aaa_sec_grid
                        else:
                            aaa_grid = aaa_sec_grid

                    #banned_until_you_find_a_way_to_do_this = ['T3_US_OSG']
                    banned_until_you_find_a_way_to_do_this = []
                    aaa_grid  = filter(lambda s : not s in banned_until_you_find_a_way_to_do_this, aaa_grid)
                    print sorted(aaa_grid),"for premix"
                    aaa_grid = list(set(aaa_grid)  & set(SI.sites_ready))
                    print sorted(aaa_grid),"that are ready"
                    if aaa_grid:
                        wfi.sendLog('equalizor','Extending site whitelist for %s to %s due to PREMIX_overflow'%(task.pathName,sorted(aaa_grid)))
                        modifications[wfo.name][task.pathName]= {"AddWhitelist" : sorted(aaa_grid)}

            ## rule to overflow jobs on the primary input
            if campaign in PRIM_overflow:
                if task.taskType in ['Processing','Production']:
                    if not wfi.request['TrustSitelists']:
                        ###xrootd is OFF
                        dataset = list(prim)[0]
                        all_blocks,blocks = wfi.getActiveBlocks()
                        count_all = sum([len(v) for k,v in all_blocks.items()])
                        
                        presence = getDatasetPresence(url, dataset, only_blocks=blocks )
                        in_full = [SI.SE_to_CE(site) for site,(there,_) in presence.items() if there]
                        aaa_grid= set()
                        aaa_grid_in_full = set(in_full)
                        for site in sorted(aaa_grid_in_full):
                            if site == 'T1_US_FNAL':
                                print  site,mapping.get(site, [])
                            aaa_grid_in_full.update( mapping.get(site, []) )
                        ## just add the neighbors to the existing whitelist. we could do more with block classAd
                        for site in wfi.request['SiteWhitelist']:
                            aaa_grid.update( mapping.get(site, []) )
                        add_on = [
                            'T3_US_OSG',
                            #'T3_US_NERSC'
                            ]
                        aaa_grid = aaa_grid & set(sites_allowed + add_on) ## and restrict to site that would be allowed at all (mcore, mem)
                        aaa_grid_in_full = aaa_grid_in_full & set(sites_allowed + add_on) ## and restrict to site that would be allowed at all (mcore, mem)
                        gmon = wfi.getGlideMon()
                        needs, task_name, running, idled = needs_action(wfi, task)
                        print needs,running,idled
                        site_in_use = set(gmon[task_name]['Sites']) if gmon and task_name in gmon and 'Sites' in gmon[task_name] else set()
                        print dataset,"at",sorted(in_full),len(blocks),"/",count_all
                        print "running at",sorted(site_in_use)
                        print "set for",sorted(wfi.request['SiteWhitelist'])
                        print "around current whitelist" ,sorted(aaa_grid)
                        print "around where the data is now in full", sorted(aaa_grid_in_full)

                        if needs and not (site_in_use & set(in_full)) and aaa_grid_in_full:
                            print "we could be going for replace at that point"
                            wfi.sendLog('equalizor','Replaceing site whitelie to %s dynamically'% sorted(aaa_grid_in_full))
                            modifications[wfo.name][task.pathName] = { "ReplaceSiteWhitelist" : sorted( aaa_grid_in_full) }
                        else:
                            if needs and aaa_grid:
                                print wfo.name
                                wfi.sendLog('equalizor','Adding in site white list %s dynamically'% sorted(aaa_grid) )
                                if wfo.name in modifications and task.pathName in modifications[wfo.name] and 'AddWhitelist' in modifications[wfo.name][task.pathName]:
                                    modifications[wfo.name][task.pathName]["AddWhitelist"].extend(sorted(aaa_grid))
                                else:
                                    modifications[wfo.name][task.pathName] = { "AddWhitelist" : sorted(aaa_grid) }
                    else:
                        ## the request is already is in xrootd mode (either too generous, or just about right with neighbors of full data)
                        dataset = list(prim)[0]
                        all_blocks,blocks = wfi.getActiveBlocks()
                        count_all = sum([len(v) for k,v in all_blocks.items()])
                        fraction_left = float(len(blocks))/ count_all
                        #if fraction_left< 0.5:                            print '\n'.join( blocks )
                        presence = getDatasetPresence(url, dataset, only_blocks=blocks )
                        ## in full is really the only place we can go to safely, since we have no job-data matching
                        in_full = [SI.SE_to_CE(site) for site,(there,_) in presence.items() if there]
                        gmon = wfi.getGlideMon()
                        needs, task_name, running, idled = needs_action(wfi, task)
                        site_in_use = set(gmon[task_name]['Sites']) if gmon and task_name in gmon and 'Sites' in gmon[task_name] else set()
                        print needs,running,idled

                        aaa_grid = set(in_full)
                        for site in list(aaa_grid):
                            aaa_grid.update( mapping.get(site, []) )

                        new_ones = set(in_full) - set(wfi.request['SiteWhitelist']) ## symptomatic of data have been repositionned
                        common = set(in_full) & set(wfi.request['SiteWhitelist'])
                        extra_shit = set(wfi.request['SiteWhitelist']) - aaa_grid ## symptomatic of too generous site-whitelist

                        aaa_grid = aaa_grid & set(sites_allowed+ ['T3_US_NERSC']) ## restrict to site that would be allowed at all (mcore, mem)
                        new_grid = aaa_grid - set(wfi.request['SiteWhitelist'])
                        print dataset,"is in full ",len(blocks),"/",count_all," at",in_full
                        print '\n'.join( sorted(blocks) )
                        print "running at",site_in_use
                        print "in common of the site whitelist",sorted(common)
                        print "site now also hosting the data",sorted(new_ones)
                        print "site in whitelist with no data",sorted(extra_shit)## with no data and not within aaa reach
                        if new_ones:
                            ## we will be add sites 
                            if needs and aaa_grid:
                                print wfo.name,"would replace for",sorted(aaa_grid)
                                print "but no thanks"
                                wfi.sendLog('equalizor','Changing the site whitelist to %s dynamically'%(sorted(aaa_grid)))
                                modifications[wfo.name][task.pathName] = { "ReplaceSiteWhitelist" : sorted(aaa_grid) }
                            elif new_grid:
                                print wfo.name,"would complement up to",sorted(aaa_grid)
                                wfi.sendLog('equalizor','Adding site white list to %s dynamically'% sorted(new_grid) )
                                modifications[wfo.name][task.pathName] = { "AddWhitelist" : sorted(new_grid) }
                                
                        elif len(extra_shit)>5:
                            if aaa_grid:
                                print wfo.name,"would be restricting down to",sorted(aaa_grid),"because of",sorted(extra_shit)
                                wfi.sendLog('equalizor','Restricting the white list to %s dynamically'% sorted(aaa_grid) )
                                modifications[wfo.name][task.pathName] = { "ReplaceSiteWhitelist" : sorted(aaa_grid) }    
                        else:
                            print wfo.name,"don't do anything"                            



            if wfo.name in remove_from and task.taskType in ['Processing','Production']:
                remove = remove_from[wfo.name]
                restrict_to = set(wfi.request['SiteWhitelist'])
                intersection= set(remove)&set(restrict_to)
                if intersection:
                    print intersection,"is indeed in the original whitelist"
                    restrict_to = restrict_to - set(remove)
                    modifications[wfo.name][task.pathName] = { "ReplaceSiteWhitelist" : sorted(restrict_to) }

            if wfo.name in add_to:
                if task.taskType in ['Production','Processing']:
                    augment_to = add_to[wfo.name]
                    print "adding",sorted(augment_to),"to",wfo.name
                    if wfo.name in modifications and task.pathName in modifications[wfo.name] and 'AddWhitelist' in modifications[wfo.name][task.pathName]:
                        modifications[wfo.name][task.pathName]['AddWhitelist'].extend( augment_to )
                    else:
                        modifications[wfo.name][task.pathName] = { "AddWhitelist" : augment_to }

            ### rule to avoid the issue of taskchain secondary jobs being stuck at sites processing the initial step
            if campaign in LHE_overflow:
                ## this is a rule for very light/no input to the job.
                task_has_input = (task.taskType in ['Processing'])
                
                ## find the CEs that have the same SE than the allowed sites
                ## it is not the same as doing the intersection with sites_allowed, as it can be broader here 
                neighboring_ces = set()
                for s in SI.sites_ready:
                    s_se = SI.CE_to_SEs(s)
                    if any([se in sites_allowed_se for se in s_se]):
                        neighboring_ces.add( s )
                
                needs, task_name, running, idled = needs_action(wfi, task)
                needs_overide = overide_from_agent( wfi, needs_overide)
                extend_to = set( LHE_overflow[campaign]) & neighboring_ces ## at the intersection
                

                if wfi.heavyRead():
                    needs = False
                    needs_overide = False

                ## need to stay within the allowed site white list
                #extend_to = extend_to & set(sites_allowed)

                if needs or needs_overide:
                    wfi.sendLog('equalizor','Extending site whitelist for %s to %s due to LHE_overflow'%(task.pathName.split('/')[-1],sorted(extend_to)))
                    modifications[wfo.name][task.pathName] = { "AddWhitelist" : list(extend_to),
                                                               "Running" : running,
                                                               "Pending" : idled,
                                                               "Priority" : wfi.request['RequestPriority']
                                                               }
                    altered_tasks.add( task.pathName )
                else:
                    wfi.sendLog('equalizor','%s of %s is running %d and pending %d. no light overflow'%( task_name, wfo.name, running, idled))

                """
                if task.taskType in ['Processing']: ## this is for a task processing some input
                    needs, task_name, running, idled = needs_action(wfi, task)
                    needs_overide = overide_from_agent( wfi, needs_overide)
                    extend_to = list(set(copy.deepcopy( LHE_overflow[campaign] )))
                    if stay_within_site_whitelist:
                        extend_to = list(set(extend_to) & set(wfi.request['SiteWhitelist'])) ## restrict to stupid-site-whitelist
                    extend_to = list(set(extend_to) & set(SI.sites_ready + force_sites))

                    if is_chain:
                        print "further restricting to initially allowed sites"
                        ## restrict to initial allowed sites
                        extend_to = list(set(extend_to) & set(sites_allowed))

                    if not extend_to: 
                        print "Nowhere to extend to"
                        continue
                    if extend_to and needs or needs_overide:
                        
                        modifications[wfo.name][task.pathName] = { "ReplaceSiteWhitelist" : extend_to ,"Running" : running, "Pending" : idled, "Priority" : wfi.request['RequestPriority']}
                        wfi.sendLog('equalizor','%s of %s is running %d and pending %d, taking action : ReplaceSiteWhitelist \n %s'%( task_name,
                                                                                                                                      wfo.name,
                                                                                                                                      running,
                                                                                                                                      idled ,
                                                                                                                                      json.dumps( sorted(modifications[wfo.name][task.pathName]['ReplaceSiteWhitelist']))))

                        altered_tasks.add( task.pathName )
                    else:
                        wfi.sendLog('equalizor','%s of %s is running %d and pending %d'%( task_name, wfo.name, running, idled))
                """     


            ### overflow the 76 digi-reco to the site holding the pileup
            if campaign in PU_overflow:
                force = PU_overflow[campaign]['force'] if 'force' in PU_overflow[campaign] else False
                better_secondary_locations = wfi.getClassicalPUOverflow( taskname )
                if not better_secondary_locations:
                    secondary_locations = set(SI.sites_ready + force_sites)
                    for s in sec:
                        if not s in PU_locations:
                            presence = getDatasetPresence( url, s)
                            print json.dumps( presence, indent=2)
                            one_secondary_locations = [site for (site,(there,frac)) in presence.items() if frac>98.]
                            PU_locations[s] = one_secondary_locations
                        print "secondary is at",sorted(PU_locations[s])
                        secondary_locations = set([SI.SE_to_CE(site) for site in PU_locations[s]]) & secondary_locations
                    
                    ## we should add all sites that hold the secondary input if any
                    ### given that we have the secondary location available, it is not necessary to use the add-hoc list
                    ##secondary_locations = list(set(PU_overflow[campaign]['sites']) & set( SI.sites_ready ))
                    ## intersect with the sites that are allowed from the request requirement

                    secondary_locations = secondary_locations & set(sites_allowed)
                else:
                    print "Agents know that the secondary is at",better_secondary_locations
                    secondary_locations = set(better_secondary_locations) & set(memory_allowed)

                print "Using good locations allowed",secondary_locations

                ends = ['_0','StepOneProc','Production', 
                        #'_1' ## overflow the reco too ...
                        ]
                if UC.get('PU_overflow_overflow_reco') or options.augment:
                    ends.append('_1')

                if any([task.pathName.endswith(finish) for finish in ends]) :
                    needs, task_name, running, idled = needs_action(wfi, task)
                    ## removing the ones in the site whitelist already since they encode the primary input location
                    if stay_within_site_whitelist:
                        original_site_in_use = set(wfi.request['SiteWhitelist'] & set(secondary_locations))
                    else:
                        original_site_in_use = set(secondary_locations)

                    mode = 'AddWhitelist'
                    if not prim and i_task==0:
                        print "because there isn't any input, one should be able to just replace the sitewhitelist instead of adding, with the restriction of not reaching every possible sites"
                        mode='ReplaceSiteWhitelist'

                    ## remove the sites that have already running jobs
                    gmon = wfi.getGlideMon()
                    if gmon and task_name in gmon and 'Sites' in gmon[task_name] and mode=='AddWhitelist':
                        site_in_use = set(gmon[task_name]['Sites'])
                        site_in_use = set([]) ## at this time I cannot find a reason to apply such limitation
                        print "removing",sorted(site_in_use)
                        ## that determines where you want to run in addition
                        augment_by = list((set(secondary_locations)- site_in_use) & original_site_in_use)
                    else:
                        print "no existing running site"
                        augment_by = list(original_site_in_use)

                    if not augment_by: print "Nowhere to extend to"

                    needs_overide = overide_from_agent( wfi, needs_overide)
                    if augment_by and (needs or needs_overide or force) and PU_overflow[campaign]['pending'] < PU_overflow[campaign]['max']:
                        PU_overflow[campaign]['pending'] += idled
                        print "raising overflow to",PU_overflow[campaign]['pending'],"for",PU_overflow[campaign]['max']
                        ## the step with an input ought to be the digi part : make this one go anywhere
                        modifications[wfo.name][task.pathName] = { mode : augment_by , "Running" : running, "Pending" : idled, "Priority" : wfi.request['RequestPriority']}
                        altered_tasks.add( task.pathName )
                        wfi.sendLog('equalizor','%s of %s is running %d and pending %d, taking action : %s \n %s'%( task_name, wfo.name,
                                                                                                                    running, idled,
                                                                                                                    mode,
                                                                                                                    json.dumps( sorted(augment_by), indent=2 )))
                    else:
                        print task_name,"of",wfo.name,"running",running,"and pending",idled

            ### overflow the skims back to multi-core 
            if campaign in ['Run2015D','Run2015C_25ns'] and task.taskType =='Skim':
                original_swl = wfi.request['SiteWhitelist']
                needs, task_name, running, idled = needs_action(wfi, task)
                if (needs or needs_overide):
                    modifications[wfo.name][task.pathName] = { 'AddWhitelist' : original_swl, 
                                                               "Running" : running, "Pending" : idled, "Priority" : wfi.request['RequestPriority']}
                    altered_tasks.add( task.pathName )
                    wfi.sendLog('equalizor','%s of %s is running %d and pending %d, taking action : AddWhitelist \n %s'%( task_name, wfo.name,
                                                                                                                              running, idled,
                                                                                                                          json.dumps( sorted(original_swl), indent=2 )))


            if options.augment:
                #print "uhm ....",sorted(wfi.request['SiteWhitelist']),i_task,use_HLT
                pass

            ### this is a hack when we need to kick gensim out of everything
            if campaign in [
                #'RunIIWinter15GS',
                #'RunIISummer15GS',
                #'RunIISummer15wmLHEGS',
                #'Summer12',
                ] and task.taskType in ['Production'] and is_chain:
                #what are the site you want to take out. What are the jobs in whitelist, make the diff and replace
                t1s = set([site for site in SI.all_sites if site.startswith('T1')])
                ust2s = set([site for site in SI.all_sites if site.startswith('T2_US')])
                #ust2s = set([site for site in SI.sites_mcore_ready if site.startswith('T2_US')])
                allmcores = set(SI.sites_mcore_ready)
                #set_for = set(wfi.request['SiteWhitelist']) - t1s
                #set_for = set(wfi.request['SiteWhitelist']) - t1s - ust2s
                #set_for = set(wfi.request['SiteWhitelist']) - allmcores
                set_for = set(wfi.request['SiteWhitelist']) & t1s
                print wfo.name,"going for",set_for
                print task.pathName
                if set_for:
                    modifications[wfo.name][task.pathName] = { "ReplaceSiteWhitelist" : sorted(set_for) }
                



            ### add the HLT at partner of CERN
            if 'T2_CH_CERN' in wfi.request['SiteWhitelist'] and i_task in [0,1] and use_HLT and not wfi.request['TrustSitelists']:
                needs, task_name, running, idled = needs_action(wfi, task)
                if options.augment: needs=True
                needs = True
                ##needs = random.random()<0.40 remove the random, just add up to a limit
                if (needs or needs_overide) and pending_HLT < max_HLT:
                    pending_HLT += idled
                    if task.pathName in modifications[wfo.name] and 'AddWhitelist' in modifications[wfo.name][task.pathName]:
                        modifications[wfo.name][task.pathName]["AddWhitelist"].append( "T2_CH_CERN_HLT" )
                        wfi.sendLog('equalizor','also adding the HLT in whitelist of %s to %d for %d'%( task.pathName, pending_HLT, max_HLT))

                    ## this Replace does not work at all for HLT
                    elif task.pathName in modifications[wfo.name] and 'ReplaceSiteWhitelist' in modifications[wfo.name][task.pathName]:
                        #modifications[wfo.name][task.pathName]["ReplaceSiteWhitelist"].append( "T2_CH_CERN_HLT" )
                        #print "\t",wfo.name,"adding replace HLT up to",pending_HLT,"for",max_HLT
                        print "already having a site replacement, not adding the HLT for now"
                        pass
                    else:
                        modifications[wfo.name][task.pathName] = { "AddWhitelist" : ["T2_CH_CERN_HLT"],
                                                                   "Priority" : wfi.request['RequestPriority'],
                                                                   "Running" : running,
                                                                   "Pending" : idled}
                        wfi.sendLog('equalizor','adding the HLT in whitelist of %s to %d for %d'%( task.pathName, pending_HLT, max_HLT))

            if i_task==0 and not sec and use_T0 and False: 
                needs, task_name, running, idled = needs_action(wfi, task)
                
                if options.augment: needs=True
                #needs = True
                good_type = wfi.request['RequestType'] in ['MonteCarlo','MonteCarloFromGEN'] 
                read_lhe = ((not 'LheInputFiles' in wfi.request) or bool(wfi.request['LheInputFiles']))
                good_type &= not read_lhe
                if not good_type and not options.augment: needs = False
                
                ##needs = random.random()<0.40 remove the random, just add up to a limit
                if (needs or needs_overide):
                    pending_T0 += idled
                    if task.pathName in modifications[wfo.name] and 'AddWhitelist' in modifications[wfo.name][task.pathName]:
                        if not "T0_CH_CERN" in modifications[wfo.name][task.pathName]["AddWhitelist"]:
                            modifications[wfo.name][task.pathName]["AddWhitelist"].append( "T0_CH_CERN" )
                            wfi.sendLog('equalizor','adding the T0 for %s to %d for %d'%( task.pathName, pending_T0, max_T0))
                    elif task.pathName in modifications[wfo.name] and 'ReplaceSiteWhitelist' in modifications[wfo.name][task.pathName]:
                        if not "T0_CH_CERN" in modifications[wfo.name][task.pathName]["ReplaceSiteWhitelist"]:
                            modifications[wfo.name][task.pathName]["ReplaceSiteWhitelist"].append( "T0_CH_CERN" )
                            wfi.sendLog('equalizor','adding the T0 to replacement for %s to %d for %d'%( task.pathName, pending_T0, max_T0))
                    else:
                        modifications[wfo.name][task.pathName] = { "AddWhitelist" : ["T0_CH_CERN"],
                                                                   "Priority" : wfi.request['RequestPriority'],
                                                                   "Running" : running,
                                                                   "Pending" : idled}
                        wfi.sendLog('equalizor','adding the T0 for %s to %d for %d'%( task.pathName, pending_T0, max_T0))
            if options.manual and options.manual.count(':')==2:
                manual_task,a_sites,r_sites = options.manual.split(':')
                if manual_task == taskname:

                    if a_sites:
                        print "adding manually",a_sites,"for",task.pathName
                        if task.pathName in modifications[wfo.name] and 'AddWhitelist' in modifications[wfo.name][task.pathName]:
                            modifications[wfo.name][task.pathName]['AddWhitelist'] = list(set(modifications[wfo.name][task.pathName]['AddWhitelist'] +a_sites.split(',')))
                        else:
                            modifications[wfo.name][task.pathName] = {'AddWhitelist' : list(set(a_sites.split(',')))}
                    elif r_sites:
                        print "replacing manually",r_sites,"for",task.pathName
                        if task.pathName in modifications[wfo.name] and 'ReplaceSiteWhitelist' in modifications[wfo.name][task.pathName]:
                            modifications[wfo.name][task.pathName]['ReplaceSiteWhitelist'] = list(set(modifications[wfo.name][task.pathName]['ReplaceSiteWhitelist'] +r_sites.split(',')))
                        else:
                            modifications[wfo.name][task.pathName] = {'ReplaceSiteWhitelist' : list(set(r_sites.split(',')))}                
    ## completely add-hoc
    if options.manual and options.manual.count(':')==3:
        wf,path,a_sites,r_sites = options.manual.split(':')
        if a_sites:
            if path in modifications[wf] and 'AddWhitelist' in modifications[wf][path]:
                modifications[wf][path]['AddWhitelist'] = list(set(modifications[wf][path]['AddWhitelist'] +a_sites.split(',')))
            else:
                modifications[wf][path] = {'AddWhitelist' : list(set(a_sites.split(',')))}
        elif r_sites:
            if path in modifications[wf] and 'ReplaceSiteWhitelist' in modifications[wf][path]:
                modifications[wf][path]['ReplaceSiteWhitelist'] = list(set(modifications[wf][path]['ReplaceSiteWhitelist'] +r_sites.split(',')))
            else:
                modifications[wf][path] = {'ReplaceSiteWhitelist' : list(set(r_sites.split(',')))}


    if short_tasks:
        sendLog('equalizor','These tasks are running very short jobs. Shorter than %d [min] \n %s'%(warning_short_time,'\n'.join( ["%s, %.f [mins] observed time at original %d core count"%(a,b,c) for (a,b,c) in sorted( short_tasks )] )), level='critical')

    if long_tasks:
        sendLog('equalizor','These tasks are running very long jobs. Longer than %d [min] per core.\n %s'%(warning_long_time,'\n'.join( ["%s, %.f [h] %.f [mins] observed time at original %d core count"%(a,divmod(b,60)[0], divmod(b,60)[1],c) for (a,b,c) in sorted( long_tasks )] )), level='critical')

    if hungry_tasks:
        sendLog('equalizor','These tasks are running with more memory than usual\n %s'%( '\n'.join( ["%s, %.f [MB] observed concumption at original %d core count"%(a,b,c) for (a,b,c) in sorted( hungry_tasks )] )), level='critical')

    if bad_hungry_tasks:
        sendLog('equalizor','These tasks are configured with more memory than usual\n %s'%( '\n'.join( ["%s, %.f [MB] submitted requirement at original %d core count"%(a,b,c) for (a,b,c) in sorted( bad_hungry_tasks )] )), level='critical')

        
    interface['modifications'].update( modifications )



    ###  manage the number of core and job resizing
    interface['resizing'].update(resizing)

    ### manage the modification of the memory and target time
    new_times = defaultdict(list)
    new_memories = defaultdict(list)
    new_slopes = defaultdict(list)
    new_reads = defaultdict(list)


    for t,o in performance.items():
        if 'time' in o:
            new_times[s_quantize(o['time'], time_quanta)].append( t )
        if 'memory' in o:
            new_memories[s_quantize(o['memory'], mem_quanta)].append( t )
        if 'slope' in o:
            new_slopes[s_quantize(o['slope'], slope_quanta)].append( t )
        if 'read' in o:
            new_reads[s_quantize(o['read'], read_quanta)].append( t )

    interface['time'].update( new_times )
    interface['memory'].update( new_memories )
    interface['slope'].update( new_slopes )
    interface['read'].update( new_reads )

    if options.high_prio:
        toggles = options.high_prio.split(',')
        already_high = interface['highprio']
        remove = set(already_high) & set(toggles) ## the intersection
        add = set(toggles) - remove
        to_set = (set(interface['highprio']) - remove)
        to_set.update( add )
        interface['highprio'] = list(to_set)

    ## catch wf to be held/release
    hold_wf = []
    already_holding = interface['hold'].keys()
    release_wf = list(set(already_holding)-set(hold_wf))+interface['release'].keys() 
    hold_wf = list(set(hold_wf)-set(release_wf))
    for wf in hold_wf:
        interface['hold'].setdefault(wf,"Everywhere")
    for wf in release_wf:
        interface['release'].setdefault(wf,"Everywhere")

    ## catch site to be held/release
    site_drive = json.loads(eosRead('%s/site_drive.json'%base_eos_dir))
    hold_site = site_drive.get('hold',[])
    already_holding = interface['hold_site']
    release_site = list(set(already_holding) - set(hold_site))+interface['release_site']+site_drive.get('release',[])
    hold_site = list(set(hold_site) - set(release_site))
    interface['hold_site'] = hold_site
    interface['release_site'] = release_site

    ## drain agents
    interface['speed_drain'] = list(agent_speed_draining())

    ## close and save
    close( interface )

    eosFile('%s/perf_per_config.json'%base_eos_dir,'w').write( json.dumps( perf_per_config, indent=2)).close()


if __name__ == "__main__":
    url = reqmgr_url
    parser = optparse.OptionParser()
    parser.add_option('-a','--augment',help='add on top of the document', default=False, action='store_true')
    parser.add_option('-r','--remove',help='remove on workflow from the document', default=False, action='store_true')
    parser.add_option('--t0',help="Allow to use T0", default=False, action='store_true')
    parser.add_option('--hlt',help="Allow to use HLT", default=False, action='store_true')
    parser.add_option('--cscs',help="Allow to use CSCS", default=False, action='store_true')
    parser.add_option('--tune',help='Enable performance tuning', default=False, action='store_true')
    parser.add_option('--high_prio',help='Toggle in the list of high priority workflows', default=None)
    parser.add_option('--manual',help='Just add something fully by hand in there', default=None)
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    equalizor(url, spec, options=options)

    if not spec and do_html_in_each_module:
        htmlor()
