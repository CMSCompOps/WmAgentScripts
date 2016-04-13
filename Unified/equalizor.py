#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, getWorkflows, siteInfo, sendEmail, componentInfo, getDatasetPresence, monitor_dir, reqmgr_url
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

def equalizor(url , specific = None, options=None):
    up = componentInfo(mcm=False, soft=['mcm']) 
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
    SI = siteInfo()
    for site in SI.sites_ready:
        region = site.split('_')[1]
        if not region in ['US','DE','IT']: continue
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

    for site in SI.sites_ready:
        region = site.split('_')[1]
        ## fallback to the region, to site with on-going low pressure
        mapping[site] = [fb for fb in SI.sites_ready if any([('_%s_'%(reg) in fb and fb!=site and site_in_depletion(fb))for reg in regions[region]]) ]
    

    use_T0 = False
    if options.augment : use_T0 = True

    use_HLT = False
    if options.augment : use_HLT=True

    if use_HLT:
        mapping['T2_CH_CERN'].append('T2_CH_CERN_HLT')
    #mapping['T2_IT_Legnaro'].append('T1_IT_CNAF')
    for reg in ['IT','DE','UK']:
        mapping['T2_CH_CERN'].extend([fb for fb in SI.sites_ready if '_%s_'%reg in fb])

    for site,fallbacks in mapping.items():
        for fb in fallbacks:
            reversed_mapping[fb].append(site)

    ## this is the fallback mapping
    print json.dumps( mapping, indent=2)
    #print json.dumps( reversed_mapping, indent=2)

    altered_tasks = set()

    def running_idle( wfi , task_name):
        gmon = wfi.getGlideMon()
        #print gmon
        if not gmon: return (0,0)
        if not task_name in gmon: return (0,0)
        return (gmon[task_name]['Running'], gmon[task_name]['Idle'])

    def needs_action( wfi, task, min_idled = 100, pressure = 0.2):
        task_name = task.pathName.split('/')[-1]
        running, idled = running_idle( wfi, task_name)
        go = True
        if not idled and not running : 
            go = False
        if idled < 100: 
            go = False
        if (not running and idled) or (running and (idled / float(running) > pressure)):
            go = True
        else:
            go = False
        return go, task_name, running, idled

    def getcampaign( task ):
        taskname = task.pathName.split('/')[-1]
        if hasattr( task, 'prepID'):
            return task.prepID.split('-')[1]
        elif taskname.count('-')>=1:
            return taskname.split('-')[1]
        else:
            return None

    def close( interface ):
        open('%s/equalizor.json.new'%monitor_dir,'w').write( json.dumps( interface, indent=2))
        os.system('mv %s/equalizor.json.new %s/equalizor.json'%(monitor_dir,monitor_dir))
        os.system('cp %s/equalizor.json %s/logs/equalizor/equalizor.%s.json'%(monitor_dir,monitor_dir,time.mktime(time.gmtime())))

    interface = {
        'reversed_mapping' : reversed_mapping,
        'modifications' : {}
        }
    if options.augment or options.remove:
        interface['modifications'] = json.loads( open('%s/equalizor.json'%monitor_dir).read())['modifications']

    if options.remove:
        if specific in interface['modifications']:
            print "poping",specific
            interface['modifications'].pop(specific)
            close( interface )
        return 


    PU_locations = {}
    PU_overflow = {
        #'RunIISpring15PrePremix' : { 
        #    'sites' : ["T1_US_FNAL", "T1_DE_KIT" , "T1_IT_CNAF", "T1_RU_JINR" ,"T2_CH_CERN"],
        #    'max' : 20000,
        #    'pending' : 0
        #    },
        'RunIIFall15DR76' : {
            'sites':['T1_ES_PIC','T2_US_Purdue','T2_UK_SGrid_RALPP','T2_BE_IIHE','T2_DE_DESY','T2_IT_Legnaro','T2_US_Caltech','T1_DE_KIT',
                     'T2_UK_London_Brunel','T2_IT_Pisa',
                     'T1_US_FNAL',
                     'T2_IT_Rome','T2_US_Florida','T1_IT_CNAF','T1_RU_JINR','T2_UK_London_IC','T2_US_Nebraska','T2_FR_CCIN2P3','T2_US_UCSD','T2_ES_CIEMAT',
                     'T1_FR_CCIN2P3','T2_US_Wisconsin','T2_US_MIT','T2_DE_RWTH',
                     'T1_UK_RAL','T2_US_Vanderbilt','T2_CH_CERN'],
            'max': 20000,
            'pending' : 0},
        'RunIISpring16DR80' : {
            'sites':['T1_ES_PIC','T2_US_Purdue','T2_UK_SGrid_RALPP','T2_BE_IIHE','T2_DE_DESY','T2_IT_Legnaro','T2_US_Caltech','T1_DE_KIT',
                     'T2_UK_London_Brunel','T2_IT_Pisa',
                     'T1_US_FNAL',
                     'T2_IT_Rome','T2_US_Florida','T1_IT_CNAF','T1_RU_JINR','T2_UK_London_IC','T2_US_Nebraska','T2_FR_CCIN2P3','T2_US_UCSD','T2_ES_CIEMAT',
                     'T1_FR_CCIN2P3','T2_US_Wisconsin','T2_US_MIT','T2_DE_RWTH',
                     'T1_UK_RAL','T2_US_Vanderbilt','T2_CH_CERN'],
            'max': 20000,
            'pending' : 0,
            'force' : True},
        'RunIISpring15DR74' : {
            'sites' : ['T1_ES_PIC','T1_DE_KIT','T1_US_FNAL','T1_IT_CNAF','T1_RU_JINR','T1_FR_CCIN2P3','T1_UK_RAL','T2_CH_CERN'],
            'max' : 20000,
            'pending' : 0}
        }
    
    set_to = SI.sites_AAA
    LHE_overflow = {
        'RunIIWinter15GS' : set_to,
        'RunIISummer15GS' : set_to,
        'Summer12' : set_to,
        'Summer11Leg' : set_to
        #'RunIIFall15MiniAODv2' : set_to,
        }

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

    t0_special = [
        'vlimant_BPH-RunIISummer15GS-00030_00212_v0__160129_135314_9755',
        'pdmvserv_TSG-RunIISummer15GS-00044_00240_v0__160210_121223_8582'
        ]
    no_routing = [ 
        #'vlimant_BPH-RunIISummer15GS-00030_00212_v0__160129_135314_9755',
        #'pdmvserv_TOP-RunIIWinter15GS-00074_00187_v0__160207_162312_1992',
                   ]

    stay_within_site_whitelist = False
    specific_task=None
    if specific and ":" in specific:
        specific,specific_task = specific.split(':')

    if specific:
        wfs = session.query(Workflow).filter(Workflow.name.contains(specific)).all()
    else:
        wfs = session.query(Workflow).filter(Workflow.status == 'away').all()
        
    random.shuffle( wfs )
    for wfo in wfs:
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
        
        ## only running should get re-routed
        if not wfi.request['RequestStatus'] in ['running-open','running-closed'] and not specific: continue

        tasks_and_campaigns = []
        for task in wfi.getWorkTasks():
            tasks_and_campaigns.append( (task, getcampaign(task) ) )
        
        _,_,_,sec = wfi.getIO()

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

        ## now parse this for action
        for i_task,(task,campaign) in enumerate(tasks_and_campaigns):
            if options.augment:
                print task.pathName
                print campaign

            ### rule to avoid the issue of taskchain secondary jobs being stuck at sites processing the initial step
            if campaign in LHE_overflow:
                if task.taskType in ['Processing']:
                    needs, task_name, running, idled = needs_action(wfi, task)
                    needs_overide = overide_from_agent( wfi, needs_overide)
                    extend_to = copy.deepcopy( LHE_overflow[campaign] )
                    if stay_within_site_whitelist:
                        extend_to = list(set(extend_to) & set(wfi.request['SiteWhitelist'])) ## restrict to stupid-site-whitelist

                    if extend_to and needs or needs_overide:
                        print "\t",task_name,"of",wfo.name,"running",running,"and pending",idled,"taking action : ReplaceSiteWhitelist"
                        modifications[wfo.name][task.pathName] = { "ReplaceSiteWhitelist" : copy.deepcopy( LHE_overflow[campaign] ) ,"Running" : running, "Pending" : idled, "Priority" : wfi.request['RequestPriority']}
                        #print json.dumps( modifications[wfo.name][task.pathName]['ReplaceSiteWhitelist']
                        altered_tasks.add( task.pathName )
                    else:
                        print task_name,"of",wfo.name,"running",running,"and pending",idled


            ### overflow the 76 digi-reco to the site holding the pileup
            if campaign in PU_overflow:
                force = PU_overflow[campaign]['force'] if 'force' in PU_overflow[campaign] else False
                secondary_locations = set(SI.sites_ready)
                for s in sec:
                    if not s in PU_locations:
                        presence = getDatasetPresence( url, s)
                        #one_secondary_locations = [site for (site,(there,frac)) in presence.items() if there]
                        one_secondary_locations = [site for (site,(there,frac)) in presence.items() if frac>98.]
                        PU_locations[s] = one_secondary_locations
                    print "secondary is at",sorted(PU_locations[s])
                    secondary_locations = set(PU_locations[s]) & secondary_locations
                    
                ## we should add all sites that hold the secondary input if any
                secondary_locations = list(set(PU_overflow[campaign]['sites']) & set( SI.sites_ready ))
                if any([task.pathName.endswith(finish) for finish in ['_0','StepOneProc','Production']]) :
                    needs, task_name, running, idled = needs_action(wfi, task)
                    ## removing the ones in the site whitelist already since they encode the primary input location
                    if stay_within_site_whitelist:
                        original_site_in_use = set(wfi.request['SiteWhitelist'])
                    else:
                        original_site_in_use = set(secondary_locations)
                    ## remove the sites that have already running jobs
                    gmon = wfi.getGlideMon()
                    if gmon and task_name in gmon and 'Sites' in gmon[task_name]:
                        site_in_use = set(gmon[task_name]['Sites'])
                        ## that determines where you want to run in addition
                        #augment_by = list((set(secondary_locations)- site_in_use))
                        augment_by = list((set(secondary_locations)- site_in_use) & original_site_in_use) ## restrict to stupid-site-whitelist
                    else:
                        augment_by = list(original_site_in_use)

                    needs_overide = overide_from_agent( wfi, needs_overide)
                    if augment_by and (needs or needs_overide or force) and PU_overflow[campaign]['pending'] < PU_overflow[campaign]['max']:
                        PU_overflow[campaign]['pending'] += idled
                        print "raising overflow to",PU_overflow[campaign]['pending'],"for",PU_overflow[campaign]['max']
                        ## the step with an input ought to be the digi part : make this one go anywhere
                        modifications[wfo.name][task.pathName] = { "AddWhitelist" : augment_by , "Running" : running, "Pending" : idled, "Priority" : wfi.request['RequestPriority']}
                        altered_tasks.add( task.pathName )
                        print "\t",task_name,"of",wfo.name,"running",running,"and pending",idled,"taking action : AddWhitelist"
                        #print json.dumps( augment_by, indent=2 )
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
                    print "\t",task_name,"of",wfo.name,"running",running,"and pending",idled,"taking action : AddWhitelist"


            if options.augment:
                print sorted(wfi.request['SiteWhitelist']),i_task,use_HLT
            ### add the HLT at partner of CERN
            if 'T2_CH_CERN' in wfi.request['SiteWhitelist'] and i_task==0 and use_HLT:
                needs, task_name, running, idled = needs_action(wfi, task)
                if options.augment: needs=True
                needs = True
                ##needs = random.random()<0.40 remove the random, just add up to a limit
                if (needs or needs_overide) and pending_HLT < max_HLT:
                    pending_HLT += idled
                    if task.pathName in modifications[wfo.name] and 'AddWhitelist' in modifications[wfo.name][task.pathName]:
                        modifications[wfo.name][task.pathName]["AddWhitelist"].append( "T2_CH_CERN_HLT" )
                        print "\t",wfo.name,"adding addHLT up to",pending_HLT,"for",max_HLT
                        print task.pathName
                    ## this Replace does not work at all for HLT
                    #elif task.pathName in modifications[wfo.name] and 'ReplaceSiteWhitelist' in modifications[wfo.name][task.pathName]:
                        #modifications[wfo.name][task.pathName]["ReplaceSiteWhitelist"].append( "T2_CH_CERN_HLT" )
                        #print "\t",wfo.name,"adding replace HLT up to",pending_HLT,"for",max_HLT
                    else:
                        modifications[wfo.name][task.pathName] = { "AddWhitelist" : ["T2_CH_CERN_HLT"],
                                                                   "Priority" : wfi.request['RequestPriority'],
                                                                   "Running" : running,
                                                                   "Pending" : idled}
                        print "\t",wfo.name,"adding HLT up to",pending_HLT,"for",max_HLT
                        print task.pathName

            if i_task==0 and not sec and use_T0:
                needs, task_name, running, idled = needs_action(wfi, task)
                
                if options.augment: needs=True
                #needs = True
                #if not (wfo.name in t0_special) and not options.augment: needs = False
                if not wfi.request['RequestType'] in ['MonteCarlo','MonteCarloFromGEN'] and not options.augment: needs = False
                
                ##needs = random.random()<0.40 remove the random, just add up to a limit
                if (needs or needs_overide):
                    pending_T0 += idled
                    if task.pathName in modifications[wfo.name] and 'AddWhitelist' in modifications[wfo.name][task.pathName]:
                        modifications[wfo.name][task.pathName]["AddWhitelist"].append( "T0_CH_CERN" )
                        print "\t",wfo.name,"adding addT0 up to",pending_T0,"for",max_T0
                        print task.pathName
                    elif task.pathName in modifications[wfo.name] and 'ReplaceSiteWhitelist' in modifications[wfo.name][task.pathName]:
                        modifications[wfo.name][task.pathName]["ReplaceSiteWhitelist"].append( "T0_CH_CERN" )
                        print "\t",wfo.name,"adding replace T0 up to",pending_T0,"for",max_T0
                    else:
                        modifications[wfo.name][task.pathName] = { "AddWhitelist" : ["T0_CH_CERN"],
                                                                   "Priority" : wfi.request['RequestPriority'],
                                                                   "Running" : running,
                                                                   "Pending" : idled}
                        print "\t",wfo.name,"adding T0 up to",pending_T0,"for",max_T0
                        print task.pathName


    interface['modifications'].update( modifications )


    ## temporary core managing
    interface['cores']={'T2_CH_CERN_HLT': {'min':4,'max':16}, 'default': {'min':1, 'max':4}}
    #interface['max_cores']={'T2_CH_CERN_HLT': 16, 'default': 4}
    #interface['min_cores']={'T2_CH_CERN_HLT': 4, 'default': 1}
    #interface['resize_subtasks'] = 'RunIISpring16DR80'
    interface['resizes'] = ['RunIISpring16DR80','NotACampaign']

    ## close and save
    close( interface )


if __name__ == "__main__":
    url = reqmgr_url
    parser = optparse.OptionParser()
    parser.add_option('-a','--augment',help='add on top of the document', default=False, action='store_true')
    parser.add_option('-r','--remove',help='remove on workflow from the document', default=False, action='store_true')
    #parser.add_option('--t0',help="Allow to use T0
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    equalizor(url, spec, options=options)

    if not spec:
        htmlor()
