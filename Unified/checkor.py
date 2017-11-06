#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, workflowInfo, getDatasetEventsAndLumis, findCustodialLocation, getDatasetEventsPerLumi, siteInfo, getDatasetPresence, campaignInfo, getWorkflowById, forceComplete, makeReplicaRequest, getDatasetSize, getDatasetFiles, sendLog, reqmgr_url, dbs_url, dbs_url_writer, getForceCompletes
from utils import componentInfo, unifiedConfiguration, userLock, duplicateLock, dataCache, unified_url, getDatasetLumisAndFiles, getDatasetRuns, duplicateAnalyzer, invalidateFiles
import phedexClient
import dbs3Client
dbs3Client.dbs3_url = dbs_url
dbs3Client.dbs3_url_writer = dbs_url_writer
import reqMgrClient
import json
from collections import defaultdict
import optparse
import os
import copy
import time
import random
import math
from McMClient import McMClient
from htmlor import htmlor
from utils import sendEmail 
from utils import closeoutInfo
from showError import parse_one
#import csv 

def checkor(url, spec=None, options=None):
    if userLock():   return
    if duplicateLock() and not options.go:  return


    
    fDB = closeoutInfo()

    UC = unifiedConfiguration()
    use_recoveror = UC.get('use_recoveror')
    #if UC.get('step_checkor_down') and not options.go: return 
    #if not options.go: return ## disable checkor until further notice
    
    use_mcm = True
    up = componentInfo(mcm=use_mcm, soft=['mcm'])
    if not up.check(): return
    use_mcm = up.status['mcm']

    now_s = time.mktime(time.gmtime())
    def time_point(label="",sub_lap=False, percent=None):
        now = time.mktime(time.gmtime())
        nows = time.asctime(time.gmtime())

        print "[checkor] Time check (%s) point at : %s"%(label, nows)
        if percent:
            print "[checkor] finishing in about %.2f [s]" %( (now - time_point.start) / percent )
        print "[checkor] Since start: %s [s]"% ( now - time_point.start)
        if sub_lap:
            print "[checkor] Sub Lap : %s [s]"% ( now - time_point.sub_lap ) 
            time_point.sub_lap = now
        else:
            print "[checkor] Lap : %s [s]"% ( now - time_point.lap ) 
            time_point.lap = now            
            time_point.sub_lap = now

    time_point.sub_lap = time_point.lap = time_point.start = time.mktime(time.gmtime())
    
    runnings = session.query(Workflow).filter(Workflow.status == 'away').all()
    standings = session.query(Workflow).filter(Workflow.status.startswith('assistance')).all()

    ## intersect with what is actually in completed status in request manager now
    all_completed = set(getWorkflows(url, 'completed' ))

    wfs=[]
    exceptions=[
        ]
    #for wfn in exceptions:
    #    wfs.extend( session.query(Workflow).filter(Workflow.name == wfn).all() )

    if options.strict:
        ## the one which were running and now have completed
        print "strict option is on: checking workflows that freshly completed"
        wfs.extend( filter(lambda wfo: wfo.name in all_completed , runnings))
    if options.update:
        print "update option is on: checking workflows that have not completed yet"
        wfs.extend( filter(lambda wfo: not wfo.name in all_completed , runnings))

    if options.clear:
        print "clear option is on: checking workflows that are ready to toggle closed-out"
        wfs.extend( filter(lambda wfo: 'custodial' in wfo.status, standings))
    if options.review:
        print "review option is on: checking the workflows that needed intervention"
        wfs.extend( filter(lambda wfo: not 'custodial' in wfo.status, standings))

    ## what is left out are the wf which were running and ended up aborted/failed/...




    custodials = defaultdict(list) #sites : dataset list
    transfers = defaultdict(list) #sites : dataset list
    invalidations = [] #a list of files
    SI = siteInfo()
    CI = campaignInfo()
    mcm = McMClient(dev=False) if use_mcm else None

    def get_campaign(output, wfi):
        ## this should be a perfect matching of output->task->campaign
        campaign = None
        era = None
        wf_campaign = None
        if 'Campaign' in wfi.request:   wf_campaign = wfi.request['Campaign']
        try:
            era = output.split('/')[2].split('-')[0]
        except:
            era = None
            
        if wfi.isRelval(): 
            campaign = wf_campaign
        else:
            campaign = era if era else wf_campaign
        return campaign

    ## retrieve bypass and onhold configuration
    bypasses = []
    forcings = []
    overrides = getForceCompletes()
    holdings = []

    
    actors = UC.get('allowed_bypass')

    for bypassor,email in actors:
        bypass_file = '/afs/cern.ch/user/%s/%s/public/ops/bypass.json'%(bypassor[0],bypassor)
        if not os.path.isfile(bypass_file):
            #sendLog('checkor','no file %s',bypass_file)
            continue
        try:
            print "Can read bypass from", bypassor
            extending = json.loads(open(bypass_file).read())
            print bypassor,"is bypassing",json.dumps(sorted(extending))
            bypasses.extend( extending )
        except:
            sendLog('checkor',"cannot get by-passes from %s for %s"%(bypass_file ,bypassor))
            sendEmail("malformated by-pass information","%s is not json readable"%(bypass_file), destination=[email])
        
        holding_file = '/afs/cern.ch/user/%s/%s/public/ops/onhold.json'%(bypassor[0],bypassor)
        if not os.path.isfile(holding_file):
            #sendLog('checkor',"no file %s"%holding_file)
            continue
        try:
            extending = json.loads(open(holding_file).read())
            print bypassor,"is holding",json.dumps(sorted(extending))
            holdings.extend( extending )
        except:
            sendLog('checkor',"cannot get holdings from %s for %s"%(holding_file, bypassor))
            sendEmail("malformated by-pass information","%s is not json readable"%(holding_file), destination=[email])

    unhold= ['fabozzi_Run2016B-v2-Tau-07Aug17_ver2_8029_170831_201310_7397']
    for unh in unhold:
        if unh in holdings:
            holdings.remove(unh)

    
    for rider,extending in overrides.items():
        print rider,"bypasses by forcecompleting",json.dumps(sorted(extending))
        bypasses.extend( extending )

    ## once this was force-completed, you want to bypass
    #for rider,email in actors:
    #    rider_file = '/afs/cern.ch/user/%s/%s/public/ops/forcecomplete.json'%(rider[0],rider)
    #    if not os.path.isfile(rider_file):
    #        print "no file",rider_file
    #        #sendLog('checkor',"no file %s"%rider_file)
    #        continue
    #    try:
    #        extending = json.loads(open( rider_file ).read() )
    #        print rider,"is force completing",json.dumps(sorted(extending))
    #        bypasses.extend( extending )
    #    except:
    #        sendLog('checkor',"cannot get force complete list from %s"%rider)
    #        sendEmail("malformated force complet file","%s is not json readable"%rider_file, destination=[email])

    if use_mcm:
        ## this is a list of prepids that are good to complete
        forcings = mcm.get('/restapi/requests/forcecomplete')
    
    
    ## remove empty entries ...
    bypasses = filter(None, bypasses)

    pattern_fraction_pass = UC.get('pattern_fraction_pass')

    total_running_time = 1.*60. 
    sleep_time = 1
    will_do_that_many = len(wfs)
    if len(wfs):
        sleep_time = min(max(0.5, total_running_time / will_do_that_many), 10)

    random.shuffle( wfs )

    in_manual = 0

    ## now you have a record of what file was invalidated globally from TT
    TMDB_invalid = dataCache.get('file_invalidation') 

    print len(wfs),"to consider, pausing for",sleep_time
    max_per_round = UC.get('max_per_round').get('checkor',None)
    if options.limit: max_per_round=options.limit
    if max_per_round and not spec: wfs = wfs[:max_per_round]
    
    ## record all evolution
    full_picture = defaultdict(dict)
    
    for iwfo,wfo in enumerate(wfs):
        if spec and not (spec in wfo.name): continue
        
        time.sleep( sleep_time )
        
        time_point("Starting checkor with with %s Progress [%d/%d]"% (wfo.name, iwfo, will_do_that_many), percent = float(iwfo)/will_do_that_many)

        ## get info
        wfi = workflowInfo(url, wfo.name)
        wfi.sendLog('checkor',"checking on %s %s"%( wfo.name,wfo.status))
        ## make sure the wm status is up to date.
        # and send things back/forward if necessary.
        wfo.wm_status = wfi.request['RequestStatus']

        if wfo.wm_status == 'closed-out' and not wfo.name in exceptions:
            ## manually closed-out
            wfi.sendLog('checkor',"%s is already %s, setting close"%( wfo.name , wfo.wm_status))
            wfo.status = 'close'
            session.commit()
            continue

        elif wfo.wm_status in ['failed','aborted','aborted-archived','rejected','rejected-archived','aborted-completed']:
            ## went into trouble
            if wfi.isRelval():
                wfi.sendLog('checkor',"%s is %s, but will not be set in trouble to find a replacement."%( wfo.name, wfo.wm_status))
                wfo.status = 'forget'
            else:
                wfo.status = 'trouble'
                wfi.sendLog('checkor',"%s is in trouble %s"%(wfo.name, wfo.wm_status))
            session.commit()
            continue
        elif wfo.wm_status in ['assigned','acquired']:
            ## not worth checking yet
            wfi.sendLog('checkor',"%s is not running yet"%wfo.name)
            session.commit()
            continue
        
        if wfo.wm_status != 'completed' and not wfo.name in exceptions:
            ## for sure move on with closeout check if in completed
            wfi.sendLog('checkor',"no need to check on %s in status %s"%(wfo.name, wfo.wm_status))
            session.commit()
            continue


        session.commit()        
        #sub_assistance="" # if that string is filled, there will be need for manual assistance
        existing_assistance_tags = set(wfo.status.split('-')[1:]) #[0] should be assistance
        assistance_tags = set()

        is_closing = True

        ## get it from somewhere
        bypass_checks = False

        for bypass in bypasses:
            #if bypass and bypass in wfo.name:
            if bypass == wfo.name:
                wfi.sendLog('checkor',"we can bypass checks on %s because of keyword %s "%( wfo.name, bypass))
                bypass_checks = True
                break
        pids = wfi.getPrepIDs()
        force_by_mcm = False
        force_by_user = False
        for force in forcings:
            if force in pids:
                wfi.sendLog('checkor',"we can bypass checks and force complete %s because of prepid %s "%( wfo.name, force))
                bypass_checks = True
                force_by_mcm = True
                break
        for user in overrides:
            for force in overrides[user]:
                if force in wfo.name:
                    wfi.sendLog('checkor',"we can bypass checks and force complete %s because of keyword %s of user %s"%( wfo.name, force, user))
                    bypass_checks = True
                    force_by_user = True
                    break
        
        if '-onhold' in wfo.status:
            if wfo.name in holdings and not bypass_checks:
                wfi.sendLog('checkor',"%s is on hold"%wfo.name)
                continue

        if wfo.name in holdings and not bypass_checks:
            wfo.status = 'assistance-onhold'
            wfi.sendLog('checkor',"setting %s on hold"%wfo.name)
            session.commit()
            continue

        tiers_with_no_check = copy.deepcopy(UC.get('tiers_with_no_check')) # dqm*
        vetoed_custodial_tier = copy.deepcopy(UC.get('tiers_with_no_custodial')) if not wfi.isRelval() else [] #no veto for relvals
        to_ddm_tier = copy.deepcopy(UC.get('tiers_to_DDM'))
        campaigns = {} ## this mapping of campaign per output dataset assumes era==campaing, which is not true for relval
        expected_outputs = copy.deepcopy( wfi.request['OutputDatasets'] )

        ### NEEDS A BUG FIX : find campaign per dataset
        ## probably best to get outpuut per task, then campaign per task
        
        for out in wfi.request['OutputDatasets']:
            c = get_campaign(out, wfi)
            campaigns[out] = c 
        wf_campaigns = wfi.getCampaigns()
        ## override the previous if there is only one campaign in the workflow
        if len(wf_campaigns)==1:
            for out in campaigns:
                campaigns[out] = wf_campaigns[0]

        for out,c in campaigns.items():
            if c in CI.campaigns and 'custodial_override' in CI.campaigns[c]:
                vetoed_custodial_tier = list(set(vetoed_custodial_tier) - set(CI.campaigns[c]['custodial_override']))
                ## add those that we need to check for custodial copy
                tiers_with_no_check = list(set(tiers_with_no_check) - set(CI.campaigns[c]['custodial_override'])) ## would remove DQM from the vetoed check

        print campaigns

        check_output_text = "Initial outputs:"+",".join(sorted(wfi.request['OutputDatasets'] ))
        wfi.request['OutputDatasets'] = [ out for out in wfi.request['OutputDatasets'] if not any([out.split('/')[-1] == veto_tier for veto_tier in tiers_with_no_check])]
        check_output_text += "\nWill check on:"+",".join(sorted(wfi.request['OutputDatasets'] ))
        check_output_text += "\ntiers out:"+",".join( sorted(tiers_with_no_check ))
        check_output_text += "\ntiers no custodial:"+",".join( sorted(vetoed_custodial_tier) )

        wfi.sendLog('checkor', check_output_text )

        ## anything running on acdc : getting the real prepid is not worth it
        familly = getWorkflowById(url, wfi.request['PrepID'], details=True)
        acdc = []
        acdc_inactive = []
        forced_already=False
        acdc_bads = []
        true_familly = []
        for member in familly:
            if member['RequestType'] != 'Resubmission': continue
            if member['RequestName'] == wfo.name: continue
            if member['RequestDate'] < wfi.request['RequestDate']: continue
            if member['PrepID'] != wfi.request['PrepID'] : continue
            #if 'OriginalRequestName' in member and (not 'ACDC' in member['OriginalRequestName']) and member['OriginalRequestName'] != wfo.name: continue
            if member['RequestStatus'] == None: continue

            if not set(member['OutputDatasets']).issubset( set(expected_outputs)):
                if not member['RequestStatus'] in ['rejected-archived','rejected','aborted','aborted-archived']:
                    ##this is not good at all
                    wfi.sendLog('checkor','inconsistent ACDC %s'%member['RequestName'] )
                    #sendLog('checkor','inconsistent ACDC %s'%member['RequestName'], level='critical')
                    acdc_bads.append( member['RequestName'] )
                    is_closing = False
                    assistance_tags.add('manual')
                continue

            true_familly.append( member['RequestName'] )
            #try:
            #    parse_one(url, member['RequestName'])
            #except:
            #    print "Could not make error report for",member['RequestName']

            if member['RequestStatus'] in ['running-open','running-closed','assigned','acquired']:
                print wfo.name,"still has an ACDC running",member['RequestName']
                acdc.append( member['RequestName'] )
                ## cannot be bypassed!
                is_closing = False
                assistance_tags.add('recovering')
                if (force_by_mcm or force_by_user) and not forced_already:
                    wfi.sendLog('checkor','%s is being forced completed while recovering'%wfo.name)
                    wfi.notifyRequestor("The workflow %s was force completed"% wfo.name, do_batch=False)
                    forceComplete(url, wfi)
                    forced_already=True
            else:
                acdc_inactive.append( member['RequestName'] )
                assistance_tags.add('recovered')
        if acdc_bads:
            #sendEmail('inconsistent ACDC','for %s, ACDC %s is inconsistent, preventing from closing'%( wfo.name, ','.join(acdc_bads) ))
            sendLog('checkor','For %s, ACDC %s is inconsistent, preventing from closing or will create a mess.'%( wfo.name, ','.join(acdc_bads) ), level='critical')

        time_point("checked workflow familly", sub_lap=True)


        ## completion check
        percent_completions = {}
        percent_avg_completions = {}
        fractions_pass = {}
        fractions_announce = {}
        fractions_truncate_recovery = {}
        events_per_lumi = {}

        over_100_pass = True
        (lhe,prim,_,_) = wfi.getIO()
        if lhe or prim: over_100_pass = False

        ## this will create funky issue with LHEGS where the two output of a task can have different expected #of events ... as predicted this is a major complication
        event_expected_per_task = {} 
        output_per_task = wfi.getOutputPerTask()
        task_outputs = {}
        for task,outs in output_per_task.items():
            for out in outs:
                task_outputs[out] = task


        ## lumi_expected is constant over all tasks
        ## event_expected is only valid for the "first task" and one needs to consider all efficiency on the way
        event_expected,lumi_expected = wfi.request.get('TotalInputEvents',None), wfi.request.get('TotalInputLumis', None)

        if event_expected == None:
            sendEmail("missing member of the request","TotalInputEvents is missing from the workload of %s"% wfo.name)
            sendLog('checkor',"TotalInputEvents is missing from the workload of %s"% wfo.name, level='critical')
            event_expected = 0 

        ttype = 'Task' if 'TaskChain' in wfi.request else 'Step'
        it = 1
        tname_dict = {}
        while True:
            tt = '%s%d'%(ttype,it)
            it+=1
            if tt in wfi.request:
                tname = wfi.request[tt]['%sName'% ttype]
                tname_dict[tname] = tt
                if not 'Input%s'%ttype in wfi.request[tt] and 'RequestNumEvents' in wfi.request[tt]: 
                    ## pick up the value provided by the requester, that will work even if the filter effiency is broken
                    event_expected = wfi.request[tt]['RequestNumEvents']
            else:
                break

        if '%sChain'%ttype in wfi.request:
            ## go on and make the accounting
            it = 1
            while True:
                tt = '%s%d'%(ttype,it)
                it+=1
                if tt in wfi.request:
                    tname = wfi.request[tt]['%sName'% ttype]
                    event_expected_per_task[tname] = event_expected
                    ### then go back up all the way to the root task to count filter-efficiency
                    a_task = wfi.request[tt]
                    while 'Input%s'%ttype in a_task:
                        event_expected_per_task[tname] *= a_task.get('FilterEfficiency',1)
                        mother_task = a_task['Input%s'%ttype]
                        ## go up
                        a_task = wfi.request[ tname_dict[mother_task] ]
                else:
                    break

        time_point("expected statistics", sub_lap=True)

        default_fraction_overdoing = UC.get('default_fraction_overdoing')
        for output in wfi.request['OutputDatasets']:
            default_pass = UC.get('default_fraction_pass')
            fractions_pass[output] = default_pass
            fractions_announce[output] = 1.0
            #fractions_truncate_recovery[output] = 0.98 ## above this threshold if the request will pass stats check, we close the acdc
            c = campaigns[output]
            if c in CI.campaigns and 'earlyannounce' in CI.campaigns[c]:
                wfi.sendLog('checkor', "Allowed to announce the output %s over %.2f by campaign requirement"%(out, CI.campaigns[c]['earlyannounce']))
                fractions_announce[output] = CI.campaigns[c]['earlyannounce']

            if c in CI.campaigns and 'fractionpass' in CI.campaigns[c]:
                if type(CI.campaigns[c]['fractionpass']) == dict:
                    tier = output.split('/')[-1]
                    priority = str(wfi.request['RequestPriority'])
                    ## defined per tier
                    fractions_pass[output] = CI.campaigns[c]['fractionpass'].get('all', default_pass)
                    if tier in CI.campaigns[c]['fractionpass']:
                        tier_pass_content = CI.campaigns[c]['fractionpass'][tier]
                        if type(tier_pass_content) == dict:
                            fractions_pass[output] = CI.campaigns[c]['fractionpass'][tier].get('all', default_pass)
                            for exp,pass_exp in CI.campaigns[c]['fractionpass'][tier].items():
                                if output.startswith(exp):
                                    fractions_pass[output] = pass_exp
                        else:
                            fractions_pass[output] = CI.campaigns[c]['fractionpass'][tier]
                    if priority in CI.campaigns[c]['fractionpass']:
                        fractions_pass[output] = CI.campaigns[c]['fractionpass'][priority]
                else:
                    fractions_pass[output] = CI.campaigns[c]['fractionpass']
                wfi.sendLog('checkor', "overriding fraction to %s for %s by campaign requirement"%( fractions_pass[output], output))

            if options.fractionpass:
                fractions_pass[output] = options.fractionpass
                print "overriding fraction to",fractions_pass[output],"by command line for",output

            for key in pattern_fraction_pass:
                if key in output:
                    fractions_pass[output] = pattern_fraction_pass[key]
                    print "overriding fraction to",fractions_pass[output],"by dataset key",key

            pass_percent_below = fractions_pass[output]-0.02
            completed_log = filter(lambda change : change["Status"] in ["completed"],wfi.request['RequestTransition'])
            delay = (now_s - completed_log[-1]['UpdateTime']) / (60.*60.*24.) if completed_log else 0 ## in days
            print delay,"since completed"
            weight_full = 7.
            weight_pass = delay
            weight_under_pass = 2*delay if int(wfi.request['RequestPriority'])< 80000 else 0. ## allow to drive it below the threshold
            weight_under_pass = 0. ## otherwise we can end-up having request at 94% waiting for 95%
            fractions_truncate_recovery[output] = (fractions_pass[output]*weight_pass +1.*weight_full + pass_percent_below*weight_under_pass) / ( weight_pass+weight_full+weight_under_pass)

            if c in CI.campaigns and 'truncaterecovery' in CI.campaigns[c]:
                wfi.sendLog('checkor', "Allowed to truncate recovery of %s over %.2f by campaign requirement"%(out, CI.campaigns[c]['truncaterecovery']))            
                fractions_truncate_recovery[output] = CI.campaigns[c]['truncaterecovery']
            else:
                wfi.sendLog('checkor', "Can truncate recovery of %s over %.2f"%(out, fractions_truncate_recovery[output]))

            if fractions_truncate_recovery[output] < fractions_pass[output]:
                print "This is not going to end well if you truncate at a lower threshold than passing",fractions_truncate_recovery[output],fractions_pass[output]
                ## floor truncating
                fractions_truncate_recovery[output] = fractions_pass[output]
                ##### OR 
                ##wfi.sendLog('checkor', "Lowering the pass bar since recovery is being truncated")
                ## fractions_pass[output] = fractions_truncate_recovery[output]

        time_point("statistics thresholds", sub_lap=True)

        for output in wfi.request['OutputDatasets']:
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=output)
            events_per_lumi[output] = event_count/float(lumi_count) if lumi_count else 100
            percent_completions[output] = 0.

            if lumi_expected:
                wfi.sendLog('checkor', "lumi completion %s expected %d for %s"%( lumi_count, lumi_expected, output))
                percent_completions[output] = lumi_count / float( lumi_expected )


            output_event_expected = event_expected_per_task.get(task_outputs.get(output,'NoTaskFound'), 0)
            if output_event_expected:
                e_fraction = float(event_count) / float( output_event_expected )
                if e_fraction > percent_completions[output]:
                    percent_completions[output] = e_fraction
                    wfi.sendLog('checkor', "overiding : event completion real %s expected %s for %s"%(event_count, output_event_expected, output))


            percent_avg_completions[output] = percent_completions[output]
        time_point("observed statistics", sub_lap=True)
                    
        pass_stats_check = dict([(out, bypass_checks or (percent_completions[out] >= fractions_pass[out])) for out in fractions_pass ])

        lumis_per_run = {} # a dict of dict run:[lumis]
        files_per_rl = {} # a dict of dict "run:lumi":[files]
        fetched = dict([(out,False) for out in pass_stats_check])

        blocks = wfi.getBlockWhiteList()
        rwl = wfi.getRunWhiteList()
        lwl = wfi.getLumiWhiteList()


        ## need to come to a way to do this "fast" so that it can be done more often
        if not all(pass_stats_check.values()):# and False:
            n_runs = 1
            ## should recalculate a couple of things to be able to make a better check on expected fraction
            for p in prim:
                nr = getDatasetRuns(p)
                if len(nr)>1:
                    print "fecthing input lumis and files for",p
                    lumis_per_run[p], files_per_rl[p] = getDatasetLumisAndFiles(p, runs = rwl, lumilist = lwl)
                    n_runs = len(set(lumis_per_run[p].keys()))

            for out in pass_stats_check:
                if prim and n_runs>1: 
                    ## do only for multiple runs output and something in input
                    lumis_per_run[out], files_per_rl[out] = getDatasetLumisAndFiles(out)
                    fetched[out] = True
                    ## now do a better check of fractions
                    fraction_per_run = {}
                    a_primary = list(prim)[0]
                    all_runs = sorted(set(lumis_per_run[a_primary].keys() + lumis_per_run[out].keys()))
                    for run in all_runs:
                        denom = lumis_per_run[a_primary].get(run,[])
                        numer = lumis_per_run[out].get(run,[])
                        if denom:
                            fraction_per_run[run] = float(len(numer))/len(denom)
                        else:
                            print "for run",run,"in output, there isnt any run in input/output..."
                    if fraction_per_run:
                        lowest_fraction = min( fraction_per_run.values())
                        highest_fraction = max( fraction_per_run.values())
                        average_fraction = sum(fraction_per_run.values())/len(fraction_per_run.values())
                        print "the lowest completion fraction per run for",out," is",lowest_fraction
                        print "the highest completion fraction per run for",out," is",highest_fraction
                        print "the average completion fraction per run for",out," is",average_fraction
                        percent_avg_completions[out] = average_fraction
                        percent_completions[out] = lowest_fraction
                
        time_point("more detailed observed statistics", sub_lap=True)

        pass_stats_check = dict([(out, bypass_checks or (percent_completions[out] >= fractions_pass[out])) for out in fractions_pass ])
        pass_stats_check_to_announce = dict([(out, (percent_avg_completions[out] >= fractions_announce[out])) for out in fractions_pass ])
        pass_stats_check_to_truncate_recovery = dict([(out, (percent_avg_completions[out] >= fractions_truncate_recovery[out])) for out in fractions_truncate_recovery ])
        pass_stats_check_over_completion = dict([(out, (percent_completions[out] >= default_fraction_overdoing)) for out in percent_completions ])

        print "announce checks"
        should_announce = False
        if pass_stats_check_to_announce and all(pass_stats_check_to_announce.values()):
            wfi.sendLog('checkor',"The output of this workflow are essentially good to be announced while we work on the rest\n%s \n%s"% ( json.dumps( percent_avg_completions , indent =2 ), json.dumps( fractions_announce , indent =2 )))
            assistance_tags.add('announced' if 'announced' in wfo.status else 'announce')
            
            should_announce = True
        

        if not all(pass_stats_check.values()):
            possible_recoveries = wfi.getRecoveryDoc()
            if possible_recoveries == []:
                wfi.sendLog('checkor','%s has missing statistics \n%s \n%s, but nothing is recoverable. passing through to annoucement'%( 
                        wfo.name, json.dumps(percent_completions, indent=2), json.dumps(fractions_pass, indent=2) ))
                sendLog('checkor','%s is not completed, but has nothing to be recovered, passing along ?'%wfo.name, level='critical')
                ## do not bypass for now, until Alan understands why we are loosing ACDC docs 
                bypass_checks = True
            else:
                wfi.sendLog('checkor','%s is not completed  \n%s \n%s'%( 
                        wfo.name, json.dumps(percent_completions, indent=2), json.dumps(fractions_pass, indent=2) ))

            ## hook for creating automatically ACDC ?
            if not bypass_checks:
                ###############################
                assistance_tags.add('recovery' if use_recoveror else 'manual')
                in_manual += 0 if use_recoveror else 1
                ###############################
                is_closing = False
        else:
            wfi.sendLog('checkor','passing stats check \n%s \n%s'%( json.dumps(percent_completions, indent=2), json.dumps(fractions_pass, indent=2) ))

        if acdc and all(pass_stats_check.values()) and all(pass_stats_check_to_truncate_recovery.values()):
            print "This is essentially good to truncate"

            wfi.sendLog('checkor','Will force-complete the recovery to speed things up')
            forceComplete(url, wfi)

        if over_100_pass and all(pass_stats_check_over_completion.values()):
            ## all outputs are over the top ...
            wfi.sendLog('checkor','Should force-complete the request going over 100%')
            wfi.sendLog('checkor',json.dumps(percent_completions, indent=2))
            sendEmail( "dataset over completion", "Please take a look at %s"% wfo.name)
            assistance_tags.add('over100')
            ## set to force complete the whole thing
            #forceComplete(url, wfi)

        time_point("checked output size", sub_lap=True)

        lumi_upper_limit = {}
        for output in wfi.request['OutputDatasets']:
            upper_limit = 301.
            campaign = campaigns[output]

            if campaign in CI.campaigns and 'lumisize' in CI.campaigns[campaign]:
                upper_limit = CI.campaigns[campaign]['lumisize']
                print "overriding the upper lumi size to",upper_limit,"for",campaign

            if options.lumisize:
                upper_limit = options.lumisize
                print "overriding the upper lumi size to",upper_limit,"by command line"
                
            lumi_upper_limit[output] = upper_limit
            if wfi.request['RequestType'] in ['ReDigi','ReReco']: lumi_upper_limit[output] = -1
        
        if any([ (lumi_upper_limit[out]>0 and events_per_lumi[out] >= lumi_upper_limit[out]) for out in events_per_lumi]):
            print wfo.name,"has big lumisections"
            print json.dumps(events_per_lumi, indent=2)
            ## hook for rejecting the request ?
            if not bypass_checks:
                assistance_tags.add('biglumi')
                #is_closing = False 


        any_presence = {}
        for output in wfi.request['OutputDatasets']:
            any_presence[output] = getDatasetPresence(url, output, vetoes=[])

        time_point("checked dataset presence", sub_lap=True)

        ## custodial copy
        custodial_locations = {}
        custodial_presences = {}
        for output in wfi.request['OutputDatasets']:
            custodial_presences[output] = [s for s in any_presence[output] if 'MSS' in s]
            custodial_locations[output] = phedexClient.getCustodialSubscriptionRequestSite(output)

            if not custodial_locations[output]:
                custodial_locations[output] = []

        time_point("checked custodiality", sub_lap=True)

        ## presence in phedex
        phedex_presence ={}
        for output in wfi.request['OutputDatasets']:
            phedex_presence[output] = phedexClient.getFileCountDataset(url, output )

        time_point("checked phedex count", sub_lap=True)

        ## presence in dbs
        dbs_presence = {}
        dbs_invalid = {}
        for output in wfi.request['OutputDatasets']:
            dbs_presence[output] = dbs3Client.getFileCountDataset( output )
            dbs_invalid[output] = dbs3Client.getFileCountDataset( output, onlyInvalid=True)

        ## prepare the check on having a valid subscription to tape
        out_worth_checking = [out for out in custodial_locations.keys() if out.split('/')[-1] not in vetoed_custodial_tier]
        size_worth_checking = sum([getDatasetSize(out)/1023. for out in out_worth_checking ]) ## size in TBs of all outputs
        size_worht_going_to_ddm = sum([getDatasetSize(out)/1023. for out in out_worth_checking if out.split('/')[-1] in to_ddm_tier ]) ## size in TBs of all outputs
        all_relevant_output_are_going_to_tape = all(map( lambda sites : len(sites)!=0, [custodial_locations[out] for out in out_worth_checking]))

        time_point("dbs file count", sub_lap=True)

        if not all([dbs_presence[out] == (dbs_invalid[out]+phedex_presence[out]) for out in wfi.request['OutputDatasets']]) and not options.ignorefiles:
            mismatch_notice = wfo.name+" has a dbs,phedex mismatch\n"
            mismatch_notice += "in dbs\n"+json.dumps(dbs_presence, indent=2) +"\n"
            mismatch_notice += "invalide in dbs\n"+json.dumps(dbs_invalid, indent=2) +"\n"
            mismatch_notice += "in phedex\n"+json.dumps(phedex_presence, indent=2) +"\n"

            wfi.sendLog('checkor',mismatch_notice)
            if not 'recovering' in assistance_tags:
                assistance_tags.add('filemismatch')
                #print this for show and tell if no recovery on-going
                for out in dbs_presence:
                    _,_,missing_phedex,missing_dbs  = getDatasetFiles(url, out)
                    if missing_phedex:
                        wfi.sendLog('checkor',"These %d files are missing in phedex\n%s"%(len(missing_phedex),
                                                                                          "\n".join( missing_phedex )))
                        were_invalidated = sorted(set(missing_phedex) & set(TMDB_invalid ))
                        if were_invalidated or options.go:
                            wfi.sendLog('checkor',"These %d files were invalidated globally\n%s"%(len(were_invalidated),
                                                                                                  "\n".join(were_invalidated)))
                            sendLog('checkor',"These %d files were invalidated globally\n%s\nand are not invalidated in dbs"%(len(were_invalidated),
                                                                                                                          "\n".join(were_invalidated)), level='critical')
                            dbs3Client.setFileStatus( were_invalidated, newstatus=0 )
                                
                    if missing_dbs:
                        wfi.sendLog('checkor',"These %d files are missing in dbs\n%s"%(len(missing_dbs),
                                    "\n".join( missing_dbs )))
                        were_invalidated = sorted(set(missing_dbs) & set(TMDB_invalid ))
                        if were_invalidated:
                            wfi.sendLog('checkor',"These %d files were invalidated globally\n%s"%(len(were_invalidated),
                                                                                                  "\n".join(were_invalidated)))
            #if not bypass_checks:
            ## I don't think we can by pass this
            is_closing = False
        
        time_point("checked file count", sub_lap=True)

        ## put that heavy part almost at the end
        ## duplication check
        duplications = {}
        #files_per_rl = {}
        lumis_with_duplicates = {}
        for output in wfi.request['OutputDatasets']:
            duplications[output] = "skiped"
            #files_per_rl[output] = "skiped"

        ## check for duplicates prior to making the tape subscription ## this is quite expensive and we run it twice for each sample
        if (is_closing or bypass_checks) and (not options.ignoreduplicates) and (not all_relevant_output_are_going_to_tape):
            print "starting duplicate checker for",wfo.name
            for output in wfi.request['OutputDatasets']:
                print "\tchecking",output
                duplications[output] = True
                if not output in lumis_per_run or not output in files_per_rl:
                    lumis_per_run[output], files_per_rl[output] = getDatasetLumisAndFiles(output)
                    fetched[output] = True

                lumis_with_duplicates[output] = [rl for (rl,files) in files_per_rl[output].items() if len(files)>1]
                duplications[output] = len(lumis_with_duplicates[output])!=0 

            if is_closing and any(duplications.values()) and not options.ignoreduplicates:
                duplicate_notice = ""
                duplicate_notice += "%s has duplicates\n"%wfo.name
                #duplicate_notice += json.dumps( duplications,indent=2)
                #duplicate_notice += '\n'
                ## TO DO, make the file list invalidation analysis. to find the files with least number of lumis
                duplicate_notice += "This number of lumis are duplicated\n"
                duplicate_notice += json.dumps( dict([(o,len(badl)) for o,badl in lumis_with_duplicates.items() ]), indent=2)
                wfi.sendLog('checkor',duplicate_notice)

                bad_files = {}
                for out in duplications:
                    bad_files[out] = duplicateAnalyzer().files_to_remove( files_per_rl[out] )
                    if bad_files[out]:
                        duplicate_notice = "These files %d will be invalidated\n"%(len(bad_files[out]))
                        wfi.sendLog('checkor',duplicate_notice)
                        ## sending the list is not possible
                        duplicate_notice += json.dumps( sorted(bad_files[out]), indent=2)
                        print duplicate_notice
                
                ## and invalidate the files in DBS directly witout asking
                for out,bads in bad_files.items():
                    if bads:
                        invalidateFiles(bads) ## for full removal
                        dbs3Client.setFileStatus( bads, newstatus=0 ) ## invalidate in dbs in the meantime so that the dataset goes into filemismatch category
                    pass

                assistance_tags.add('duplicates')
                is_closing = False 

        time_point("checked duplicates", sub_lap=True)

            
        
        if is_closing and not all_relevant_output_are_going_to_tape:
            print wfo.name,"has not all custodial location"
            print json.dumps(custodial_locations, indent=2)

            ##########
            ## hook for making a custodial replica ?
            custodial = None
            ## get from other outputs
            for output in out_worth_checking:
                if len(custodial_locations[output]): 
                    custodial = custodial_locations[output][0]
            if custodial and float(SI.storage[custodial]) < size_worth_checking:
                print "cannot use the other output custodial:",custodial,"because of limited space"
                custodial = None

            ## try to get it from campaign configuration
            force_custodial = False
            if not custodial:
                for output in out_worth_checking:
                    campaign = campaigns[output]
                    if campaign in CI.campaigns and 'custodial' in CI.campaigns[campaign]:
                        custodial = CI.campaigns[campaign]['custodial']
                        print "Setting custodial to",custodial,"from campaign configuration"
                        force_custodial = True

            group = None
            if campaign in CI.campaigns and 'phedex_group' in CI.campaigns[campaign]:
                group = CI.campaigns[campaign]['phedex_group']
                print "using group",group,"for replica"

            if custodial and float(SI.storage[custodial]) < size_worth_checking:
                print "cannot use the campaign configuration custodial:",custodial,"because of limited space"
                custodial = None

            ## get from the parent
            pick_custodial = True
            use_parent_custodial = UC.get('use_parent_custodial')
            tape_size_limit = options.tape_size_limit if options.tape_size_limit else UC.get("tape_size_limit")
                
            _,prim,_,_ = wfi.getIO()
            if not custodial and prim and use_parent_custodial:
                parent_dataset = prim.pop()
                ## this is terribly dangerous to assume only 
                parents_custodial = phedexClient.getCustodialSubscriptionRequestSite( parent_dataset )
                ###parents_custodial = findCustodialLocation(url, parent_dataset)
                if not parents_custodial:
                    parents_custodial = []

                if len(parents_custodial):
                    custodial = parents_custodial[0]
                else:
                    print "the input dataset",parent_dataset,"does not have custodial in the first place. abort"
                    #sendEmail( "dataset has no custodial location", "Please take a look at %s in the logs of checkor"%parent_dataset)
                    ## does not work for RAWOADSIM
                    sendLog('checkor',"Please take a look at %s for missing custodial location"% parent_dataset)
                    ## cannot be bypassed, this is an issue to fix
                    is_closing = False
                    pick_custodial = False
                    assistance_tags.add('parentcustodial')
                                
            if custodial and float(SI.storage[custodial]) < size_worth_checking:
                print "cannot use the custodial:",custodial,"because of limited space"
                custodial = None

            if not custodial and pick_custodial and not force_custodial:
                ## pick one at random
                custodial = SI.pick_SE(size=size_worth_checking)

            if custodial and size_worht_going_to_ddm > tape_size_limit:
                wfi.sendLog('checkor',"The total output size (%s TB) is too large for the limit set (%s TB)"%( size_worth_checking, tape_size_limit))
                assistance_tags.add('bigoutput')
                custodial = None

            if not custodial:
                print "cannot find a custodial for",wfo.name
                wfi.sendLog('checkor',"cannot find a custodial for %s probably because of the total output size %d"%( wfo.name, size_worth_checking))
                sendLog('checkor',"cannot find a custodial for %s probably because of the total output size %d"%( wfo.name, size_worth_checking), level='critical')

            picked_a_tape = custodial and (is_closing or bypass_checks)
            #cannot be bypassed
            is_closing = False
                
            if picked_a_tape:
                print "picked",custodial,"for tape copy"
                ## remember how much you added this round already ; this stays locally
                SI.storage[custodial] -= size_worth_checking
                ## register the custodial request, if there are no other big issues
                holding = []
                for output in out_worth_checking:
                    if not len(custodial_locations[output]):
                        if phedex_presence[output]>=1:
                            wfi.sendLog('checkor','Using %s as a tape destination for %s'%(custodial, output))
                            custodials[custodial].append( output )
                            if group: custodials[custodial][-1]+='@%s'%group
                            ## let's wait and see if that's needed 
                            assistance_tags.add('custodial')
                            holding.append( output )
                        elif output in pass_stats_check and pass_stats_check[output]:
                                ## there is no file in phedex, but the actual stats check is OK, meaning we are good to let this pass along. the dbs/phedex check will pick this up anyways otherwise
                            wfi.sendLog('checkor','No file in phedex for %s, but statistics check passed'%output)
                        else:
                            ## does not look good
                            wfi.sendLog('checkor','No file in phedex for %s, not good to add to custodial requests'%output)
                            holding.append( output )
                if not holding:
                    is_closing = True

        time_point("determined tape location", sub_lap=True)

        ## disk copy 
        disk_copies = {}
        for output in wfi.request['OutputDatasets']:
            disk_copies[output] = [s for s in any_presence[output] if (not 'MSS' in s) and (not 'Buffer' in s)]

        if not all(map( lambda sites : len(sites)!=0, disk_copies.values())):
            print wfo.name,"has not all output on disk"
            print json.dumps(disk_copies, indent=2)



        fraction_invalid = 0.20
        if not all([(dbs_invalid[out] <= int(fraction_invalid*dbs_presence[out])) for out in wfi.request['OutputDatasets']]) and not options.ignoreinvalid:
            print wfo.name,"has a dbs invalid file level too high"
            print json.dumps(dbs_presence, indent=2)
            print json.dumps(dbs_invalid, indent=2)
            print json.dumps(phedex_presence, indent=2)
            ## need to be going and taking an eye
            assistance_tags.add('invalidfiles')
            ## no need for holding stuff because of a fraction of invalid files
            #if not bypass_checks:
            #    #sub_assistance+="-invalidfiles"
            #    is_closing = False


        time_point("checked invalidation", sub_lap=True)

        time_point("done with %s"%wfo.name)

        ## for visualization later on
        if not wfo.name in fDB.record: 
            #print "adding",wfo.name,"to close out record"
            fDB.record[wfo.name] = {
            'datasets' :{},
            'name' : wfo.name,
            'closeOutWorkflow' : None,
            }
        fDB.record[wfo.name]['closeOutWorkflow'] = is_closing
        fDB.record[wfo.name]['priority'] = wfi.request['RequestPriority']
        fDB.record[wfo.name]['prepid'] = wfi.request['PrepID']

        for output in wfi.request['OutputDatasets']:
            if not output in fDB.record[wfo.name]['datasets']: fDB.record[wfo.name]['datasets'][output] = {}
            rec = fDB.record[wfo.name]['datasets'][output]
            #rec['percentage'] = float('%.2f'%(percent_completions[output]*100))
            rec['percentage'] = math.floor(percent_completions[output]*10000)/100.## round down
            rec['duplicate'] = duplications[output] if output in duplications else 'N/A'
            rec['phedexReqs'] = float('%.2f'%any_presence[output][custodial_presences[output][0]][1]) if len(custodial_presences[output])!=0 else 'N/A'
            rec['closeOutDataset'] = is_closing
            rec['transPerc'] = float('%.2f'%any_presence[output][ disk_copies[output][0]][1]) if len(disk_copies[output])!=0 else 'N/A'
            rec['correctLumis'] = int(events_per_lumi[output]) if (events_per_lumi[output] > lumi_upper_limit[output]) else True
            rec['missingSubs'] = False if len(custodial_locations[output])==0 else ','.join(list(set(custodial_locations[output])))
            rec['dbsFiles'] = dbs_presence[output]
            rec['dbsInvFiles'] = dbs_invalid[output]
            rec['phedexFiles'] = phedex_presence[output]
            rec['acdc'] = "%d / %d"%(len(acdc),len(acdc+acdc_inactive))
            rec['familly'] = true_familly
            now = time.gmtime()
            rec['timestamp'] = time.mktime(now)
            rec['updated'] = time.asctime(now)+' (GMT)'

        ## make the lumi summary 
        if wfi.request['RequestType'] == 'ReReco':
            try:
                #os.system('python Unified/lumi_summary.py %s 1 > /dev/null'%(wfi.request['PrepID']))
                os.system('python Unified/lumi_summary.py %s %d > /dev/null'%(wfi.request['PrepID'],
                                                                              0 if all(fetched.values()) else 1)) ## no need for fresh fetch if that has been done for all already
                os.system('python Unified/lumi_plot.py %s > /dev/null'%(wfi.request['PrepID']))
                wfi.sendLog('checkor','Lumi summary available at %s/datalumi/lumi.%s.html'%(unified_url,wfi.request['PrepID']))
            except Exception as e:
                print str(e)
        ## make the error report
        
    
        ## and move on
        if is_closing:
            ## toggle status to closed-out in request manager
            wfi.sendLog('checkor',"setting %s closed-out"% wfo.name)
            if not options.test:
                if wfo.wm_status in ['closed-out','announced','normal-archived']:
                    print wfo.name,"is already",wfo.wm_status,"not trying to closed-out and assuming it does"
                    res = None
                else:
                    res = reqMgrClient.closeOutWorkflowCascade(url, wfo.name)
                    print "close out answer",res

                if not res in ["None",None]:
                    print "try to get the current status again"
                    wfi_bis = workflowInfo(url, wfo.name)
                    if wfi_bis.request['RequestStatus'] == 'closed-out':
                        print "the request did toggle to closed-out"
                        res = None
                    
                if not res in ["None",None]:
                    print "retrying to closing out"
                    print res
                    res = reqMgrClient.closeOutWorkflowCascade(url, wfo.name)
                    
                
                if res in [None,"None"]:
                    wfo.status = 'close'
                    session.commit()
                    if use_mcm and force_by_mcm:
                        ## shoot large on all prepids, on closing the wf
                        for pid in pids:
                            mcm.delete('/restapi/requests/forcecomplete/%s'%pid)
                else:
                    print "could not close out",wfo.name,"will try again next time"
        else:
            if not 'custodial' in assistance_tags or wfi.isRelval():
                ## do only the report for those
                time_point("Going on and making error reports")
                for member in acdc+acdc_inactive+[wfo.name]:
                    try:
                        if options and options.no_report: continue
                        parse_one(url, member)
                    except:
                        print "Could not make error report for",member
                time_point("Done with reports")

            ## full known list
            #recovering # has active ACDC
            ##OUT #recovered #had inactive ACDC
            #recovery #not over the pass bar
            #over100 # over 100%
            #biglumi # has a big lumiblock
            #parentcustodial # the parent does not have a valid subscription yet
            #custodial # has had the transfer made, is waiting for a valid custodial subscription to appear
            #filemismatch # there is a dbs/phedex mismatch
            #duplicates #a lumi section is there twice

            ## manual is not added yet, and should be so by recoveror
            print wfo.name,"was tagged with :",list(assistance_tags)
            if 'recovering' in assistance_tags:
                ## if active ACDC, being under threshold, filemismatch do not matter
                assistance_tags = assistance_tags - set(['recovery','filemismatch','manual'])
            if 'recovery' in assistance_tags and 'recovered' in assistance_tags:
                ## should not set -recovery to anything that had ACDC already
                assistance_tags = assistance_tags - set(['recovery','recovered']) 
                ## straight to manual
                assistance_tags.add('manual')
                in_manual += 1
            if 'recovery' in assistance_tags and 'manual' in assistance_tags:
                ## this is likely because something bad is happening, so leave it to manual
                assistance_tags = assistance_tags - set(['recovery'])
                assistance_tags.add('manual')
                in_manual += 1
            if 'custodial' in assistance_tags:
                assistance_tags = assistance_tags - set(['announce','announced'])

            ## that means there is something that needs to be done acdc, lumi invalidation, custodial, name it
            print wfo.name,"needs assistance with",",".join( assistance_tags )
            print wfo.name,"existing conditions",",".join( existing_assistance_tags )
        
            #########################################
            ##### notification to requester #########
            go_notify=False
            if assistance_tags and not 'manual' in existing_assistance_tags and existing_assistance_tags != assistance_tags:
                go_notify=True
            

            if go_notify:
                #if wfo.name in already_notified:
                #    print "double notification"
                #    sendEmail('double notification','please take a look at %s'%(wfo.name))                    
                #else:
                #    already_notified.append( wfo.name )

                ###detailslink = 'https://cmsweb.cern.ch/reqmgr/view/details/%s'
                #detailslink = 'https://cmsweb.cern.ch/reqmgr2/fetch?rid=%s'%(wfo.name)
                ###perflink = 'https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s'%(wfo.name)
                perflink = '%s/report/%s'%(unified_url,wfo.name)
                splitlink = 'https://cmsweb.cern.ch/reqmgr/view/splitting/%s'%(wfo.name)
                ## notify templates
                messages= {
                    'recovery': 'Samples completed with missing statistics:\n%s\n%s '%( '\n'.join(['%.2f %% complete for %s'%(percent_completions[output]*100, output) for output in wfi.request['OutputDatasets'] ] ), perflink ),
                    'biglumi': 'Samples completed with large luminosity blocks:\n%s\n%s '%('\n'.join(['%d > %d for %s'%(events_per_lumi[output], lumi_upper_limit[output], output) for output in wfi.request['OutputDatasets'] if (events_per_lumi[output] > lumi_upper_limit[output])]), splitlink),
                    'duplicates': 'Samples completed with duplicated luminosity blocks:\n%s\n'%( '\n'.join(['%s'%output for output in wfi.request['OutputDatasets'] if output in duplications and duplications[output] ] ) ),
                    'filemismatch': 'Samples completed with inconsistency in DBS/Phedex',
                    #'manual' :                     'Workflow completed and requires manual checks by Ops',
                    }
                
                content = "The request PREPID (WORKFLOW) is facing issue in production.\n"
                motive = False
                for case in messages:
                    if case in assistance_tags:
                        content+= "\n"+messages[case]+"\n"
                        motive = True
                content += "You are invited to check, while this is being taken care of by Comp-Ops.\n"
                content += "This is an automated message from Comp-Ops.\n"

                items_notified = set()
                if use_mcm and motive:
                    wfi.notifyRequestor( content , mcm = mcm)

            #########################################


            ## logic to set the status further
            if assistance_tags:
                new_status = 'assistance-'+'-'.join(sorted(assistance_tags) )
            else:
                new_status = 'assistance'
                
            #if should_announce and not 'custodial' in new_status and not 'announce' in wfo.status:
            #    new_status += '-announce'
            #if should_announce and '-announced' in wfo.status:
            #    new_status += '-announced'

            ## case where the workflow was in manual from recoveror
            if not 'manual' in wfo.status or new_status!='assistance-recovery':
                wfo.status = new_status
                if not options.test:
                    wfi.sendLog('checkor','setting %s to %s'%(wfo.name, wfo.status))
                    session.commit()
            else:
                print "current status is",wfo.status,"not changing to anything"


    fDB.html()
    if not spec and in_manual!=0:
        some_details = ""
        if options.strict:
            some_details +="Workflows which just got in completed were looked at. Look in manual.\n"
        if options.update:
            some_details +="Workflows that are still running (and not completed) got looked at.\n"
        if options.clear:
            some_details +="Workflows that just need to close-out were verified. Nothing too new a-priori.\n"
        if options.review:
            some_details +="Workflows under intervention got review.\n"
        count_statuses = defaultdict(int)
        for wfo in session.query(Workflow).filter(Workflow.status.startswith('assistance')).all():
            count_statuses[wfo.status]+=1
        some_details +='\n'.join(['%3d in status %s'%( count_statuses[st], st ) for st in sorted(count_statuses.keys())])
        sendEmail("fresh assistance status available","Fresh status are available at %s/assistance.html\n%s"%(unified_url, some_details),destination=['katherine.rozo@cern.ch'])
        #it's a bit annoying
        pass

    ## custodial requests
    print "Custodials"
    print json.dumps(custodials, indent=2)
    for site in custodials:
        items_at = defaultdict(set)
        for i in custodials[site]:
            item, group = i.split('@') if '@' in i else (i,'DataOps')
            items_at[group].add( item )
        for group,items in items_at.items():
            print ','.join(items),'=>',site,'@',group
            if not options.test:
                result = makeReplicaRequest(url, site, sorted(items) ,"custodial copy at production close-out",custodial='y',priority='low', approve = (site in SI.sites_auto_approve) , group=group)
                print result

    print "File Invalidation"
    print invalidations

if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
    parser.add_option('-t','--test', help='Only test the checkor', action='store_true', default=False)
    parser.add_option('--go',help='Does not check on duplicate process', action='store_true', default=False)

    parser.add_option('--strict', help='Only running workflow that reached completed', action='store_true', default=False)
    parser.add_option('--update', help='Running workflow that have not yet reached completed', action='store_true', default=False)

    parser.add_option('--clear', help='Only the workflow that have reached custodial', action ='store_true', default=False)
    parser.add_option('--review', help='Look at the workflows that have already completed and had required actions', action='store_true', default=False)

    parser.add_option('--limit',help='The number of workflow to consider for checking', default=0, type=int)
    parser.add_option('--fractionpass',help='The completion fraction that is permitted', default=0.0,type='float')
    parser.add_option('--ignorefiles', help='Force ignoring dbs/phedex differences', action='store_true', default=False)
    parser.add_option('--ignoreinvalid', help='Force ignoring high level of invalid files', action='store_true', default=False)
    parser.add_option('--lumisize', help='Force the upper limit on lumisection', default=0, type='float')
    parser.add_option('--ignoreduplicates', help='Force ignoring lumi duplicates', default=False, action='store_true')
    parser.add_option('--tape_size_limit', help='The limit in size of all outputs',default=0,type=int)
    parser.add_option('--html',help='make the monitor page',action='store_true', default=False)
    parser.add_option('--no_report',help='Prevent from making the error report',action='store_true', default=False)
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    if not options.strict and not options.update and not options.clear and not options.review:
        options.strict=True
        options.update=True
        options.clear=True
        options.review=True
        print "no options passed, assuming we do everything"

    checkor(url, spec, options=options)
    
    if not spec or options.html:
        htmlor()


