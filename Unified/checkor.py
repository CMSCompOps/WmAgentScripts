#!/usr/bin/env python

import json
from collections import defaultdict
import optparse
import os
import copy
import time
import random
import math
import threading

from assignSession import *
from htmlor import htmlor
from showError import parse_one, showError_options

from RucioClient import RucioClient
from McMClient import McMClient
from JIRAClient import JIRAClient
import dbs3Client
import reqMgrClient

from utils import getWorkflows,\
                  workflowInfo,\
                  getDatasetEventsAndLumis, \
                  siteInfo, \
                  campaignInfo, \
                  getWorkflowById, \
                  forceComplete, \
                  sendLog, \
                  reqmgr_url, \
                  dbs_url, \
                  dbs_url_writer, \
                  checkMemory,\
                  ThreadHandler,\
                  wtcInfo,\
                  componentInfo, \
                  unifiedConfiguration, \
                  userLock, \
                  moduleLock, \
                  dataCache, \
                  unified_url, \
                  getDatasetLumisAndFiles,\
                  getDatasetRuns,\
                  duplicateAnalyzer,\
                  invalidateFiles, \
                  findParent, \
                  do_html_in_each_module, \
                  getDatasetFileArray,\
                  sendEmail, \
                  closeoutInfo

dbs3Client.dbs3_url = dbs_url
dbs3Client.dbs3_url_writer = dbs_url_writer

def get_campaign(output, wfi):
    ## this should be a perfect matching of output->task->campaign
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

def getDatasetFiles(url, dataset ,without_invalid=True ):

    files = getDatasetFileArray( dataset, validFileOnly=without_invalid, detail=True)
    dbs_filenames = [f['logical_file_name'] for f in files]
    rucioClient = RucioClient()
    rucio_filenames = rucioClient.getFileNamesDataset(dataset)
    return dbs_filenames, rucio_filenames, list(set(dbs_filenames) - set(rucio_filenames)), list(set(rucio_filenames)-set(dbs_filenames))

def checkor(url, spec=None, options=None):
    if userLock():   return

    mlock = moduleLock(locking=False)
    ml=mlock()
    fDB = closeoutInfo()
    UC = unifiedConfiguration()
    up = componentInfo(soft=['mcm','wtc'])
    if not up.check(): return

    use_mcm = up.status['mcm']

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
    exceptions=[]

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
        #print "review option is on: checking the workflows that needed intervention"
        these = filter(lambda wfo: not 'custodial' in wfo.status, standings)
        if options.recovering:
            print "review-recovering is on: checking only the workflows that had been already acted on"
            these = filter(lambda wfo: not 'manual' in wfo.status, these)
            wfs.extend( these )
        if options.manual:
            print "review-manual is on: checking the workflows to be acted on"
            these = filter(lambda wfo: 'manual' in wfo.status, these)
            wfs.extend( these )


    invalidations = [] #a list of files
    SI = siteInfo()
    CI = campaignInfo()
    mcm = McMClient(dev=False) if use_mcm else None
    JC = JIRAClient() if up.status.get('jira',False) else None

    ## retrieve bypass and onhold configuration
    bypasses = []
    forcings = []
    holdings = []

    WI = wtcInfo()
    actors = [a for a,_ in UC.get('allowed_bypass')]
    for user,extending in WI.getHold().items():
        if not user in actors:
            print user,"is not allowed to hold"
            continue
        print user,"is holding",extending
        holdings.extend( extending )

    for user,extending in WI.getBypass().items():
        if not user in actors:
            print user,"is not allowed to bypass"
            continue
        print user,"is bypassing",extending
        bypasses.extend( extending )
        
    overrides = defaultdict(list)
    for user,extending in WI.getForce().items():
        if not user in actors:
            print user,"is not allowed to force complete"
            continue
        print user,"is force-completing",extending
        bypasses.extend( extending )
        overrides[user].extend(extending)

    if JC:
        force_complete_jira_string = "cmsunified please do force-complete"
        to_check = JC.find({'text' : force_complete_jira_string, "status" : "!CLOSED"})
        for j in to_check:
            jira = JC.get(j.key)
            for c in jira.fields.comment.comments:
                if force_complete_jira_string in c.body:
                    user = c.author.name
                    prepid = jira.fields.summary.split()[0]
                    keyword = c.body[(c.body.find(force_complete_jira_string)+len(force_complete_jira_string)):].split()[0]
                    if keyword and user in actors:
                        print user,"is force-completing", keyword,"from JIRA"
                        bypasses.append( keyword )
                        overrides[user].append( keyword )
                    break
        bypass_jira_string = "cmsunified please do bypass"
        to_check = JC.find({'text' : bypass_jira_string, "status" : "!CLOSED"})
        for j in to_check:
            jira = JC.get(j.key)
            for c in jira.fields.comment.comments:
                if bypass_jira_string in c.body:
                    user = c.author.name
                    prepid = jira.fields.summary.split()[0]
                    keyword = c.body[(c.body.find(bypass_jira_string)+len(bypass_jira_string)):].split()[0]
                    if keyword and user in actors:
                        print user,"is bypassing", keyword,"from JIRA"
                        bypasses.append( keyword )
                    break
                    
    if use_mcm:
        ## this is a list of prepids that are good to complete
        forcings = mcm.get('/restapi/requests/forcecomplete')
        if not forcings: forcings = []
    
    ## remove empty entries ...
    bypasses = filter(None, bypasses)
    random.shuffle( wfs )
    in_manual = 0

    ## now you have a record of what file was invalidated globally from TT
    TMDB_invalid = dataCache.get('file_invalidation') 

    print "considering",len(wfs),"before any limitation"
    max_per_round = UC.get('max_per_round').get('checkor',None)
    if options.limit: 
        print "command line to limit to",options.limit
        max_per_round=options.limit
    if max_per_round and not spec: 
        print "limiting to",max_per_round,"this round"
        """
        ##should be ordering by priority if you can
        ## order wfs with rank of wfname
        all_completed_plus = sorted(getWorkflows(url, 'completed' , details=True), key = lambda r : r['RequestPriority'])
        all_completed_plus = [r['RequestName'] for r in all_completed_plus]
        def rank( wfn ):
            return all_completed_plus.index( wfn ) if wfn in all_completed_plus else 0
        wfs = sorted( wfs, key = lambda wfo : rank( wfo.name ),reverse=True)
        if options.update: random.shuffle( wfs )
        """
        wfs = wfs[:max_per_round]

    will_do_that_many = len(wfs)
    report_created = 0

    checkers = []
    for iwfo,wfo in enumerate(wfs):
        ## do the check other one workflow
        if spec and not (spec in wfo.name): continue
        checkers.append( CheckBuster(
            will_do_that_many = will_do_that_many,
            url = url,
            wfo = wfo,
            iwfo = iwfo,
            bypasses = bypasses,
            overrides = overrides,
            holdings = holdings,
            forcings = forcings,
            exceptions = exceptions,
            TMDB_invalid = TMDB_invalid,
            UC = UC,
            CI = CI,
            SI = SI,
            JC = JC,
            use_mcm = use_mcm,
            mcm = mcm
            ))

    ## run the threads
    run_threads = ThreadHandler( threads = checkers,
                                 n_threads = options.threads,
                                 sleepy = 10,
                                 timeout = None,
                                 verbose = True,
                                 label = 'checkor'
                             )
    run_threads.start()

    ## waiting on all to complete
    while run_threads.is_alive():
        time.sleep(5)

    print len(run_threads.threads),"finished thread to gather information from"

    ## then wrap up from the threads
    failed_threads = 0
    for to in run_threads.threads:
        if to.failed:
            failed_threads += 1
            continue
        report_created += to.report_created
        ## change status
        if to.put_record:
            fDB.update( to.wfo.name, to.put_record )

        if to.to_status:
            to.wfo.status = to.to_status
            if 'manual' in to.to_status:
                in_manual += 1
            session.commit()
            if to.to_status == 'close':
                fDB.pop( wfo.name )
                if use_mcm and to.force_by_mcm:
                    for pid in to.pids:
                        mcm.delete('/restapi/requests/forcecomplete/%s'%pid)

    n_wfs = len(run_threads.threads)
    if n_wfs and float(failed_threads/n_wfs) > 0:
        sendLog('checkor','%d/%d threads have failed, better check this out'% (failed_threads, n_wfs), level='critical')
        ## remove once it's all good
        sendEmail('checkor','%d/%d threads have failed, better check this out'% (failed_threads,n_wfs))

    ## conclude things, the good old way
    print report_created,"reports created in this run"

    ## warn us if the process took a bit longer than usual
    if wfs:
        now = time.mktime(time.gmtime())
        time_spend_per_workflow = float(now - time_point.start)/ float( float(len(wfs)))
        print "Average time spend per workflow is", time_spend_per_workflow
        ## set a threshold to it
        if time_spend_per_workflow > 120:
            sendLog('checkor','The module checkor took %.2f [s] per workflow'%( time_spend_per_workflow), level='critical')

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
        pass

    print "File Invalidation"
    print invalidations

    ## a hook to halt checkor nicely at this stage
    if os.path.isfile('.checkor_stop'):
        print "The loop on workflows was shortened"
        sendEmail('checkor','Checkor loop was shortened artificially using .checkor_stop')
        os.system('rm -f .checkor_stop')

class CheckBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        ## a bunch of other things
        for k,v in args.items():
            setattr(self, k, v)
        
        ## actions
        self.to_status = None
        self.put_record = None
        self.force_by_mcm = None
        self.pids = None
        self.report_created = 0 
        self.failed = False
        ## need to find a way to redirect the printouts

    def run(self):
        try:
            self.check()
        except Exception as e:
            ## there should be a warning at this point
            import traceback
            sendLog('checkor','failed on %s due to %s and %s'%( self.wfo.name, str(e), traceback.format_exc()), level='critical')
            self.failed = True

    def check(self):

        ## a hook to halt checkor nicely at this stage
        if os.path.isfile('.checkor_stop'):
            print "The check on workflows is shortened"
            return

        UC = self.UC
        CI = self.CI            
        JC = self.JC
        url = self.url

        bypasses = self.bypasses
        overrides = self.overrides
        holdings = self.holdings
        forcings = self.forcings
        exceptions = self.exceptions

        TMDB_invalid = self.TMDB_invalid

        use_mcm = self.use_mcm
        mcm = self.mcm

        will_do_that_many = self.will_do_that_many

        wfo = self.wfo
        iwfo = self.iwfo

        pattern_fraction_pass = UC.get('pattern_fraction_pass')
        cumulative_fraction_pass = UC.get('cumulative_fraction_pass')
        timeout_for_damping_fraction = UC.get('damping_fraction_pass')
        damping_time = UC.get('damping_fraction_pass_rate')
        damping_fraction_pass_max = float(UC.get('damping_fraction_pass_max')/ 100.)
        acdc_rank_for_truncate = UC.get('acdc_rank_for_truncate')
        use_recoveror = UC.get('use_recoveror')

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
        
        now_s = time.mktime(time.gmtime())

        time_point("Starting checkor with %s Progress [%d/%d]"% (wfo.name, iwfo, will_do_that_many), percent = float(iwfo)/will_do_that_many)
        usage = checkMemory()
        print "memory so far",usage

        wfi = workflowInfo(url, wfo.name)
        wfi.sendLog('checkor',"checking on %s %s"%( wfo.name,wfo.status))
        ## make sure the wm status is up to date.
        # and send things back/forward if necessary.
        wfo.wm_status = wfi.request['RequestStatus']

        if wfo.wm_status in ['closed-out','announced'] and not wfo.name in exceptions:
            ## manually closed-out
            wfi.sendLog('checkor',"%s is already %s, setting close"%( wfo.name , wfo.wm_status))
            self.to_status = 'close'
            return

        elif wfo.wm_status in ['failed','aborted','aborted-archived','rejected','rejected-archived','aborted-completed']:
            ## went into trouble
            if wfi.isRelval():
                wfi.sendLog('checkor',"%s is %s, but will not be set in trouble to find a replacement."%( wfo.name, wfo.wm_status))
                self.to_status = 'forget'
            else:
                self.to_status = 'trouble'
                wfi.sendLog('checkor',"%s is in trouble %s"%(wfo.name, wfo.wm_status))
            return 
        elif wfo.wm_status in ['assigned','acquired']:
            ## not worth checking yet
            wfi.sendLog('checkor',"%s is not running yet"%wfo.name)
            return
        
        if wfo.wm_status != 'completed' and not wfo.name in exceptions:
            ## for sure move on with closeout check if in completed
            wfi.sendLog('checkor',"no need to check on %s in status %s"%(wfo.name, wfo.wm_status))
            return

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
        self.pids = pids
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

        delays = [ (now_s - completed_log[-1]['UpdateTime']) / (60.*60.*24.) if completed_log else 0 for completed_log in [filter(lambda change : change["Status"] in ["completed"], m['RequestTransition']) for m in wfi.getFamilly(details=True,and_self=True)]]
        delays = filter(None, delays)
        min_completed_delays = min(delays) ## take the shortest time since a member of the familly completed
        completed_log = filter(lambda change : change["Status"] in ["completed"],wfi.request['RequestTransition'])
        delay = (now_s - completed_log[-1]['UpdateTime']) / (60.*60.*24.) if completed_log else 0 ## in days
        completed_delay = delay ## this is for the workflow itself
        onhold_completed_delay = min_completed_delays ## this is for any workflows (itself, and ACDC)
        onhold_timeout = UC.get('onhold_timeout')

        if '-onhold' in wfo.status:
            print "onhold since",onhold_completed_delay,"timeout at",onhold_timeout
            if onhold_timeout>0 and onhold_timeout<onhold_completed_delay:
                bypass_checks = True
                wfi.sendLog('checkor',"%s is on hold and stopped for %.2f days, letting this through with current statistics"%( wfo.name, onhold_completed_delay))
            else:
                if wfo.name in holdings and not bypass_checks:
                    wfi.sendLog('checkor',"%s is on hold"%wfo.name)
                    return

        if wfo.name in holdings and not bypass_checks:
            if onhold_timeout>0 and onhold_timeout<onhold_completed_delay:
                bypass_checks =True
                wfi.sendLog('checkor',"%s is on hold and stopped for %.2f days, letting this through with current statistics"%( wfo.name, onhold_completed_delay))
            else:
                self.to_status = 'assistance-onhold'
                wfi.sendLog('checkor',"setting %s on hold"%wfo.name)
                return

        tiers_with_no_check = copy.deepcopy(UC.get('tiers_with_no_check')) # dqm*
        vetoed_custodial_tier = copy.deepcopy(UC.get('tiers_with_no_custodial')) if not wfi.isRelval() else [] #no veto for relvals
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
                if type(CI.campaigns[c]['custodial_override'])==list:
                    vetoed_custodial_tier = list(set(vetoed_custodial_tier) - set(CI.campaigns[c]['custodial_override']))
                    ## add those that we need to check for custodial copy
                    tiers_with_no_check = list(set(tiers_with_no_check) - set(CI.campaigns[c]['custodial_override'])) ## would remove DQM from the vetoed check
                elif CI.campaigns[c]['custodial_override'] == 'notape':
                    vetoed_custodial_tier = sorted(set([o.split('/')[-1] for o in wfi.request['OutputDatasets'] ]))

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
        acdc_failed = []
        acdc_inactive = []
        forced_already=False
        acdc_bads = []
        acdc_order = -1
        true_familly = []
        for member in familly:
            if member['RequestType'] != 'Resubmission': continue
            if member['RequestName'] == wfo.name: continue
            if member['RequestDate'] < wfi.request['RequestDate']: continue
            if member['PrepID'] != wfi.request['PrepID'] : continue
            if member['RequestStatus'] == None: continue

            if not set(member['OutputDatasets']).issubset( set(expected_outputs)):
                if not member['RequestStatus'] in ['rejected-archived','rejected','aborted','aborted-archived']:
                    ##this is not good at all
                    wfi.sendLog('checkor','inconsistent ACDC %s'%member['RequestName'] )
                    acdc_bads.append( member['RequestName'] )
                    is_closing = False
                    assistance_tags.add('manual')
                    assistance_tags.add('inconsistent')
                continue

            true_familly.append( member['RequestName'] )

            for irank in range(10):
                if 'ACDC%d'%irank in member['RequestName']:
                    acdc_order = max( irank, acdc_order )

            if member['RequestStatus'] in ['running-open','running-closed','assigned','acquired','staging','staged']:
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
            elif member['RequestStatus'] in ['failed']:
                acdc_failed.append( member['RequestName'] )
            else:
                acdc_inactive.append( member['RequestName'] )
                assistance_tags.add('recovered')
        if acdc_failed:
            sendLog('checkor','For %s, ACDC %s failed' % (wfo.name, ','.join(acdc_failed)), level='critical')
        if acdc_bads:
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
                #task_outputs[out] = task
                task_outputs[out] = wfi.request.get(task,{}).get('TaskName',task)


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
        
        print event_expected_per_task
        print task_outputs
        time_point("expected statistics", sub_lap=True)
        running_log = filter(lambda change : change["Status"] in ["running-open","running-closed"],wfi.request['RequestTransition'])
        print delay,"since completed"

        default_fraction_overdoing = UC.get('default_fraction_overdoing')
        for output in wfi.request['OutputDatasets']:
            default_pass = UC.get('default_fraction_pass')
            fractions_pass[output] = default_pass
            fractions_announce[output] = 1.0
            c = campaigns[output]
            if c in CI.campaigns and 'earlyannounce' in CI.campaigns[c]:
                wfi.sendLog('checkor', "Allowed to announce the output %s over %.2f by campaign requirement"%(out, CI.campaigns[c]['earlyannounce']))
                fractions_announce[output] = CI.campaigns[c]['earlyannounce']

            if c in CI.campaigns and 'damping' in CI.campaigns[c]:
                ## allow to decrease the pass threshold
                pass 

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
            weight_full = 7.
            weight_pass = delay
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


        #introduce a reduction factor on very old requests
        fraction_damping = min(0.01*(max(completed_delay - timeout_for_damping_fraction,0)/damping_time),damping_fraction_pass_max)
        print "We could reduce the passing fraction by",fraction_damping,"given it's been in for long"
        long_lasting_choped = False
        for out in fractions_pass:
            if fractions_pass[out]!=1.0 and fraction_damping: ## strictly ones cannot be set less than one
                if timeout_for_damping_fraction:
                    fractions_pass[out] -= fraction_damping
                    fractions_truncate_recovery[out] -= fraction_damping
                    long_lasting_choped = True

        if long_lasting_choped :
            msg = 'Reducing pass thresholds by %.3f%% for long lasting workflow %s '%(100*fraction_damping, wfi.request['RequestName'])
            wfi.sendLog('checkor', msg)

        ## do something about workflow with high order ACDC
        if acdc_order > acdc_rank_for_truncate:
            ## there is high order acdc on-going. chop the output at the pass fraction
            wfi.sendLog('checkor','Truncating at pass threshold because of ACDC of rank %d'% acdc_order)
            fractions_truncate_recovery[out] = fractions_pass[out]

        #and then make the fraction multiplicative per child
        parentage = {} ## a daugther: parents kind of thing
        for out in fractions_pass.keys():
            parentage[out] = findParent( out )

        def upward( ns ):
            r = set(ns)
            for n in ns:
                if n in parentage:
                    r.update(upward( parentage[n] ))
            return r

        for out in fractions_pass:
            ancestors = upward(parentage.get(out,[]))
            initial_pass = fractions_pass[out]
            descending_pass = fractions_pass[out]
            descending_truncate = fractions_truncate_recovery[out]
            for a in ancestors:
                descending_pass*=fractions_pass.get(a,1.) ## multiply by fraction of all ancestors
                descending_truncate*=fractions_pass.get(a,1.)
            if cumulative_fraction_pass:
                fractions_pass[out] = descending_pass
                fractions_truncate_recovery[out] = descending_truncate
                print "For",out,"previously passing at",initial_pass,"is now passing at",descending_pass
            else:
                print "For",out,"isntead of passing at",initial_pass,"could be done with",descending_pass


        time_point("statistics thresholds", sub_lap=True)
        expectedL = defaultdict(int)
        expectedN = defaultdict(int)
        producedL = defaultdict(int)
        producedN = defaultdict(int)
        for output in wfi.request['OutputDatasets']:
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=output)
            producedL[output] = lumi_count
            producedN[output] = event_count
            events_per_lumi[output] = event_count/float(lumi_count) if lumi_count else 100
            percent_completions[output] = 0.

            if lumi_expected:
                wfi.sendLog('checkor', "lumi completion %s expected %d for %s"%( lumi_count, lumi_expected, output))
                percent_completions[output] = lumi_count / float( lumi_expected )
                expectedL[output] = lumi_expected

            output_event_expected = event_expected_per_task.get(task_outputs.get(output,'NoTaskFound'), event_expected)
            
            if output_event_expected:
                expectedN[output] = output_event_expected
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
        if pass_stats_check_to_announce and all(pass_stats_check_to_announce.values()):
            wfi.sendLog('checkor',"The output of this workflow are essentially good to be announced while we work on the rest\n%s \n%s"% ( json.dumps( percent_avg_completions , indent =2 ), json.dumps( fractions_announce , indent =2 )))
            assistance_tags.add('announced' if 'announced' in wfo.status else 'announce')

        if not all(pass_stats_check.values()):
            possible_recoveries = wfi.getRecoveryDoc()
            if possible_recoveries == []:
                wfi.sendLog('checkor','%s has missing statistics \n%s \n%s, but nothing is recoverable. passing through to annoucement'%( 
                        wfo.name, json.dumps(percent_completions, indent=2), json.dumps(fractions_pass, indent=2) ))
                sendLog('checkor','%s is not completed, but has nothing to be recovered, passing along ?'%wfo.name, level='critical')
                ## do not bypass for now, until Alan understands why we are loosing ACDC docs 
                bypass_checks = True
            else:
                wfi.sendLog('checkor','%s is not completed.\nCurrent stats:\n%s \nRequired stats:\n%s'%( 
                        wfo.name, json.dumps(percent_completions, indent=2), json.dumps(fractions_pass, indent=2) ))

            ## hook for creating automatically ACDC ?
            if not bypass_checks:
                assistance_tags.add('recovery' if use_recoveror else 'manual')
                is_closing = False
        else:
            wfi.sendLog('checkor','passing stats check \nCurrent stats:\n%s \nRequired stats:\n%s'%( json.dumps(percent_completions, indent=2), json.dumps(fractions_pass, indent=2) ))

        if acdc and all(pass_stats_check.values()) and all(pass_stats_check_to_truncate_recovery.values()):
            print "This is essentially good to truncate"

            wfi.sendLog('checkor','Will force-complete the recovery to speed things up')
            forceComplete(url, wfi)

        if over_100_pass and all(pass_stats_check_over_completion.values()):
            ## all outputs are over the top ...
            wfi.sendLog('checkor','Should force-complete the request going over 100%')
            wfi.sendLog('checkor',json.dumps(percent_completions, indent=2))
            assistance_tags.add('over100')
            ## set to force complete the whole thing

        time_point("checked output size", sub_lap=True)

        lumi_upper_limit = {}
        lumi_lower_limit = UC.get("min_events_per_lumi_output") 
        for output in wfi.request['OutputDatasets']:
            upper_limit = 1001.
            campaign = campaigns[output]

            if campaign in CI.campaigns and 'lumisize' in CI.campaigns[campaign]:
                upper_limit = CI.campaigns[campaign]['lumisize']
                print "overriding the upper lumi size to",upper_limit,"for",campaign

            if options.lumisize:
                upper_limit = options.lumisize
                print "overriding the upper lumi size to",upper_limit,"by command line"
                
            lumi_upper_limit[output] = upper_limit
            if wfi.request['RequestType'] in ['ReDigi','ReReco']: lumi_upper_limit[output] = -1
        
        if (not wfi.isRelval()) and (not prim) and any([ (events_per_lumi[out] <= lumi_lower_limit) for out in events_per_lumi if not out.endswith(('DQMIO','ALCARECO'))]):
            wfi.sendLog('checkor',"%s has very small lumisections\n%s"%( wfo.name, json.dumps(events_per_lumi, indent=2)))
            assistance_tags.add('smalllumi')
            is_closing = False

        if any([ (lumi_upper_limit[out]>0 and events_per_lumi[out] >= lumi_upper_limit[out]) for out in events_per_lumi]):
            wfi.sendLog('checkor',"%s has large lumisections\n%s"%( wfo.name, json.dumps(events_per_lumi, indent=2)))
            ## hook for rejecting the request ?
            assistance_tags.add('biglumi')
            is_closing = False

        ## presence in rucio
        rucio_presence ={}
        rucioClient = RucioClient()
        for output in wfi.request['OutputDatasets']:
            _,dsn,process_string,tier = output.split('/')

            rucio_filecount_pb = rucioClient.getFileCountPerBlock(output)
            all_filecount_pb =  set(rucio_filecount_pb)
            all_blocks = set(map(lambda x: x[0], rucio_filecount_pb))

            if len(all_blocks) == len(all_filecount_pb):
                rucio_presence[output] = sum(map(lambda x: x[1], all_filecount_pb))
            else:
                # TODO: to check if we need to rise a higher level of alarm here.
                msg = "There are inconsistences of number of files per block"
                msg += "between Phedex and Rucio for dataset: {}".format(output)
                wfi.sendLog('checkor', msg)
                rucio_presence[output] = 0
                # we do not announce this output untill the discrepancy from above is resolved
            del(all_filecount_pb)
            del(all_blocks)

        one_output_not_in_rucio = any([Nfiles==0 for Nfiles in rucio_presence.values()])
        if one_output_not_in_rucio and 'announce' in assistance_tags:
            wfi.sendLog('checkor','No files in rucio yet, no good to announce')
            assistance_tags.remove('announce')
            
        time_point("checked rucio count", sub_lap=True)

        ## presence in dbs
        dbs_presence = {}
        dbs_invalid = {}
        for output in wfi.request['OutputDatasets']:
            dbs_presence[output] = dbs3Client.getFileCountDataset( output )
            dbs_invalid[output] = dbs3Client.getFileCountDataset( output, onlyInvalid=True)


        show_N_only = 10 ## number of files to include in a report log

        time_point("dbs file count", sub_lap=True)

        if not all([dbs_presence[out] == (dbs_invalid[out]+rucio_presence[out]) for out in wfi.request['OutputDatasets']]) and not options.ignorefiles:
            mismatch_notice = wfo.name+" has a dbs,rucio mismatch\n"
            mismatch_notice += "in dbs\n"+json.dumps(dbs_presence, indent=2) +"\n"
            mismatch_notice += "invalide in dbs\n"+json.dumps(dbs_invalid, indent=2) +"\n"
            mismatch_notice += "in rucio\n"+json.dumps(rucio_presence, indent=2) +"\n"

            wfi.sendLog('checkor',mismatch_notice)
            if not 'recovering' in assistance_tags:
                if min_completed_delays  < 2: ## less than 2 days in completed for any of the workflow of the prepid
                    assistance_tags.add('agentfilemismatch')
                else:
                    assistance_tags.add('filemismatch')
                #print this for show and tell if no recovery on-going
                for out in dbs_presence:
                    dbs_filenames,rucio_filenames,missing_rucio,missing_dbs  = getDatasetFiles(url, out)

                    # Corrections to the lists of files present in Phedex for the data Tiers managed by Rucio
                    _,dsn,process_string,tier = output.split('/')
                    if True:
                        # Here recalculating the filenames as a union of the rucio_files | rucio_files
                        all_filenames = set(rucioClient.getFileNamesDataset(out))
                        missing_rucio = list(set(dbs_filenames) - all_filenames)
                        missing_dbs = list(all_filenames - set(dbs_filenames))

                    if missing_rucio:
                        wfi.sendLog('checkor',"These %d files are missing in rucio, or extra in dbs, showing %s only\n%s"%(len(missing_rucio),show_N_only,
                                                                                                           "\n".join( missing_rucio[:show_N_only] )))
                        were_invalidated = sorted(set(missing_rucio) & set(TMDB_invalid ))
                        if were_invalidated:
                            wfi.sendLog('checkor',"These %d files were invalidated globally, showing %d only\n%s"%(len(were_invalidated),show_N_only,
                                                                                                                   "\n".join(were_invalidated[:show_N_only])))
                            sendLog('checkor',"These %d files were invalidated globally, showing %d only\n%s\nand are not invalidated in dbs"%(len(were_invalidated),show_N_only,
                                                                                                                                               "\n".join(were_invalidated[:show_N_only])), level='critical')
                            dbs3Client.setFileStatus( were_invalidated, newstatus=0 )
                                
                    if missing_dbs:
                        wfi.sendLog('checkor',"These %d files are missing in dbs, or extra in rucio, showing %d only\n%s"%(len(missing_dbs),show_N_only,
                                                                                                        "\n".join( missing_dbs[:show_N_only] )))
                        were_invalidated = sorted(set(missing_dbs) & set(TMDB_invalid ))
                        if were_invalidated:
                            wfi.sendLog('checkor',"These %d files were invalidated globally,showing %d only\n%s"%(len(were_invalidated),show_N_only,
                                                                                                                  "\n".join(were_invalidated[:show_N_only])))
            is_closing = False
        
        time_point("checked file count", sub_lap=True)

        ## duplication check
        duplications = {}
        lumis_with_duplicates = {}
        for output in wfi.request['OutputDatasets']:
            duplications[output] = "skiped"

        ignoreduplicates ={}
        for out,c in campaigns.items():
            if c in CI.campaigns and 'ignoreduplicates' in CI.campaigns[c]:
                ignoreduplicates[out] = CI.campaigns[c]['ignoreduplicates'] or options.ignoreduplicates
            else:
                ignoreduplicates[out] = options.ignoreduplicates

        # Duplicate check is disabled for now
        stop_duplicate_check = True
        if (not stop_duplicate_check) and (is_closing or bypass_checks):
            print "starting duplicate checker for",wfo.name
            for output in wfi.request['OutputDatasets']:
                if (output in ignoreduplicates) and (ignoreduplicates[output]):
                    print "\tNot checking",output
                    duplications[output] = False
                    continue
                print "\tchecking",output
                duplications[output] = True
                if not output in lumis_per_run or not output in files_per_rl:
                    lumis_per_run[output], files_per_rl[output] = getDatasetLumisAndFiles(output)
                    fetched[output] = True

                lumis_with_duplicates[output] = [rl for (rl,files) in files_per_rl[output].items() if len(files)>1]
                duplications[output] = len(lumis_with_duplicates[output])!=0 

            if is_closing and any(duplications.values()):
                duplicate_notice = ""
                duplicate_notice += "%s has duplicates\n"%wfo.name
                ## TO DO, make the file list invalidation analysis. to find the files with least number of lumis
                duplicate_notice += "This number of lumis are duplicated\n"
                duplicate_notice += json.dumps( dict([(o,len(badl)) for o,badl in lumis_with_duplicates.items() ]), indent=2)
                wfi.sendLog('checkor',duplicate_notice)

                bad_files = {}
                for out in duplications:
                    bad_files[out] = duplicateAnalyzer().files_to_remove( files_per_rl[out] )
                    if bad_files[out]:
                        duplicate_notice = "These files %d will be invalidated, showing %d only\n%s"%(len(bad_files[out]),
                                                                                                      show_N_only,
                                                                                                      "\n".join( bad_files[out][:show_N_only]))
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

        fraction_invalid = 0.20
        if not all([(dbs_invalid[out] <= int(fraction_invalid*dbs_presence[out])) for out in wfi.request['OutputDatasets']]) and not options.ignoreinvalid:
            print wfo.name,"has a dbs invalid file level too high"
            print json.dumps(dbs_presence, indent=2)
            print json.dumps(dbs_invalid, indent=2)
            print json.dumps(rucio_presence, indent=2)
            ## need to be going and taking an eye
            assistance_tags.add('invalidfiles')
            ## no need for holding stuff because of a fraction of invalid files

        time_point("checked invalidation", sub_lap=True)
        time_point("done with %s"%wfo.name)

        ## for visualization later on
        put_record = {
            'datasets' :{},
            'name' : wfo.name,
            'closeOutWorkflow' : is_closing,
            'priority' : wfi.request['RequestPriority'],
            'prepid' :  wfi.request['PrepID'],
            }
        for output in wfi.request['OutputDatasets']:
            if not output in put_record['datasets']: put_record['datasets'][output] = {}
            rec = put_record['datasets'][output]
            rec['expectedL'] = expectedL[output]
            rec['expectedN'] = expectedN[output]
            rec['producedL'] = producedL[output]
            rec['producedN'] = producedN[output]
            rec['percentage'] = math.floor(percent_completions[output]*10000)/100.## round down
            rec['fractionpass'] = math.floor(fractions_pass.get(output,0)*10000)/100.
            rec['duplicate'] = duplications[output] if output in duplications else 'N/A'
            rec['closeOutDataset'] = is_closing
            rec['correctLumis'] = int(events_per_lumi[output]) if (events_per_lumi[output] > lumi_upper_limit[output]) else True
            rec['dbsFiles'] = dbs_presence[output]
            rec['dbsInvFiles'] = dbs_invalid[output]
            rec['rucioFiles'] = rucio_presence[output]
            rec['acdc'] = "%d / %d"%(len(acdc),len(acdc+acdc_inactive))
            rec['familly'] = true_familly
            now = time.gmtime()
            rec['timestamp'] = time.mktime(now)
            rec['updated'] = time.asctime(now)+' (GMT)'

        self.put_record = put_record

        ## make the lumi summary 
        if wfi.request['RequestType'] == 'ReReco':
            try:
                os.system('python Unified/lumi_summary.py %s %d > /dev/null'%(wfi.request['PrepID'],
                                                                              0 if all(fetched.values()) else 1)) ## no need for fresh fetch if that has been done for all already
                os.system('python Unified/lumi_plot.py %s > /dev/null'%(wfi.request['PrepID']))
                wfi.sendLog('checkor','Lumi summary available at %s/datalumi/lumi.%s.html'%(unified_url,wfi.request['PrepID']))
            except Exception as e:
                print str(e)
    
        ## and move on
        if is_closing:
            ## toggle status to closed-out in request manager
            wfi.sendLog('checkor',"setting %s closed-out"% wfo.name)
            if wfi.isRelval():
                ## error report for relval regardless
                so = showError_options( expose = 2 )
                try:
                    parse_one(url, wfo.name, so)
                except Exception as e:
                    print "Could not make error report for",wfo.name
                    print "because",str(e)
                
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
                    self.to_status = 'close'
                    self.force_by_mcm = force_by_mcm
                else:
                    print "could not close out",wfo.name,"will try again next time"
        else:
            if not 'custodial' in assistance_tags or wfi.isRelval():
                ## do only the report for those
                time_point("Going on and making error reports")
                for member in acdc+acdc_inactive+[wfo.name]:
                    try:
                        if options and options.no_report: continue
                        expose = UC.get('n_error_exposed') if ('manual' in assistance_tags) else 0
                        so = showError_options( expose = expose )
                        parse_one(url, member, so)
                        self.report_created += 1
                    except Exception as e:
                        print "Could not make error report for",member
                        print "because",str(e)

                time_point("Done with reports")

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
            if 'recovery' in assistance_tags and 'manual' in assistance_tags:
                ## this is likely because something bad is happening, so leave it to manual
                assistance_tags = assistance_tags - set(['recovery'])
                assistance_tags.add('manual')
            if 'custodial' in assistance_tags:
                assistance_tags = assistance_tags - set(['announce','announced'])
            if any((tag in assistance_tags) for tag in ['duplicates','filemismatch','agentfilemismatch']):
                assistance_tags = assistance_tags - set(['announce'])

            ## that means there is something that needs to be done acdc, lumi invalidation, custodial, name it
            print wfo.name,"needs assistance with",",".join( assistance_tags )
            print wfo.name,"existing conditions",",".join( existing_assistance_tags )
        
            #########################################
            ##### notification to requester #########
            go_notify=False
            if assistance_tags and not 'manual' in existing_assistance_tags and existing_assistance_tags != assistance_tags:
                go_notify=True

            if go_notify:
                perflink = '%s/report/%s'%(unified_url,wfo.name)
                splitlink = 'https://cmsweb.cern.ch/reqmgr/view/splitting/%s'%(wfo.name)
                ## notify templates
                messages= {
                    'recovery': 'Samples completed with missing statistics:\n%s\n%s '%( '\n'.join(['%.2f %% complete for %s'%(percent_completions[output]*100, output) for output in wfi.request['OutputDatasets'] ] ), perflink ),
                    'biglumi': 'Samples completed with large luminosity blocks:\n%s\n%s '%('\n'.join(['%d > %d for %s'%(events_per_lumi[output], lumi_upper_limit[output], output) for output in wfi.request['OutputDatasets'] if (events_per_lumi[output] > lumi_upper_limit[output])]), splitlink),
                    'duplicates': 'Samples completed with duplicated luminosity blocks:\n%s\n'%( '\n'.join(['%s'%output for output in wfi.request['OutputDatasets'] if output in duplications and duplications[output] ] ) ),
                    }
                
                content = "The request PREPID (WORKFLOW) is facing issue in production.\n"
                motive = False
                for case in messages:
                    if case in assistance_tags:
                        content+= "\n"+messages[case]+"\n"
                        motive = True
                content += "You are invited to check, while this is being taken care of by Comp-Ops.\n"
                content += "This is an automated message from Comp-Ops.\n"

                if use_mcm and motive:
                    wfi.notifyRequestor( content , mcm = mcm)

            #########################################

            ## logic to set the status further
            if assistance_tags:
                new_status = 'assistance-'+'-'.join(sorted(assistance_tags) )
            else:
                new_status = 'assistance'


            ## case where the workflow was in manual from recoveror
            if not 'manual' in wfo.status or new_status!='assistance-recovery':
                if not options.test:
                    wfi.sendLog('checkor','setting %s to %s'%(wfo.name, new_status))
                    self.to_status = new_status
            else:
                print "current status is",wfo.status,"not changing to anything"
        
            pop_a_jira = False
            ## rereco and manual => jira
            if 'manual' in self.to_status and 'ReReco' in wfi.request['RequestType']:
                pop_a_jira = True
            ## end of first round acdc => jira
            if 'recovered' in self.to_status and 'manual' in self.to_status:
                pop_a_jira = True
            ## create a jira in certain cases
            if pop_a_jira and JC:
                jiras = JC.find( {'prepid' : wfi.request['PrepID']})
                j_comment = None
                j = None
                if len(jiras)==0:
                    ## then you can create one
                    j = JC.create( 
                        {
                            'priority' : wfi.request['RequestPriority'],
                            'summary' : '%s issues'% wfi.request['PrepID'],
                            'label' : 'WorkflowTrafficController',
                            'description' : 'https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s \nAutomatic JIRA from unified'%( wfi.request['PrepID'])
                        } 
                    )
                    j_comment = "Appears in %s"%(self.to_status)
                else:
                    ## pick up the last one
                    print "a jira already exists, taking the last one"
                    j = sorted(jiras, key= lambda o:JC.created(o))[-1]
                    if JC.reopen(j.key):
                        j_comment = "Came back in %s"%(self.to_status)
                if j and j_comment:
                    JC.comment(j.key,j_comment)

if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
    parser.add_option('-t','--test', help='Only test the checkor', action='store_true', default=False)
    parser.add_option('--go',help='Does not check on duplicate process', action='store_true', default=False)
    parser.add_option('--strict', help='Only running workflow that reached completed', action='store_true', default=False)
    parser.add_option('--update', help='Running workflow that have not yet reached completed', action='store_true', default=False)
    parser.add_option('--clear', help='Only the workflow that have reached custodial', action ='store_true', default=False)
    parser.add_option('--review', help='Look at the workflows that have already completed and had required actions', action='store_true', default=False)
    parser.add_option('--recovering', help='Look at the workflows that already have on-going acdc', action='store_true', default=False)
    parser.add_option('--manual', help='Look at the workflows in "manual"', action='store_true', default=False)
    parser.add_option('--limit',help='The number of workflow to consider for checking', default=0, type=int)
    parser.add_option('--fractionpass',help='The completion fraction that is permitted', default=0.0,type='float')
    parser.add_option('--ignorefiles', help='Force ignoring dbs/rucio differences', action='store_true', default=False)
    parser.add_option('--ignoreinvalid', help='Force ignoring high level of invalid files', action='store_true', default=False)
    parser.add_option('--lumisize', help='Force the upper limit on lumisection', default=0, type='float')
    parser.add_option('--ignoreduplicates', help='Force ignoring lumi duplicates', default=False, action='store_true')
    parser.add_option('--tape_size_limit', help='The limit in size of all outputs',default=0,type=int)
    parser.add_option('--html',help='make the monitor page',action='store_true', default=False)
    parser.add_option('--no_report',help='Prevent from making the error report',action='store_true', default=False)
    parser.add_option('--threads',help='Number of threads for processing workflows',default=10, type=int)
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    options.manual = not options.recovering
    if not options.strict and not options.update and not options.clear and not options.review:
        options.strict=True
        options.update=True
        options.clear=True
        options.review=True
        options.recovering=True
        options.manual=True
        print "no options passed, assuming we do everything"

    checkor(url, spec, options=options)
    
    if (not spec and do_html_in_each_module) or options.html:
        htmlor()
