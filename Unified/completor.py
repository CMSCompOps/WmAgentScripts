#!/usr/bin/env python
from assignSession import *
import sys
import reqMgrClient
from utils import workflowInfo, getWorkflowById, forceComplete, getDatasetEventsAndLumis, componentInfo, monitor_dir, reqmgr_url, unifiedConfiguration, getForceCompletes, getAllStuckDataset, monitor_pub_dir
from utils import campaignInfo, siteInfo, sendLog, sendEmail
from collections import defaultdict
import json
import random
from McMClient import McMClient
from showError import parse_one
import random

def completor(url, specific):
    use_mcm = True
    up = componentInfo(mcm=use_mcm, soft=['mcm'])
    if not up.check(): return
    use_mcm = up.status['mcm']
    if use_mcm:
        mcm = McMClient(dev=False)

    safe_mode = True

    CI = campaignInfo()
    SI = siteInfo()
    UC = unifiedConfiguration()

    wfs = []
    wfs.extend( session.query(Workflow).filter(Workflow.status == 'away').all() )
    wfs.extend( session.query(Workflow).filter(Workflow.status.startswith('assistance')).all() )

    ## just take it in random order so that not always the same is seen
    random.shuffle( wfs )

    max_per_round = UC.get('max_per_round').get('completor',None)
    if max_per_round and not specific: wfs = wfs[:max_per_round]

    all_stuck = set()
    ## take into account what stagor was saying
    all_stuck.update( json.loads( open('%s/stuck_transfers.json'%monitor_pub_dir).read() ))
    ## take into account the block that needed to be repositioned recently
    all_stuck.update( [b.split('#')[0] for b in json.loads( open('%s/missing_blocks.json'%monitor_dir).read()) ] )
    ## take into account all stuck block and dataset from transfer team
    all_stuck.update( getAllStuckDataset()) 


    ## by workflow a list of fraction / timestamps
    completions = json.loads( open('%s/completions.json'%monitor_dir).read())
    
    good_fractions = {}
    overdoing_fractions = {}
    truncate_fractions = {} 
    timeout = {}
    campaign_injection_delay = {}
    for c in CI.campaigns:
        if 'force-complete' in CI.campaigns[c]:
            good_fractions[c] = CI.campaigns[c]['force-complete']
        if 'truncate-complete' in CI.campaigns[c]:
            truncate_fractions[c] = CI.campaigns[c]['truncate-complete']
        if 'force-timeout' in CI.campaigns[c]:
            timeout[c] = CI.campaigns[c]['force-timeout']
        if 'injection-delay' in CI.campaigns[c]:
            campaign_injection_delay[c] = CI.campaigns[c]['injection-delay']
        if 'overdoing-complete' in CI.campaigns[c]:
            overdoing_fractions[c] = CI.campaigns[c]['overdoing-complete']

    long_lasting = {}

    overrides = getForceCompletes()
    if use_mcm:    
        ## add all workflow that mcm wants to get force completed
        mcm_force = mcm.get('/restapi/requests/forcecomplete')
        ## assuming this will be a list of actual prepids
        overrides['mcm'] = mcm_force

    print "can force complete on"
    print json.dumps( good_fractions ,indent=2)
    print "can truncate complete on"
    print json.dumps( truncate_fractions ,indent=2)
    print "can overide on"
    print json.dumps( overrides, indent=2)
    max_force = UC.get("max_force_complete")
    max_priority = UC.get("max_tail_priority")
    injection_delay_threshold = UC.get("injection_delay_threshold")
    injection_delay_priority = UC.get("injection_delay_priority")
    delay_priority_increase = UC.get("delay_priority_increase")
    default_fraction_overdoing = UC.get('default_fraction_overdoing')

    set_force_complete = set()

    for wfo in wfs:
        if specific and not specific in wfo.name: continue

        print "looking at",wfo.name

        ## get all of the same
        wfi = workflowInfo(url, wfo.name)
        pids = wfi.getPrepIDs()
        skip=False
        campaigns = wfi.getCampaigns()

        #if not any([c in good_fractions.keys() for c in campaigns]): skip=True
        #if not any([c in truncate_fractions.keys() for c in campaigns]): skip=True

        for user,spec in overrides.items():
            if not spec: continue
            spec = filter(None, spec)
            if not wfi.request['RequestStatus'] in ['force-complete', 'completed']:
                if any(s in wfo.name for s in spec) or (wfo.name in spec) or any(pid in spec for pid in pids) or any(s in pids for s in spec):

                    wfi = workflowInfo(url, wfo.name)
                    forceComplete(url , wfi )
                    skip=True
                    wfi.notifyRequestor("The workflow %s was force completed by request of %s"%(wfo.name,user), do_batch=False)
                    wfi.sendLog('completor','%s is asking for %s to be force complete'%(user,wfo.name))
                    break
    
        if wfo.status.startswith('assistance'): skip = True

        if skip: 
            continue

        priority = wfi.request['RequestPriority']

        if not 'Campaign' in wfi.request: continue

        if not wfi.request['RequestStatus'] in ['acquired','running-open','running-closed']: continue



        ## until we can map the output to task ...
        output_per_task = wfi.getOutputPerTask() ## can use that one, and follow mapping
        good_fraction_per_out = {}
        good_fraction_nodelay_per_out = {}
        truncate_fraction_per_out = {}
        #allowed_delay_per_out = {}
        for task,outs in output_per_task.items():
            task_campaign = wfi.getCampaignPerTask( task )
            for out in outs:
                good_fraction_per_out[out] = good_fractions.get(task_campaign,1000.)
                good_fraction_nodelay_per_out[out] = overdoing_fractions.get(task_campaign,default_fraction_overdoing)
                truncate_fraction_per_out[out] = truncate_fractions.get(task_campaign,1000.)
                #allowed_delay_per_out[out] = timeout.get(task_campaign, 14)

        #print "force at", json.dumps( good_fraction_per_out, indent=2)
        #print "truncate at",json.dumps( truncate_fraction_per_out, indent=2)

        now = time.mktime(time.gmtime()) / (60*60*24.)

        running_log = filter(lambda change : change["Status"] in ["acquired","running-open","running-closed"],wfi.request['RequestTransition'])
        if not running_log:
            print "\tHas no running log"
            delay = 0
        else:
            then = running_log[-1]['UpdateTime'] / (60.*60.*24.)
            delay = now - then ## in days

        ## this is supposed to be the very initial request date, inherited from clones
        injection_delay = None
        original = wfi
        if 'OriginalRequestName' in original.request:
            ## go up the clone chain
            original = workflowInfo(url, original.request['OriginalRequestName'])
        injected_log = filter(lambda change : change["Status"] in ["assignment-approved"],original.request['RequestTransition'])
        if injected_log:
            injected_on = injected_log[-1]['UpdateTime'] / (60.*60.*24.)
            injection_delay = now - injected_on
        


        (w,d) = divmod(delay, 7 )
        print "\t"*int(w)+"Running since",delay,"[days] priority=",priority

        if injection_delay!=None and injection_delay > injection_delay_threshold and priority >= injection_delay_priority:
            quantized = 5000 ## quantize priority
            tail_cutting_priority = wfi.request['InitialPriority']+ int((delay_priority_increase * (injection_delay - injection_delay_threshold) / 7) / quantized) * quantized
            tail_cutting_priority += 101 ## to signal it is from this mechanism
            tail_cutting_priority = min(400000, tail_cutting_priority) ## never go above 400k priority
            tail_cutting_priority = max(tail_cutting_priority, priority) ## never go below the current value
            
            if priority < tail_cutting_priority:
                sendLog('completor',"%s Injected since %s [days] priority=%s, increasing to %s"%(wfo.name,injection_delay,priority, tail_cutting_priority), level='critical')
                wfi.sendLog('completor','bumping priority to %d for being injected since %s'%( tail_cutting_priority, injection_delay))
                if max_priority:
                    reqMgrClient.changePriorityWorkflow(url, wfo.name, tail_cutting_priority)
                    max_priority-=1
                else:
                    print "Could be changing the priority to higher value, but too many already were done"

        _,prim,_,_ = wfi.getIO()
        is_stuck = all_stuck & prim
        if is_stuck: wfi.sendLog('completor','%s is stuck'%','.join(is_stuck))

        monitor_delay = 7
        allowed_delay = max([timeout.get(c,14) for c in campaigns])
            
        monitor_delay = min(monitor_delay, allowed_delay)

        ### just skip if too early, just for the sake of not computing the completion fraction just now.
        # maybe this is fast enough that we can do it for all
        if delay <= monitor_delay: 
            print "not enough time has passed yet"
            continue

        long_lasting[wfo.name] = { "delay" : delay,
                                   "injection_delay" : injection_delay }

        percent_completions = wfi.getCompletionFraction(caller='completor')
        
        if not percent_completions:
            sendLog('completor','%s has no output at all'% wfo.name, level='critical')
            continue

        is_over_allowed_delay = (all([percent_completions[out] >= good_fraction_per_out[out] for out in percent_completions]) and delay >= allowed_delay)
        is_over_truncation_delay = (is_stuck and (all([percent_completions[out] >= truncate_fraction_per_out[out] for out in percent_completions])) and delay >= allowed_delay)
        is_over_completion = (all([percent_completions[out] >= good_fraction_nodelay_per_out[out] for out in percent_completions]))

        if is_over_completion:
            wfi.sendLog('completor', "all is over completed %s\n %s"%( json.dumps( good_fraction_nodelay_per_out, indent=2 ),
                                                                       json.dumps( percent_completions, indent=2 )
                                                                       ))
        elif is_over_allowed_delay:
            wfi.sendLog('completor', "all is above %s \n%s"%( json.dumps(good_fraction_per_out, indent=2 ), 
                                                              json.dumps( percent_completions, indent=2 )
                                                              ))
        elif is_over_truncation_delay:
            wfi.sendLog('completor', "all is above %s truncation level, and the input is stuck\n%s"%( json.dumps(truncate_fraction_per_out, indent=2 ),
                                                                                                      json.dumps( percent_completions, indent=2 ) ) )

        else:
            long_lasting[wfo.name].update({
                    'completion': sum(percent_completions.values()) / len(percent_completions),
                    'completions' : percent_completions
                    })
            
            ## do something about the agents this workflow is in
            long_lasting[wfo.name]['agents'] = wfi.getAgents()
            wfi.sendLog('completor', "%s not over bound \ncomplete at %s \n truncate at %s \nRunning %s"%(json.dumps( percent_completions, indent=2), 
                                                                                                 json.dumps(good_fraction_per_out, indent=2),
                                                                                                 json.dumps( truncate_fraction_per_out, indent=2),
                                                                                                 json.dumps( long_lasting[wfo.name]['agents'], indent=2) ))
            continue

        for output in  percent_completions:
            completions[output]['injected'] = then

        #further check on delays
        cpuh = wfi.getComputingTime(unit='d')

        ran_at = wfi.request['SiteWhitelist']
                        
        wfi.sendLog('completor',"Required %s, time spend %s"%( cpuh, delay))
                    
        ##### WILL FORCE COMPLETE BELOW
        # only really force complete after n days

        ## find ACDCs that might be running
        if max_force>0:
            print "going for force-complete of",wfo.name
            if not safe_mode:
                forceComplete(url, wfi )
                set_force_complete.add( wfo.name )
                wfi.sendLog('completor','going for force completing')
                wfi.notifyRequestor("The workflow %s was force completed for running too long"% wfo.name)
                max_force -=1
            else:
                sendEmail('completor', 'The workflow %s is ready for force complete, but completor is in safe mode'%wfo.name)
        else:
            wfi.sendLog('completor',"too many completion this round, cannot force complete")

    if set_force_complete:
        sendLog('completor','The followings were set force-complete \n%s'%('\n'.join(set_force_complete)))
    
    open('%s/completions.json'%monitor_dir,'w').write( json.dumps( completions , indent=2))
    text="These have been running for long"
    
    open('%s/longlasting.json'%monitor_dir,'w').write( json.dumps( long_lasting, indent=2 ))

    for wf,info in sorted(long_lasting.items(), key=lambda tp:tp[1]['delay'], reverse=True):
        delay = info['delay']
        text += "\n %s : %s days"% (wf, delay)
        if 'completion' in info:
            text += " %d%%"%( info['completion']*100 )


    print text


if __name__ == "__main__":
    url = reqmgr_url
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
        
    completor(url, spec)

