#!/usr/bin/env python
from assignSession import *
import sys
import reqMgrClient
from utils import workflowInfo, getWorkflowById, forceComplete, getDatasetEventsAndLumis, componentInfo, monitor_dir, reqmgr_url, unifiedConfiguration, getForceCompletes
from utils import campaignInfo, sendEmail, siteInfo, sendLog
from collections import defaultdict
import json
import random
from McMClient import McMClient



def completor(url, specific):
    use_mcm = True
    up = componentInfo(mcm=use_mcm, soft=['mcm'])
    if not up.check(): return
    use_mcm = up.status['mcm']
    if use_mcm:
        mcm = McMClient(dev=False)

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

    ## by workflow a list of fraction / timestamps
    completions = json.loads( open('%s/completions.json'%monitor_dir).read())
    
    good_fractions = {}
    timeout = {}
    for c in CI.campaigns:
        if 'force-complete' in CI.campaigns[c]:
            good_fractions[c] = CI.campaigns[c]['force-complete']
        if 'force-timeout' in CI.campaigns[c]:
            timeout[c] = CI.campaigns[c]['force-timeout']

    long_lasting = {}

    overrides = getForceCompletes()
    if use_mcm:    
        ## add all workflow that mcm wants to get force completed
        mcm_force = mcm.get('/restapi/requests/forcecomplete')
        ## assuming this will be a list of actual prepids
        overrides['mcm'] = mcm_force

    print "can force complete on"
    print json.dumps( good_fractions ,indent=2)
    print json.dumps( overrides, indent=2)
    max_force = UC.get("max_force_complete")
    
    #wfs_no_location_in_GQ = set()
    #block_locations = defaultdict(lambda : defaultdict(list))
    #wfs_no_location_in_GQ = defaultdict(list)

    set_force_complete = set()


    for wfo in wfs:
        if specific and not specific in wfo.name: continue

        print "looking at",wfo.name
        ## get all of the same
        wfi = workflowInfo(url, wfo.name)
        pids = wfi.getPrepIDs()
        skip=False
        if not any([c in wfo.name for c in good_fractions]): skip=True
        for user,spec in overrides.items():

            if wfi.request['RequestStatus']!='force-complete':
                if any(s in wfo.name for s in spec) or (wfo.name in spec) or any(pid in spec for pid in pids) or any(s in pids for s in spec):
                    sendEmail('force-complete requested','%s is asking for %s to be force complete'%(user,wfo.name))
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

        c = wfi.request['Campaign']
        if not c in good_fractions: continue
        good_fraction = good_fractions[c]
        ignore_fraction = 2.
        
        lumi_expected = None
        event_expected = None
        if not 'TotalInputEvents' in wfi.request: 
            if 'RequestNumEvents' in wfi.request:
                event_expected = wfi.request['RequestNumEvents']
            else:
                print "truncated, cannot do anything"
                continue
        else:
            lumi_expected = wfi.request['TotalInputLumis']
            event_expected = wfi.request['TotalInputEvents']

        now = time.mktime(time.gmtime()) / (60*60*24.)

        running_log = filter(lambda change : change["Status"] in ["running-open","running-closed"],wfi.request['RequestTransition'])
        if not running_log:
            print "\tHas no running log"
            # cannot figure out when the thing started running
            continue
        then = running_log[-1]['UpdateTime'] / (60.*60.*24.)
        delay = now - then ## in days

        (w,d) = divmod(delay, 7 )
        print "\t"*int(w)+"Running since",delay,"[days] priority=",priority

        monitor_delay = 7
        allowed_delay = 14
        if c in timeout:
            allowed_delay = timeout[c]
            
        monitor_delay = min(monitor_delay, allowed_delay)
        ### just skip if too early
        if delay <= monitor_delay: continue

        long_lasting[wfo.name] = { "delay" : delay }

        percent_completions = {}
        for output in wfi.request['OutputDatasets']:
            if "/DQM" in output: continue ## that does not count
            if not output in completions: completions[output] = { 'injected' : None, 'checkpoints' : [], 'workflow' : wfo.name}
            ## get completion fraction
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=output)
            lumi_completion=0.
            event_completion=0.
            if lumi_expected:
                lumi_completion = lumi_count / float( lumi_expected )
            if event_expected:
                event_completion = event_count / float( event_expected )

            #take the less optimistic
            percent_completions[output] = min( lumi_completion, event_completion )
            completions[output]['checkpoints'].append( (now, event_completion ) )

        if all([percent_completions[out] >= good_fraction for out in percent_completions]):
            wfi.sendLog('completor', "all is above %s \n%s"%( good_fraction, 
                                                              json.dumps( percent_completions, indent=2 )
                                                              ))
        else:
            long_lasting[wfo.name].update({
                    'completion': sum(percent_completions.values()) / len(percent_completions),
                    'completions' : percent_completions
                    })
            
            ## do something about the agents this workflow is in
            long_lasting[wfo.name]['agents'] = wfi.getAgents()
            wfi.sendLog('completor', "%s not over bound %s\n%s"%(percent_completions.values(), good_fraction,
                                                                 json.dumps( long_lasting[wfo.name]['agents'], indent=2) ))
            continue

        if all([percent_completions[out] >= ignore_fraction for out in percent_completions]):
            print "all is done, just wait a bit"
            continue

        for output in  percent_completions:
            completions[output]['injected'] = then

        #further check on delays
        cpuh = wfi.getComputingTime(unit='d')

        ran_at = wfi.request['SiteWhitelist']
                        
        wfi.sendLog('completor',"Required %s, time spend %s"%( cpuh, delay))
                    
        ##### WILL FORCE COMPLETE BELOW
        # only really force complete after n days

        if delay <= allowed_delay: continue
        ## find ACDCs that might be running
        if max_force>0:
            forceComplete(url, wfi )
            set_force_complete.add( wfo.name )
            print "going for force-complete of",wfo.name
            wfi.sendLog('completor','going for force completing')
            wfi.notifyRequestor("The workflow %s was force completed for running too long"% wfo.name)
            max_force -=1
        else:
            wfi.sendLog('completor',"too many completion this round, cannot force complete")

        ## do it once only for testing
        #break
    
    if set_force_complete:
        sendLog('completor','The followings were set force-complete \n%s'%('\n'.join(set_force_complete)))
        #sendEmail('set force-complete', 'The followings were set force-complete \n%s'%('\n'.join(set_force_complete)))
    
    open('%s/completions.json'%monitor_dir,'w').write( json.dumps( completions , indent=2))
    text="These have been running for long"
    
    open('%s/longlasting.json'%monitor_dir,'w').write( json.dumps( long_lasting, indent=2 ))

    for wf,info in sorted(long_lasting.items(), key=lambda tp:tp[1]['delay'], reverse=True):
        delay = info['delay']
        text += "\n %s : %s days"% (wf, delay)
        if 'completion' in info:
            text += " %d%%"%( info['completion']*100 )

    #if wfs_no_location_in_GQ:
    #    sendEmail('workflow with no location in GQ',"there won't be able to run anytime soon\n%s"%( '\n'.join(wfs_no_location_in_GQ)))

    #sendEmail("long lasting workflow",text)
    ## you can check the log
    print text


if __name__ == "__main__":
    url = reqmgr_url
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
        
    completor(url, spec)

