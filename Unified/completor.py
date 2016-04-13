#!/usr/bin/env python
from assignSession import *
import sys
import reqMgrClient
from utils import workflowInfo, getWorkflowById, getDatasetEventsAndLumis, componentInfo, monitor_dir, reqmgr_url
from utils import campaignInfo, sendEmail, siteInfo
import json
import random


def forceComplete(url, wfi):
    familly = getWorkflowById( url, wfi.request['PrepID'] ,details=True)
    for member in familly:
        ### if member['RequestName'] == wl['RequestName']: continue ## set himself out
        if member['RequestDate'] < wfi.request['RequestDate']: continue
        if member['RequestStatus'] in ['None',None]: continue
        ## then set force complete all members
        if member['RequestStatus'] in ['running-opened','running-closed']:
            #sendEmail("force completing","%s is worth force completing\n%s"%( member['RequestName'] , percent_completions))
            print "setting",member['RequestName'],"force-complete"
            reqMgrClient.setWorkflowForceComplete(url, member['RequestName'])
        elif member['RequestStatus'] in ['acquired','assignment-approved']:
            print "rejecting",member['RequestName']
            reqMgrClient.invalidateWorkflow(url, member['RequestName'], current_status=member['RequestStatus'])

def completor(url, specific):
    up = componentInfo(mcm=False, soft=['mcm'])
    if not up.check(): return

    CI = campaignInfo()
    SI = siteInfo()

    wfs = []
    wfs.extend( session.query(Workflow).filter(Workflow.status == 'away').all() )
    wfs.extend( session.query(Workflow).filter(Workflow.status.startswith('assistance')).all() )

    ## just take it in random order so that not always the same is seen
    random.shuffle( wfs )

    ## by workflow a list of fraction / timestamps
    completions = json.loads( open('%s/completions.json'%monitor_dir).read())
    
    good_fractions = {}
    for c in CI.campaigns:
        if 'force-complete' in CI.campaigns[c]:
            good_fractions[c] = CI.campaigns[c]['force-complete']

    long_lasting = {}

    overrides = {}
    for rider,email in [('vlimant','vlimant@cern.ch'),('jen_a','jen_a@fnal.gov'),('srimanob','srimanob@mail.cern.ch')]:
        rider_file = '/afs/cern.ch/user/%s/%s/public/ops/forcecomplete.json'%(rider[0],rider)
        if not os.path.isfile(rider_file):
            print "no file",rider_file
            continue
        try:
            overrides[rider] = json.loads(open( rider_file ).read() )
        except:
            print "cannot get force complete list from",rider
            sendEmail("malformated force complet file","%s is not json readable"%rider_file, destination=[email])
        

    print "can force complete on"
    print json.dumps( good_fractions ,indent=2)
    print json.dumps( overrides, indent=2)
    max_force = 5
    
    wfs_no_location_in_GQ = set()

    for wfo in wfs:
        if specific and not specific in wfo.name: continue

        print "looking at",wfo.name
        ## get all of the same
        wfi = workflowInfo(url, wfo.name)

        skip=False
        if not any([c in wfo.name for c in good_fractions]): skip=True
        for user,spec in overrides.items():
            #print spec
            if wfo.name in spec and wfi.request['RequestStatus']!='force-complete':
                #skip=False ## do not do it automatically yet
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
        if delay <= 7: continue
        if delay >= 7:
            long_lasting[wfo.name] = { "delay" : delay }
            pass

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
            print "all is above",good_fraction,"for",wfo.name
            print json.dumps( percent_completions, indent=2 )
        else:
            print "\t",percent_completions.values(),"not over bound",good_fraction
            long_lasting[wfo.name].update({
                    'completion': sum(percent_completions.values()) / len(percent_completions),
                    'completions' : percent_completions
                    })
            
            #print json.dumps( percent_completions, indent=2 )

            ## do something about the agents this workflow is in
            long_lasting[wfo.name]['agents'] = wfi.getAgents()
            print json.dumps( long_lasting[wfo.name]['agents'], indent=2)

            ## pick up on possible issue with global queue data location
            locs = wfi.getGQLocations()
            for b,loc in locs.items():
                if not loc:
                    print b,"has no location for GQ in",wfi.request['RequestName']
                    ## this is severe !
                    wfs_no_location_in_GQ.add( wfo.name )
                ## check location and site white list
                can_run = set([SI.SE_to_CE(se) for se in loc])&set(wfi.request['SiteWhitelist'])
                if loc and not can_run:
                    print b,"is missing site to run within the whitelist"
                    wfs_no_location_in_GQ.add( wfo.name )
                can_run = can_run & set(SI.sites_ready)
                if loc and not can_run:
                    print b,"is missing available site to run"
                    wfs_no_location_in_GQ.add( wfo.name )
            continue

        if all([percent_completions[out] >= ignore_fraction for out in percent_completions]):
            print "all is done, just wait a bit"
            continue

        for output in  percent_completions:
            completions[output]['injected'] = then

        #further check on delays
        cpuh = wfi.getComputingTime(unit='d')

        ran_at = wfi.request['SiteWhitelist']
        print "Required:",cpuh,
        print "Time spend:",delay

        # only really force complete after n days
        if delay <= 14: continue
        print "going for force-complete of",wfo.name
        ## find ACDCs that might be running
        if max_force>0:
            forceComplete(url, wfi )
            max_force -=1
        else:
            print "too many completion this round"
        ## do it once only for testing
        #break
            
    open('%s/completions.json'%monitor_dir,'w').write( json.dumps( completions , indent=2))
    text="These have been running for long"
    
    open('%s/longlasting.json'%monitor_dir,'w').write( json.dumps( long_lasting, indent=2 ))

    for wf,info in sorted(long_lasting.items(), key=lambda tp:tp[1]['delay'], reverse=True):
        delay = info['delay']
        text += "\n %s : %s days"% (wf, delay)
        if 'completion' in info:
            text += " %d%%"%( info['completion']*100 )

    if wfs_no_location_in_GQ:
        sendEmail('workflow with no location in GQ',"there won't be able to run anytime soon\n%s"%( '\n'.join(wfs_no_location_in_GQ)))

    #sendEmail("long lasting workflow",text)
    ## you can check the log
    print text


if __name__ == "__main__":
    url = reqmgr_url
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
        
    completor(url, spec)

