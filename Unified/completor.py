#!/usr/bin/env python
from assignSession import *
import sys
import reqMgrClient
from utils import workflowInfo, getWorkflowById, getDatasetEventsAndLumis
from utils import campaignInfo, sendEmail
import json
import random

def completor(url, specific):
    
    CI = campaignInfo()

    wfs = []
    wfs.extend( session.query(Workflow).filter(Workflow.status == 'away').all() )
    ##wfs.extend( session.query(Workflow).filter(Workflow.status.startswith('assistance')).all() )

    ## just take it in random order so that not always the same is seen
    random.shuffle( wfs )

    ## by workflow a list of fraction / timestamps
    completions = json.loads( open('completions.json').read())
    
    good_fractions = {}
    for c in CI.campaigns:
        if 'force-complete' in CI.campaigns[c]:
            good_fractions[c] = CI.campaigns[c]['force-complete']

    long_lasting = {}

    overrides = {}
    for rider,email in [('jbadillo','julian.badillo.rojas@cern.ch'),('vlimant','vlimant@cern.ch'),('jen_a','jen_a@fnal.gov'),('srimanob','srimanob@mail.cern.ch')]:
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
    max_force = 5
    for wfo in wfs:
        if specific and not specific in wfo.name: continue

        skip=False
        if not any([c in wfo.name for c in good_fractions]): skip=True
        for user,spec in overrides.items():
            if wfo.name in spec:
                print wfo.name,"should be force complete this round by request of",user
                #skip=False ## do not do it automatically yet
                sendEmail('force-complete requested','%s is asking for %s to be force complete'%(user,wfo.name)

        if skip: continue

        print "looking at",wfo.name
        ## get all of the same
        wfi = workflowInfo(url, wfo.name)

        if not 'Campaign' in wfi.request: continue

        if not wfi.request['RequestStatus'] in ['running-open','running-closed']: continue

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
        print "\t"*int(w)+"Running since",delay,"[days]"
        if delay <= 7: continue
        if delay >= 7:
            long_lasting[wfo.name] = { "delay" : delay }
            pass

        if delay <= 14: continue

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

        ## find ACDCs that might be running
        familly = getWorkflowById( url, wfi.request['PrepID'] ,details=True)
        for member in familly:
            ### if member['RequestName'] == wl['RequestName']: continue ## set himself out
            if member['RequestDate'] < wfi.request['RequestDate']: continue
            if member['RequestStatus'] in ['None',None]: continue
            ## then set force complete all members
            if member['RequestStatus'] in ['running-opened','running-closed']:
                if max_force >0:
                    #sendEmail("force completing","%s is worth force completing\n%s"%( member['RequestName'] , percent_completions))
                    print "setting",member['RequestName'],"force-complete"
                    reqMgrClient.setWorkflowForceComplete(url, member['RequestName'])
                    max_force -=1
                else:
                    print "NOT REALLY FORCING",member['RequestName']

        ## do it once only for testing
        #break
            
    open('completions.json','w').write( json.dumps( completions , indent=2))
    text="These have been running for long"
    
    open('longlasting.json','w').write( json.dumps( long_lasting, indent=2 ))

    for wf,info in sorted(long_lasting.items(), key=lambda tp:tp[1]['delay'], reverse=True):
        delay = info['delay']
        text += "\n %s : %s days"% (wf, delay)
        if 'completion' in info:
            text += " %d%%"%( info['completion']*100 )

    #sendEmail("long lasting workflow",text)
    ## you can check the log
    print text


if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
        
    completor(url, spec)

