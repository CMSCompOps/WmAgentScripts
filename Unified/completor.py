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

    ## just take it in random order
    random.shuffle( wfs )

    ## by workflow a list of fraction / timestamps
    completions = json.loads( open('completions.json').read())
    
    good_fractions = {}
    for c in CI.campaigns:
        if 'force-complete' in CI.campaigns[c]:
            good_fractions[c] = CI.campaigns[c]['force-complete']

    print "can force complete on"
    print json.dumps( good_fractions ,indent=2)
    for wfo in wfs:
        if specific and not specific in wfo.name: continue

        if not any([c in wfo.name for c in good_fractions]): continue

        print "looking at",wfo.name
        ## get all of the same
        wfi = workflowInfo(url, wfo.name)

        if not 'Campaign' in wfi.request: continue
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

        if delay <= 21: 
            print "\tRunning since",delay,"[days]"
            continue

        if delay >= 14:
            sendEmail("long lasting workflow","%s has been running for %s days"%( wfo.name, delay ))

        percent_completions = {}
        for output in wfi.request['OutputDatasets']:
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
            #print json.dumps( percent_completions, indent=2 )
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
                print "setting",member['RequestName'],"force-complete"
                print "NOT REALLY FORCING"
                sendEmail("force completing","TAGGING %s is worth force completing\n%s"%( member['RequestName'] , percent_completions))
                ##reqMgrClient.setWorkflowForceComplete(url, member['RequestName'])

        ## do it once only for testing
        #break
            
    open('completions.json','w').write( json.dumps( completions , indent=2))


if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
        
    completor(url, spec)

