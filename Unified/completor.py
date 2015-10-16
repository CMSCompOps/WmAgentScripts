#!/usr/bin/env python
from assignSession import *
import sys
import reqMgrClient
from utils import workflowInfo, getWorkflowById, getDatasetEventsAndLumis
from utils import campaignInfo
import json
import random

def completor(url, specific):
    
    CI = campaignInfo()

    wfs = []
    wfs.extend( session.query(Workflow).filter(Workflow.status == 'away').all() )
    ##wfs.extend( session.query(Workflow).filter(Workflow.status.startswith('assistance')).all() )

    ## just take it in random order
    random.shuffle( wfs )

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
        
        if not 'TotalInputEvents' in wfi.request: continue

        lumi_expected = wfi.request['TotalInputLumis']
        event_expected = wfi.request['TotalInputEvents']

        percent_completions = {}
        for output in wfi.request['OutputDatasets']:
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

        if all([percent_completions[out] >= good_fraction for out in percent_completions]):
            print "all is above",good_fraction,"for",wfo.name
            print json.dumps( percent_completions, indent=2 )
        else:
            print "\tnot over bound",good_fraction
            #print json.dumps( percent_completions, indent=2 )
            continue

        if all([percent_completions[out] >= ignore_fraction for out in percent_completions]):
            print "all is done, just wait a bit"
            continue

        ## find ACDCs that might be running
        familly = getWorkflowById( url, wfi.request['PrepID'] ,details=True)
        for member in familly:
            ### if member['RequestName'] == wl['RequestName']: continue ## set himself out
            if member['RequestDate'] < wfi.request['RequestDate']: continue
            if member['RequestStatus'] in ['None',None]: continue
            ## then set force complete all members
            if member['RequestStatus'] in ['running-opened','running-closed']:
                print "setting",member['RequestName'],"force-complete"
                reqMgrClient.setWorkflowForceComplete(url, member['RequestName'])

        ## do it once only for testing
        #break
            
            

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
        
    completor(url, spec)

