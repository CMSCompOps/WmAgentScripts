#!/usr/bin/env python
from assignSession import *
import sys
import reqMgrClient
from utils import getWorkLoad, getWorkflowById

def completor(url, specific):
    wfs = []
    wfs.extend( session.query(Workflow).filter(Workflow.status == 'away').all() )
    wfs.extend( session.query(Workflow).filter(Workflow.status.startswith('assistance')).all() )

    for wfo in wfs:
        if specific and not specific in wfo.name: continue

        ## get all of the same
        wl = getWorkLoad(url, wfo.name)
        familly = getWorkflowById( url, wl['PrepID'] ,details=True)
        for member in familly:
            ### if member['RequestName'] == wl['RequestName']: continue
            if member['RequestDate'] < wl['RequestDate']: continue
            if member['RequestStatus'] in ['None',None]: continue
            ## then set force complete all members
            if member['RequestStatus'] in ['running-opened','running-closed']:
                print "setting",member['RequestName'],"force-complete"
                reqMgrClient.setWorkflowForceComplete(url, member['RequestName'])
                
            

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
        
    completor(url, spec)

