#!/usr/bin/env python
from utils import  reqmgr_url, getWorkflowById
from JIRAClient import JIRAClient

JC = JIRAClient()
if JC:
    those = JC.find({'status' : '!CLOSED'})
    for t in those:
        s= t.fields.summary
        s = s.replace('issues','')
        s = s.strip()
        if s.count(' ')!=0: continue
        print s
        wfs = getWorkflowById(reqmgr_url, s, details=True)
        statuses = set([r['RequestStatus'] for r in wfs])
        check_against = ['assignment-approved', 'running-open','running-closed','completed','acquired', 'staging', 'staged', 'assigned', 'closed-out', 'failed']
        if statuses:
            if all([s not in check_against for s in statuses]):
                print t.key,"can be closed"
                print statuses
                JC.close(t.key) ## uncomment to close JIRAs
                continue
        print t.key,statuses
