#!/usr/bin/env python
from utils import  componentInfo, reqmgr_url, getWorkflowById
import sys
from JIRAClient import JIRAClient

up = componentInfo(soft=['mcm','wtc','jira'])
if not up.check(): sys.exit(0)

JC = JIRAClient() if up.status.get('jira',False) else None
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
