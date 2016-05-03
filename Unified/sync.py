#!/usr/bin/env python
from assignSession import *
from utils import getWorkLoad, reqmgr_url
import sys

url = reqmgr_url

if __name__ == "__main__":
    spec = sys.argv[1]
    status = None
    if len(sys.argv)>2:
        status = sys.argv[2]
        
    for wf in session.query(Workflow).all():
        if spec and spec not in wf.name: continue
        #if not wf.status in ['away']: continue

        wl = getWorkLoad(url, wf.name)
        wf.wm_status = wl['RequestStatus']

        if status:
            if status == 'DELETE':
                print "removing",wf.name
                session.delete( wf )
            else:
                wf.status = status
        elif wf.wm_status in ['assignment-approved']:
            wf.status = 'considered'
        elif wf.wm_status in ['assigned','acquired','running-closed','running-open','completed']:
            if not wf.status.startswith('assistance'):
                wf.status = 'away'
            else:
                print wf.name,"is still in",wf.status
        elif wf.wm_status in ['closed-out']:
            wf.status = 'close'
        elif wf.wm_status in ['rejected','failed','aborted','aborted-archived','rejected-archived','failed-archived']:
            wf.status = 'trouble'
        elif wf.wm_status in ['announced','normal-arhived']:
            wf.status = 'done'
        #elif wf.wm_status in ['assignment-approved']:
        #    wf.status = 'considered'
        print wf.name, wf.wm_status, wf.status
    session.commit()
