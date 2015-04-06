from assignSession import *
from utils import getWorkLoad
import sys


if __name__ == "__main__":
    spec = sys.argv[1]
    status = None
    if len(sys.argv)>2:
        status = sys.argv[2]
        
    for wf in session.query(Workflow).all():
        if spec and spec not in wf.name: continue
        if not wf.status in ['away']: continue

        wl = getWorkLoad('cmsweb.cern.ch', wf.name)
        wf.wm_status = wl['RequestStatus']
        if status:
            wf.status = status
        elif wf.wm_status in ['assigned','acquired','running-closed','running-open','completed','closed-out']:
            wf.status = 'away'
        elif wf.wm_status in ['rejected','failed','aborted','aborted-archived','rejected-archived','failed-archived']:
            wf.status = 'trouble'
        elif wf.wm_status in ['announced','normal-arhived']:
            wf.status = 'done'
        #elif wf.wm_status in ['assignment-approved']:
        #    wf.status = 'considered'
        print wf.name, wf.wm_status, wf.status
    session.commit()
