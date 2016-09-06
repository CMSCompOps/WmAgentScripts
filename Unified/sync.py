#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, reqmgr_url
import sys
import optparse

parser = optparse.OptionParser()
parser.add_option('-w',help='the workflow name to change the status of')
parser.add_option('-s',help='the new status to be set')
parser.add_option('--comments',help='an additional comment to register in the unified log',default=None)
(options,args) = parser.parse_args()
url = reqmgr_url

if __name__ == "__main__":
    spec = options.w
    status = options.s
    comment = options.comments

    if not status:
        print "need to pass -s"
        sys.exit(0)
    if not spec:
        print "need to pass -w"
        sys.exit(0)

    #for wf in session.query(Workflow).all():
    for wf in session.query(Workflow).filter(Workflow.name.contains(spec)).all():
        if spec and spec not in wf.name: continue
        #if not wf.status in ['away']: continue

        old_status = wf.status 

        wfi = workflowInfo(url, wf.name)
        wf.wm_status = wfi.request['RequestStatus']

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

        wfi.sendLog('sync','Setting status from %s to %s (%s)%s'%(old_status,wf.status,wf.wm_status,('\nreason: %s'%(comment) if comment else "")))
    session.commit()
