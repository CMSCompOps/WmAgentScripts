#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, reqmgr_url
import sys
import argparse
import getpass
username = getpass.getuser()

parser = argparse.ArgumentParser()
parser.add_argument('-w', type=str, action='store', required=False, help='The workflow name to change the status of')
parser.add_argument('-s', type=str, action='store', required=True, help='The new status to be set')
parser.add_argument('--comments', type=str, action='store', required=True, help='Reason for manual intervention, to be registered in the Unified log')
parser.add_argument('-f', type=str, action='store', required=False, help='Filelist')
options = parser.parse_args()
url = reqmgr_url

if __name__ == "__main__":
    spec = options.w
    status = options.s
    comment = options.comments + " - {}".format(username)
    filelist = options.f
    if not status:
        print "need to pass -s"
        sys.exit(0)
    if not spec:
        if not filelist:
            print "need to pass -w"
            sys.exit(0)

    if filelist:
        for spec in filter(None, open(filelist).read().split('\n')):
            print spec
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

                wfi.sendLog('sync','Setting status from {} to {} ({}). Reason: {}'.format(old_status,wf.status,wf.wm_status, comment)) # Please always put reason
            session.commit()

    else:

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

            wfi.sendLog('sync','Setting status from {} to {} ({}). Reason: {}'.format(old_status,wf.status,wf.wm_status, comment)) # Please always put reason
        session.commit()
