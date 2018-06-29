from utils import getWorkflows, sendEmail, reqmgr_url
import reqMgrClient
import time
from collections import defaultdict

url = reqmgr_url

## clean out backfills after N days
now = time.mktime(time.gmtime())
def transitiontime(wf, status):
    logs= filter(lambda change : change["Status"]==status, wf['RequestTransition'])
    if logs:
        return logs[-1]['UpdateTime']
    else:
        return None

delays={'assignment-approved' : (7,14),
        'new':(7,14),
        'completed':(14,21),
        'closed-out':(14,21),
        }

warnings=defaultdict(set)
for checkin,(warn,timeout) in delays.items():
    wfs = getWorkflows(url, checkin, user=None, details=True)
    for wf in wfs:
        if not 'backfill' in wf['RequestName'].lower(): continue
        transition = transitiontime(wf,checkin)
        if transition and (now - transition)>(timeout*24*60*60):
            ## that can go away
            print wf['RequestName'],"is old enough to be removed",wf['RequestStatus']
            reqMgrClient.invalidateWorkflow(url, wf['RequestName'], current_status=wf['RequestStatus'])
        elif transition and (now - transition)>(warn*24*60*60):
            ## warn requester
            print wf['RequestName'],"is old enough to be removed",wf['RequestStatus']
            warnings[wf['Requestor']].add( wf['RequestName'] )

for who in warnings:
    sendEmail('Old Backfill in the system','The following backfill should be removed or moved to rejected/announced\n\n%s'%('\n'.join(sorted(warnings[who]))), destination=[who+'@cern.ch'])

