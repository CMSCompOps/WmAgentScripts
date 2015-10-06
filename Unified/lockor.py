from utils import getWorkflows, findCustodialLocation, workflowInfo
from assignSession import *
import json

url = 'cmsweb.cern.ch'

statuses = ['assignment-approved','assigned','acquired','running-open','running-closed','completed','closed-out']

tier_no_custodial = ['MINIAODSIM']


## those that are already in lock
already_locked = set(json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/globallocks.json').read()))
newly_locking = set()

## you want to take them in reverse order to make sure none go through a transition while you run this 
for status in reversed(statuses):
    wfls = getWorkflows(url , status = status,details=True)
    print len(wfls),"in",status
    for wl in wfls:
        ## unknonw to the system
        if not session.query(Workflow).filter(Workflow.name==wl['RequestName']).all(): continue

        wfi = workflowInfo( url,  wl['RequestName'], request = wl ,spec=False)
        (_,primaries,_,secondaries) = wfi.getIO()
        outputs = wfi.request['OutputDatasets']
        for dataset in list(primaries)+list(secondaries)+outputs:
            if 'FAKE' in dataset: continue
            if 'None' in dataset: continue
            newly_locking.add(dataset)
    print len(newly_locking),"locks so far"

## check on the one left out
for dataset in already_locked-newly_locking:
    (_,dsn,ps,tier) = dataset.split('/')
    if not tier in tier_no_custodial:
        custodials = findCustodialLocation(url, dataset)
        if len(custodials) == 0:
            print "Can't unlock",dataset,"because it is not custodial yet"
            ## add it back for that reason
            newly_locking.add(dataset)
            
open('/afs/cern.ch/user/c/cmst2/www/unified/globallocks.json','w').write( json.dumps( list(newly_locking), indent=2))
