from utils import getWorkflows, findCustodialLocation, workflowInfo, getDatasetStatus, getWorkflowByOutput
from assignSession import *
import json
import os

url = 'cmsweb.cern.ch'

statuses = ['assignment-approved','assigned','failed','acquired','running-open','running-closed','force-complete','completed','closed-out']

tier_no_custodial = ['MINIAODSIM','DQM','DQMIO']


## those that are already in lock
already_locked = set(json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/globallocks.json').read()))
if not already_locked:
    old = json.loads(open('datalocks.json').read())
    for site,locks in old.items():
        if type(locks) == float: continue
        for item,info in locks.items():
            if info['lock']==False: continue
            already_locked.add( item.split('#')[0] )
    print "found",len(already_locked),"old locks"

newly_locking = set()

## you want to take them in reverse order to make sure none go through a transition while you run this 
for status in reversed(statuses):
    wfls = getWorkflows(url , status = status,details=True)
    print len(wfls),"in",status
    for wl in wfls:
        ## unknonw to the system
        if not session.query(Workflow).filter(Workflow.name==wl['RequestName']).all(): 
            print wl['RequestName'],"is unknown, this is bad news"
            continue

        wfi = workflowInfo( url,  wl['RequestName'], request = wl ,spec=False)
        (_,primaries,_,secondaries) = wfi.getIO()
        outputs = wfi.request['OutputDatasets']
        for dataset in list(primaries)+list(secondaries)+outputs:
            if 'FAKE' in dataset: continue
            if 'None' in dataset: continue
            newly_locking.add(dataset)
    print len(newly_locking),"locks so far"

## check on the one left out, which would seem to get unlocked
for dataset in already_locked-newly_locking:
    try:
        unlock=False
        creators = getWorkflowByOutput( url, dataset , details=True)
        creators_status = [r['RequestStatus'] for r in creators]
        print "Statuses of workflow that made the dataset",creators_status
        if all(status in ['failed','aborted','rejected','aborted-archived','rejected-archived'] for status in creators_status):
            ## crap 
            print "\tunlocking",dataset,"for bad workflow statuses"
            unlock=True

        ds_status=None
        if not unlock:
            ds_status = getDatasetStatus( dataset )
            if ds_status in ['INVALID']: 
                ## don't even try to keep the lock
                print "\tunlocking",dataset,"for bad dataset status",ds_status
                unlock=True

        if not unlock:
            (_,dsn,ps,tier) = dataset.split('/')
            unlock = True
            if not tier in tier_no_custodial:
                custodials = findCustodialLocation(url, dataset)
                if len(custodials) == 0:
                    if not ds_status: ds_status = getDatasetStatus( dataset )
                    print "Can't unlock",dataset,"because it is not custodial yet",ds_status
                    ## add it back for that reason
                    newly_locking.add(dataset)
                    unlock = False

        if unlock:
            print "\tunlocking",dataset
            ##would like to pass to *-unlock, or even destroy from local db
            for creator in creators:
                for wfo in  session.query(Workflow).filter(Workflow.name==creator['RequestName']).all():
                    if not 'unlock' in wfo.status and not wfo.status in ['trouble','away']:
                        wfo.status +='-unlock'
                        print "setting",wfo.name,"to",wfo.status
            session.commit()
    except Exception as e:
        print "Error in checking unlockability. relocking",dataset
        print str(e)
        newly_locking.add(dataset)
            
open('/afs/cern.ch/user/c/cmst2/www/unified/globallocks.json.new','w').write( json.dumps( list(newly_locking), indent=2))
os.system('mv /afs/cern.ch/user/c/cmst2/www/unified/globallocks.json.new /afs/cern.ch/user/c/cmst2/www/unified/globallocks.json')


## now settting the statuses locally right
## this below does not function because you cannot -unlock the workflow that uses the dataset in input. 
# so doing it by output only, as above, is enough and sufficient.
#for dataset in unlocking:
#    pass
    ## if a dataset goes out of lock, you want to unlock everything about it
    #actors = getWorkflowByOutput( url, dataset , details=True)
    #actors.extend( getWorkflowByInput( url, dataset , details=True))
    ## adding secondaries ???
    #actors.extend( getWorkflowByPileup( url, dataset , details=True))
    #for actor in actors:
    #    for wfo in  session.query(Workflow).filter(Workflow.name==actor['RequestName']).all():
    #        if not 'unlock' in wfo.status:
    #            wfo.status +='-unlock'
    #            print "setting",wfo.name,"to",wfo.status
    #session.commit()
