#!/usr/bin/env python
from utils import getWorkflows, findCustodialCompletion, workflowInfo, getDatasetStatus, getWorkflowByOutput, unifiedConfiguration, getDatasetSize, sendEmail, campaignInfo
from assignSession import *
import json
import os
from collections import defaultdict

url = 'cmsweb.cern.ch'

statuses = ['assignment-approved','assigned','failed','acquired','running-open','running-closed','force-complete','completed','closed-out']

UC = unifiedConfiguration()
CI = campaignInfo()
tier_no_custodial = UC.get('tiers_with_no_custodial')
## can we catch the datasets that actually should go to tape ?
custodial_override = {}
for c in CI.campaigns:
    if 'custodial_override' in CI.campaigns[c]:
        custodial_override[c] = CI.campaigns[c]['custodial_override']


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
        known = session.query(Workflow).filter(Workflow.name==wl['RequestName']).all()
        if not known: 
            #print wl['RequestName'],"is unknown, this is bad news" ## no it is not
            continue

        if status == 'assignment-approved':
            if all([wfo.status == 'considered' for wfo in known]):
                ## skip those only assignment-approved / considered
                continue

        wfi = workflowInfo( url,  wl['RequestName'], request = wl ,spec=False)
        (_,primaries,_,secondaries) = wfi.getIO()
        outputs = wfi.request['OutputDatasets']

        for dataset in list(primaries)+list(secondaries)+outputs:
            if 'FAKE' in dataset: continue
            if 'None' in dataset: continue
            newly_locking.add(dataset)
    print len(newly_locking),"locks so far"

waiting_for_custodial={}
stuck_custodial={}
lagging_custodial={}
missing_approval_custodial={}
transfer_timeout = UC.get("transfer_timeout")
## check on the one left out, which would seem to get unlocked
for dataset in already_locked-newly_locking:
    try:
        unlock=False
        creators = getWorkflowByOutput( url, dataset , details=True)
        if not creators and not dataset.endswith('/RAW'):
            ds_status = getDatasetStatus( dataset )
            if not '-v0/' in dataset and ds_status!=None:
                sendEmail('failing get by output','%s has not been produced by anything?'%dataset)
                newly_locking.add(dataset)
                continue
            else:
                # does not matter, cannot be an OK dataset
                unlock=True
        creators_status = [r['RequestStatus'] for r in creators]
        print "Statuses of workflow that made the dataset",creators_status
        if all([status in ['failed','aborted','rejected','aborted-archived','rejected-archived'] for status in creators_status]):
            ## crap 
            print "\tunlocking",dataset,"for bad workflow statuses"
            unlock=True
            
        ds_status=None
        if not unlock:
            ds_status = getDatasetStatus( dataset )

            if ds_status in ['INVALID',None]: 
                ## don't even try to keep the lock
                print "\tunlocking",dataset,"for bad dataset status",ds_status
                unlock=True

        
        if not unlock:
            (_,dsn,ps,tier) = dataset.split('/')
            no_tape = (tier in tier_no_custodial)
            if no_tape:
                for c in custodial_override:
                    if c in ps and tier in custodial_override[c]:
                        no_tape=False
                        break
            if no_tape:
                ## could add a one-full copy consistency check
                unlock = True
            else:
                custodials,info = findCustodialCompletion(url, dataset)
                if len(custodials) == 0:
                    ## add it back for that reason
                    newly_locking.add(dataset)
                    if not ds_status: ds_status = getDatasetStatus( dataset )
                    ds_size = getDatasetSize( dataset )
                    print "Can't unlock",dataset," of size", ds_size,"[GB] because it is not custodial yet",ds_status
                    unlock = False
                    if info:
                        waiting_for_custodial[dataset] = info
                        waiting_for_custodial[dataset]['size']=ds_size

                        if info['nmissing'] == 1 and info['nblocks']>1:
                            for node,node_info in info['nodes'].items():
                                if node_info['decided'] and (info['checked'] - node_info['decided'])>(transfer_timeout*24.*60*60):
                                    ## stuck tape transfer, with only one block missing, typical of a blocked situation
                                    stuck_custodial[dataset] = {'size' : ds_size, 'since' : (info['checked'] - node_info['decided'])/(24.*60*60), 'nodes' : info['nodes'], 'nmissing': info['nmissing']}

                        for node,node_info in info['nodes'].items(): 
                            if not node_info['decided'] and (info['checked'] - node_info['created'])>(transfer_timeout*24.*60*60):
                                ## stuck in approval : missing operation
                                missing_approval_custodial[dataset] = {'size' : ds_size, 'since' : (info['checked'] - node_info['created'])/(24.*60*60), 'nodes' : info['nodes']}
                            if node_info['decided'] and (info['checked'] - node_info['decided'])>(transfer_timeout*24.*60*60):
                                ## not completed 7 days after approval
                                lagging_custodial[dataset] = {'size' : ds_size, 'since' : (info['checked'] - node_info['decided'])/(24.*60*60), 'nodes' : info['nodes'], 'nmissing': info['nmissing']}
                    else:
                        ## there was no information about missing blocks
                        ## last time you checked this was a lost dataset
                        sendEmail('dataset waiting for custodial with no block','%s looks very odd'% dataset)
                        #unlock = True
                        pass
                else:
                    unlock = True
        if unlock:
            print "\tunlocking",dataset
            ##would like to pass to *-unlock, or even destroy from local db
            for creator in creators:
                for wfo in  session.query(Workflow).filter(Workflow.name==creator['RequestName']).all():
                    if not 'unlock' in wfo.status and not any([wfo.status.startswith(key) for key in ['trouble','away','considered','assistance']]):
                        wfo.status +='-unlock'
                        print "setting",wfo.name,"to",wfo.status
            session.commit()
        else:
            newly_locking.add(dataset)            
    except Exception as e:
        print "Error in checking unlockability. relocking",dataset
        print str(e)
        newly_locking.add(dataset)

waiting_for_custodial_sum = sum([info['size'] for ds,info in waiting_for_custodial.items() if 'size' in info])
print waiting_for_custodial_sum,"[GB] out there waiting for custodial"
open('/afs/cern.ch/user/c/cmst2/www/unified/waiting_custodial.json','w').write( json.dumps( waiting_for_custodial , indent=2) )
open('/afs/cern.ch/user/c/cmst2/www/unified/stuck_custodial.json','w').write( json.dumps( stuck_custodial , indent=2) )
open('/afs/cern.ch/user/c/cmst2/www/unified/lagging_custodial.json','w').write( json.dumps( lagging_custodial , indent=2) )
open('/afs/cern.ch/user/c/cmst2/www/unified/missing_approval_custodial.json','w').write( json.dumps( missing_approval_custodial , indent=2) )

## then for all that would have been invalidated from the past, check whether you can unlock the wf based on output
for wfo in session.query(Workflow).filter(Workflow.status=='forget').all():
    wfi = workflowInfo(url, wfo.name)
    if all([o not in newly_locking for o in wfi.request['OutputDatasets']]) and not 'unlock' in wfo.status:
        wfo.status +='-unlock'
        print "then setting",wfo.name,"to",wfo.status
    session.commit()


        
            
open('/afs/cern.ch/user/c/cmst2/www/unified/globallocks.json.new','w').write( json.dumps( sorted(list(newly_locking)), indent=2))
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
