#!/usr/bin/env python
from utils import getWorkflows, findCustodialCompletion, workflowInfo, getDatasetStatus, getWorkflowByOutput, unifiedConfiguration, getDatasetSize, sendEmail, sendLog, campaignInfo, componentInfo, reqmgr_url, monitor_dir, monitor_pub_dir, getWorkflowByMCPileup, getDatasetPresence, lockInfo, getLatestMCPileup
from assignSession import *
import json
import os
from collections import defaultdict
import sys
from McMClient import McMClient
import time
from utils import lockInfo



now_s = time.mktime(time.gmtime())
def time_point(label="",sub_lap=False, percent=None):
    now = time.mktime(time.gmtime())
    nows = time.asctime(time.gmtime())
    
    print "[lockor] Time check (%s) point at : %s"%(label, nows)
    if percent:
        print "[lockor] finishing in about %.2f [s]" %( (now - time_point.start) / percent )
        print "[lockor] Since start: %s [s]"% ( now - time_point.start)
    if sub_lap:
        print "[lockor] Sub Lap : %s [s]"% ( now - time_point.sub_lap ) 
        time_point.sub_lap = now
    else:
        print "[lockor] Lap : %s [s]"% ( now - time_point.lap ) 
        time_point.lap = now            
        time_point.sub_lap = now

time_point.sub_lap = time_point.lap = time_point.start = time.mktime(time.gmtime())


time_point("Starting initialization")

url = reqmgr_url

use_mcm=True
up = componentInfo(mcm=use_mcm, soft=['mcm'])
if not up.check(): sys.exit(0)

use_mcm = up.status['mcm']
mcm=None
if use_mcm:
    print "mcm interface is up"
    mcm = McMClient(dev=False)

statuses = ['assignment-approved','assigned','failed','acquired','running-open','running-closed','force-complete','completed','closed-out']

UC = unifiedConfiguration()
CI = campaignInfo()
tier_no_custodial = UC.get('tiers_with_no_custodial')
tiers_keep_on_disk = UC.get("tiers_keep_on_disk")

now = time.mktime( time.gmtime())

## can we catch the datasets that actually should go to tape ?
custodial_override = {}
for c in CI.campaigns:
    if 'custodial_override' in CI.campaigns[c]:
        custodial_override[c] = CI.campaigns[c]['custodial_override']

newly_locking = set()
also_locking_from_reqmgr = set()

LI = lockInfo()

## add an addHoc list of things to lock. empyting this list would result in unlocking later
addHocLocks = json.loads( open('addhoc_lock.json').read())

time_point("Starting addhoc")

for item in addHocLocks:
    ds = item.split('#')[0]
    LI.lock( ds , reason='addhoc lock')
    newly_locking.add( ds )


time_point("Starting reversed statuses check")

for status in statuses:
    print time.asctime(time.gmtime()),"CEST, fetching",status
    time_point("checking %s" % status, sub_lap=True)
    wfls = getWorkflows(url , status = status,details=True)
    print len(wfls),"in",status
    for wl in wfls:
        wfi = workflowInfo( url,  wl['RequestName'], request = wl ,spec=False)
        (_,primaries,_,secondaries) = wfi.getIO()
        outputs = wfi.request['OutputDatasets']
        ## unknonw to the system
        known = session.query(Workflow).filter(Workflow.name==wl['RequestName']).all()
        if not known: 
            print wl['RequestName'],"is unknown to unified, relocking all I/O"
            for dataset in list(primaries)+list(secondaries)+outputs:
                also_locking_from_reqmgr.add( dataset )
            continue

        if status == 'assignment-approved':
            if all([wfo.status.startswith('considered') for wfo in known]):
                ## skip those only assignment-approved / considered
                continue



        for dataset in list(primaries)+list(secondaries)+outputs:
            if 'FAKE' in dataset: continue
            if 'None' in dataset: continue
            newly_locking.add(dataset)
    print len(newly_locking),"locks so far"


## avoid duplicates
also_locking_from_reqmgr = also_locking_from_reqmgr - newly_locking
print "additional lock for workflows not knonw to unified",len(also_locking_from_reqmgr)

waiting_for_custodial={}
stuck_custodial={}
lagging_custodial={}
missing_approval_custodial={}
transfer_timeout = UC.get("transfer_timeout")

## those that are already in lock
#already_locked = set(json.loads(open('%s/globallocks.json'%monitor_dir).read()))
already_locked = set( LI.items() )


if not already_locked:
    old = json.loads(open('datalocks.json').read())
    for site,locks in old.items():
        if type(locks) == float: continue
        for item,info in locks.items():
            if info['lock']==False: continue
            already_locked.add( item.split('#')[0] )
    print "found",len(already_locked),"old locks"

time_point("Starting to check for unlockability")

secondary_timeout = getLatestMCPileup(url)
time_point("Got the age of all secondaries")

## check on the one left out, which would seem to get unlocked
for dataset in already_locked-newly_locking:
    try:
        if not dataset:continue
        unlock = False
        bad_ds = False
        time_point("Checking %s" % dataset)
        
        if dataset in secondary_timeout:
            delay_days = UC.get('secondary_lock_timeout')
            delay = delay_days*24*60*60 # in days
            if (now-secondary_timeout[dataset])>delay:
                print "unlocking secondary input after",delay_days,"days"
                unlock = True
            else:
                print "keep a lock on secondary within",delay_days,"days"
                unlock = False
                newly_locking.add(dataset)
                continue

        time_point("Checked as useful secondary", sub_lap=True)

        tier = dataset.split('/')[-1]
        creators = getWorkflowByOutput( url, dataset , details=True)
        if not creators and not tier == 'RAW' and not '-PromptReco-' in dataset:
            ds_status = getDatasetStatus( dataset )
            if not '-v0/' in dataset and ds_status!=None:
                #sendEmail('failing get by output','%s has not been produced by anything?'%dataset)
                sendLog('lockor','failing get by output, %s has not been produced by anything?'%dataset, level='critical')
                newly_locking.add(dataset)
                continue
            else:
                # does not matter, cannot be an OK dataset
                unlock = True
                bad_ds = True
        creators_status = [r['RequestStatus'] for r in creators]
        print "Statuses of workflow that made the dataset",dataset,"are",creators_status
        if len(creators_status) and all([status in ['failed','aborted','rejected','aborted-archived','rejected-archived'] for status in creators_status]):
            ## crap 
            print "\tunlocking",dataset,"for bad workflow statuses"
            unlock = True
            bad_ds = True

        time_point("Check as necessary output", sub_lap=True)

        ds_status=None
        if not unlock:
            ds_status = getDatasetStatus( dataset )

            if ds_status in ['INVALID']:#,None]: 
                ## don't even try to keep the lock
                print "\tunlocking",dataset,"for bad dataset status",ds_status
                unlock = True
                bad_ds = True

        time_point("Checked status", sub_lap=True)
        if not bad_ds:
            ## get a chance at unlocking if custodial is existing
            (_,dsn,ps,tier) = dataset.split('/')
            no_tape = (tier in tier_no_custodial)
            if no_tape:
                for c in custodial_override:
                    if c in ps and tier in custodial_override[c]:
                        no_tape=False
                        break
            if no_tape:
                ## could add a one-full copy consistency check
                presence = getDatasetPresence(url, dataset)
                if any([there for _,(there,_) in presence.items()]):
                    unlock = True
                else:
                    newly_locking.add(dataset)
                    unlock = False
                time_point("Checked presence", sub_lap=True)
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
                        #sendEmail('dataset waiting for custodial with no block','%s looks very odd'% dataset)
                        sendLog('dataset waiting for custodial with no block, %s looks very odd'% dataset, level='critical')
                        #unlock = True
                        pass
                    time_point("Check for not on tape yet", sub_lap=True)
                else:
                    unlock = True

        if not bad_ds and unlock and tier in tiers_keep_on_disk:
            ## now check with mcm if possible to relock the dataset
            if use_mcm:
                requests_using = mcm.getA('requests',query='input_dataset=%s'%dataset)
                pending_requests_using = filter(lambda req: req['status'] not in ['submitted','done'], requests_using)
                if len(pending_requests_using):
                    print "relocking",dataset,"because of",len(requests_using),"using it",",".join( [req['prepid'] for req in pending_requests_using] )
                    unlock=False
                elif len(requests_using):
                    print "unlocking",dataset,"because no pending request is using it in mcm"
                    ## no one is using it
                    unlock=True
                else:
                    #print "cannot unlock",dataset,"because no request seems to be using it"
                    #unlock=False                    
                    print "Unlocking",dataset,"because no request is using it in input"
                    unlock=True
                time_point("Checked with mcm for useful input", sub_lap=True)
            else:
                ## relocking
                outs = session.query(Output).filter(Output.datasetname==dataset).all()
                delay_days = 30
                delay = delay_days*24*60*60 # 30 days
                if outs:
                    odb = outs[0]
                    if (now-odb.date) > delay: #all([(now-odb.date) > delay for odb in outs]):
                        unlock = True
                        print "unlocking",dataset,"after",(now-odb.date)/24*60*60,"[days] since announcement, limit is",delay_days,"[days]"
                    else:
                        unlock = False
                        print "re-locking",dataset,"because ",delay_days,"[days] expiration date is not passed, now:",now,"announced",odb.date,":",(now-odb.date)/24*60*60,"[days]"
                else:
                    print "re-Locking",dataset,"because of special tier needing double check"
                    unlock=False
                time_point("Checked to keep on disk for 30 days", sub_lap=True)

        if unlock:
            print "\tunlocking",dataset
            ##would like to pass to *-unlock, or even destroy from local db
            for creator in creators:
                for wfo in  session.query(Workflow).filter(Workflow.name==creator['RequestName']).all():
                    #if not 'unlock' in wfo.status and not any([wfo.status.startswith(key) for key in ['trouble','away','considered','staging','staged','assistance']]):
                    if not 'unlock' in wfo.status and any([wfo.status.startswith(key) for key in ['done','forget']]):
                        wfo.status +='-unlock'
                        print "setting",wfo.name,"to",wfo.status
            session.commit()
        else:
            newly_locking.add(dataset)            
        time_point("Checked all")
    except Exception as e:
        print "Error in checking unlockability. relocking",dataset
        print str(e)
        newly_locking.add(dataset)

waiting_for_custodial_sum = sum([info['size'] for ds,info in waiting_for_custodial.items() if 'size' in info])
print waiting_for_custodial_sum,"[GB] out there waiting for custodial"
open('%s/waiting_custodial.json'%monitor_dir,'w').write( json.dumps( waiting_for_custodial , indent=2) )
open('%s/stuck_custodial.json'%monitor_pub_dir,'w').write( json.dumps( stuck_custodial , indent=2) )
open('%s/lagging_custodial.json'%monitor_dir,'w').write( json.dumps( lagging_custodial , indent=2) )
open('%s/missing_approval_custodial.json'%monitor_dir,'w').write( json.dumps( missing_approval_custodial , indent=2) )

## then for all that would have been invalidated from the past, check whether you can unlock the wf based on output
for wfo in session.query(Workflow).filter(Workflow.status=='forget').all():
    wfi = workflowInfo(url, wfo.name)
    if all([o not in newly_locking for o in wfi.request['OutputDatasets']]) and not 'unlock' in wfo.status:
        wfo.status +='-unlock'
        print "then setting",wfo.name,"to",wfo.status
    session.commit()

time_point("verified those in forget")

### then add everything else that reqmgr knows about in a valid status
### this is rather problematic because the locks are created and dealt recursively : i.e we assume to work on the delta between the previous locks and the new created ones. If we add those below, unified will try to unlock them at next round and create all sorts of trboules.
for item in also_locking_from_reqmgr: 
    LI.lock(item, reason='Additional lock of datasets')
    pass

time_point("locking also_locking_from_reqmgr")

for item in newly_locking:
    ## relock it
    LI.lock(item)
## release all locks which were not found necessary
for item in LI.items():
    if not item in newly_locking:
        LI.release(item)

time_point("final lock added")

