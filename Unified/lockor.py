#!/usr/bin/env python
from utils import getWorkflows, findCustodialCompletion, workflowInfo, getDatasetStatus, getWorkflowByOutput, unifiedConfiguration, getDatasetSize, sendEmail, sendLog, campaignInfo, componentInfo, reqmgr_url, monitor_dir, monitor_pub_dir, getWorkflowByMCPileup, getDatasetPresence, lockInfo, getLatestMCPileup, base_eos_dir, eosFile, eosRead
from assignSession import *
import json
import os
from collections import defaultdict
import sys
from McMClient import McMClient
import time
from utils import lockInfo, moduleLock



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

mlock = moduleLock()
if mlock(): sys.exit(0)

use_mcm=True
up = componentInfo(soft=['mcm','wtc'])
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
try:
    addHocLocks = json.loads( eosRead('%s/addhoc_lock.json'%base_eos_dir))
except:
    addHocLocks = []
    sys.exit(0)

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
                print "\t", dataset
                also_locking_from_reqmgr.add( dataset )
            continue

        if status == 'assignment-approved':
            if all([wfo.status.startswith('considered') for wfo in known]):
                ## skip those only assignment-approved / considered
                continue

        print "Locking all I/O for",wl['RequestName']
        for dataset in list(primaries)+list(secondaries)+outputs:
            if 'FAKE' in dataset: continue
            if 'None' in dataset: continue
            newly_locking.add(dataset)
            print "\t", dataset
    print len(newly_locking),"locks so far"


## avoid duplicates
also_locking_from_reqmgr = also_locking_from_reqmgr - newly_locking
print "additional lock for workflows not knonw to unified",len(also_locking_from_reqmgr)

already_locked = set( LI.items() )
print len(already_locked),"already locked items"

time_point("Starting to check for unlockability")

"""
secondary_timeout = getLatestMCPileup(url)
time_point("Got the age of all secondaries")

delay_days = UC.get('secondary_lock_timeout')
delay = delay_days*24*60*60 # in days
for secondary in secondary_timeout:
    if (now-secondary_timeout[secondary])>delay:
        print "not locking",secondary,"after",delay_days,"days"
    else:
        print "keep a lock on",secondary,"within",delay_days,"days"
        newly_locking.add( secondary )
"""

## just using this to lock all valid secondaries
newly_locking.update( CI.allSecondaries())



## check on the one left out, which would seem to get unlocked
for dataset in already_locked-newly_locking-also_locking_from_reqmgr:
    try:
        if not dataset:continue
        unlock = True
        time_point("Checking %s" % dataset)
        tier = dataset.split('/')[-1]

        if tier in tiers_keep_on_disk:
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
            LI.release( dataset )
            ##would like to pass to *-unlock, or even destroy from local db
            creators = getWorkflowByOutput( url, dataset , details=True)
            for creator in creators:
                for wfo in  session.query(Workflow).filter(Workflow.name==creator['RequestName']).all():
                    if not 'unlock' in wfo.status and any([wfo.status.startswith(key) for key in ['done','forget']]):
                        wfo.status +='-unlock'
                        print "setting",wfo.name,"to",wfo.status
            session.commit()
        else:
            print "\nrelocking",dataset
            newly_locking.add(dataset) 
           
        time_point("Checked all")
    except Exception as e:
        print "Error in checking unlockability. relocking",dataset
        print str(e)
        newly_locking.add(dataset)


## just for a couple of rounds
waiting_for_custodial={}
stuck_custodial={}
lagging_custodial={}
missing_approval_custodial={}
eosFile('%s/waiting_custodial.json'%monitor_dir,'w').write( json.dumps( waiting_for_custodial , indent=2) ).close()
eosFile('%s/stuck_custodial.json'%monitor_pub_dir,'w').write( json.dumps( stuck_custodial , indent=2) ).close()
eosFile('%s/lagging_custodial.json'%monitor_dir,'w').write( json.dumps( lagging_custodial , indent=2) ).close()
eosFile('%s/missing_approval_custodial.json'%monitor_dir,'w').write( json.dumps( missing_approval_custodial , indent=2) ).close()

## then for all that would have been invalidated from the past, check whether you can unlock the wf based on output
for wfo in session.query(Workflow).filter(Workflow.status=='forget').all():
    wfi = workflowInfo(url, wfo.name)
    if all([o not in newly_locking for o in wfi.request['OutputDatasets']]) and not 'unlock' in wfo.status:
        wfo.status +='-unlock'
        print "then setting",wfo.name,"to",wfo.status
    session.commit()

time_point("verified those in forget")

for item in also_locking_from_reqmgr: 
    LI.lock(item, reason='Additional lock of datasets')
    pass

time_point("locking also_locking_from_reqmgr")

for item in newly_locking:
    ## relock it
    LI.lock(item)

## release all locks which were not found necessary
#for item in LI.items():
#    if not item in newly_locking:
#        LI.release(item)

time_point("final lock added")

