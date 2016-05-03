#!/usr/bin/env python
from utils import getWorkflows, findCustodialCompletion, workflowInfo, getDatasetStatus, getWorkflowByOutput, unifiedConfiguration, getDatasetSize, sendEmail, campaignInfo, componentInfo, reqmgr_url, monitor_dir, getWorkflowByMCPileup
from assignSession import *
import json
import os
from collections import defaultdict
import sys
from McMClient import McMClient
import time

url = reqmgr_url

use_mcm=True
up = componentInfo(mcm=use_mcm, soft=['mcm'])
if not up.check():
    sys.exit(1)

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

## those that are already in lock
already_locked = set(json.loads(open('%s/globallocks.json'%monitor_dir).read()))
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
secondary_timeout = defaultdict(int)
## check on the one left out, which would seem to get unlocked
for dataset in already_locked-newly_locking:
    try:
        unlock = False
        bad_ds = False

        if not dataset in secondary_timeout:
            ## see if it's used in secondary anywhere
            usors = getWorkflowByMCPileup(url, dataset, details=True)
            ## find the latest request date using that dataset in secondary
            for usor in usors:
                d =time.mktime(time.strptime("-".join(map(str,usor['RequestDate'])), "%Y-%m-%d-%H-%M-%S"))
                secondary_timeout[dataset] = max(secondary_timeout[dataset],d)

        if secondary_timeout[dataset]: ## different than zero
            delay_days = 30
            delay = delay_days*24*60*60 # 30 days     
            if (now-secondary_timeout[dataset])>delay:
                print "unlocking secondary input after",delay_days,"days"
                unlock = True


        tier = dataset.split('/')[-1]
        creators = getWorkflowByOutput( url, dataset , details=True)
        if not creators and not tier == 'RAW':
            ds_status = getDatasetStatus( dataset )
            if not '-v0/' in dataset and ds_status!=None:
                sendEmail('failing get by output','%s has not been produced by anything?'%dataset)
                newly_locking.add(dataset)
                continue
            else:
                # does not matter, cannot be an OK dataset
                unlock = True
                bad_ds = True
        creators_status = [r['RequestStatus'] for r in creators]
        print "Statuses of workflow that made the dataset",dataset,"are",creators_status
        if all([status in ['failed','aborted','rejected','aborted-archived','rejected-archived'] for status in creators_status]):
            ## crap 
            print "\tunlocking",dataset,"for bad workflow statuses"
            unlock = True
            bad_ds = True

        ds_status=None
        if not unlock:
            ds_status = getDatasetStatus( dataset )

            if ds_status in ['INVALID',None]: 
                ## don't even try to keep the lock
                print "\tunlocking",dataset,"for bad dataset status",ds_status
                unlock = True
                bad_ds = True

        
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
                    print "cannot unlock",dataset,"because no request seems to be using it"
                    unlock=False                    
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
    except Exception as e:
        print "Error in checking unlockability. relocking",dataset
        print str(e)
        newly_locking.add(dataset)

waiting_for_custodial_sum = sum([info['size'] for ds,info in waiting_for_custodial.items() if 'size' in info])
print waiting_for_custodial_sum,"[GB] out there waiting for custodial"
open('%s/waiting_custodial.json'%monitor_dir,'w').write( json.dumps( waiting_for_custodial , indent=2) )
open('%s/stuck_custodial.json'%monitor_dir,'w').write( json.dumps( stuck_custodial , indent=2) )
open('%s/lagging_custodial.json'%monitor_dir,'w').write( json.dumps( lagging_custodial , indent=2) )
open('%s/missing_approval_custodial.json'%monitor_dir,'w').write( json.dumps( missing_approval_custodial , indent=2) )

## then for all that would have been invalidated from the past, check whether you can unlock the wf based on output
for wfo in session.query(Workflow).filter(Workflow.status=='forget').all():
    wfi = workflowInfo(url, wfo.name)
    if all([o not in newly_locking for o in wfi.request['OutputDatasets']]) and not 'unlock' in wfo.status:
        wfo.status +='-unlock'
        print "then setting",wfo.name,"to",wfo.status
    session.commit()


        
            
open('%s/globallocks.json.new'%monitor_dir,'w').write( json.dumps( sorted(list(newly_locking)), indent=2))
os.system('mv %s/globallocks.json.new %s/globallocks.json'%(monitor_dir,monitor_dir))


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
