#!/usr/bin/env python
from assignSession import *
from utils import checkTransferStatus, checkTransferApproval, approveSubscription, getWorkflowByInput, workflowInfo, getDatasetBlocksFraction, findLostBlocks, findLostBlocksFiles, getDatasetBlockFraction, getDatasetFileFraction, getDatasetPresence, reqmgr_url, monitor_dir, getAllStuckDataset, monitor_pub_dir
from utils import unifiedConfiguration, componentInfo, sendEmail, checkTransferLag, sendLog
from utils import siteInfo, campaignInfo, unified_url
import json
import sys
import itertools
import pprint
import optparse
from htmlor import htmlor
from collections import defaultdict
import copy 
import time

def stagor(url,specific =None, options=None):
    
    if not componentInfo().check(): return
    SI = siteInfo()
    CI = campaignInfo()
    UC = unifiedConfiguration()

    done_by_wf_id = {}
    done_by_input = {}
    completion_by_input = {}
    good_enough = 100.0
    
    lost_blocks = json.loads(open('%s/lost_blocks_datasets.json'%monitor_dir).read())
    lost_files = json.loads(open('%s/lost_files_datasets.json'%monitor_dir).read())
    known_lost_blocks = {}
    known_lost_files = {}
    for dataset in set(lost_blocks.keys()+lost_files.keys()):
        b,f = findLostBlocksFiles(url, dataset)
        if dataset in lost_blocks and not b:
            print dataset,"has no really lost blocks"
        else:
            known_lost_blocks[dataset] = [i['name'] for i in b]

        if dataset in lost_files and not f: 
            print dataset,"has no really lost files"
        else:
            known_lost_files[dataset] = [i['name'] for i in f]

    try:
        cached_transfer_statuses = json.loads(open('cached_transfer_statuses.json').read())
    except:
        print "inexisting transfer statuses. starting fresh"
        cached_transfer_statuses = {}
        return False

    transfer_statuses = {}


    def time_point(label="",sub_lap=False):
        now = time.mktime(time.gmtime())
        nows = time.asctime(time.gmtime())

        print "Time check (%s) point at : %s"%(label, nows)
        print "Since start: %s [s]"% ( now - time_point.start)
        if sub_lap:
            print "Sub Lap : %s [s]"% ( now - time_point.sub_lap ) 
            time_point.sub_lap = now
        else:
            print "Lap : %s [s]"% ( now - time_point.lap ) 
            time_point.lap = now            
            time_point.sub_lap = now

    time_point.sub_lap = time_point.lap = time_point.start = time.mktime(time.gmtime())


    time_point("Check cached transfer")

    ## collect all datasets that are needed for wf in staging, correcting the status of those that are not really in staging
    wfois = []
    needs = defaultdict(list)
    needs_by_priority = defaultdict(list)
    for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
        wfi = workflowInfo(url, wfo.name)
        if wfi.request['RequestStatus'] in ['running-open','running-closed','completed','assigned','acquired']:
            wfi.sendLog('stagor', "is in status %s"%wfi.request['RequestStatus'])
            wfi.status='away'
            session.commit()
            continue
        if not wfi.request['RequestStatus'] in ['assignment-approved']:
            ## should be setting 'away' too
            print wfo.name,"is",wfi.request['RequestStatus']
            sendEmail("wrong status in staging. debug","%s is in %s, should set away."%(wfo.name,wfi.request['RequestStatus']))
        wfois.append( (wfo,wfi) )            
        _,primaries,_,secondaries = wfi.getIO()
        for dataset in list(primaries)+list(secondaries):
            needs[wfo.name].append( dataset)
            done_by_input[dataset] = {}
            completion_by_input[dataset] = {}
            needs_by_priority[wfi.request['RequestPriority']].append( dataset )
            wfi.sendLog('stagor', '%s needs %s'%( wfo.name, dataset))

    time_point("Check staging workflows")            

    open('%s/dataset_requirements.json'%monitor_dir,'w').write( json.dumps( needs, indent=2))
    for prio in needs_by_priority: needs_by_priority[prio] = list(set(needs_by_priority[prio]))
    open('%s/dataset_priorities.json'%monitor_dir,'w').write( json.dumps( needs_by_priority , indent=2))
        

    dataset_endpoints = defaultdict(set)
    endpoint_in_downtime = defaultdict(set)
    #endpoint_completed = defaultdict(set)
    endpoint_incompleted = defaultdict(set)
    #endpoint = defaultdict(set)
    send_back_to_considered = set()


    ## first check if anything is inactive
    all_actives = set([transfer.phedexid for transfer in session.query(TransferImp).filter(TransferImp.active).all()])
    for active_phedexid in all_actives:
        skip = True
        transfers_phedexid = session.query(TransferImp).filter(TransferImp.phedexid == active_phedexid).all()
        for imp in transfers_phedexid:
            if imp.workflow.status == 'staging':
                skip =False
                sendLog('stagor',"\t%s is staging for %s"%(imp.phedexid, imp.workflow.name))
        if skip:
            sendLog('stagor',"setting %s inactive" % active_phedexid)
            for imp in transfers_phedexid:
                imp.active = False
        session.commit()

    all_actives = sorted(set([transfer.phedexid for transfer in session.query(TransferImp).filter(TransferImp.active).all()]))
    for phedexid in all_actives:

        if specific: continue

        ## check on transfer completion
        not_cached = False
        if str(phedexid) in cached_transfer_statuses:
            ### use a cache for transfer that already looked done
            sendLog('stagor',"read %s from cache"%phedexid)
            checks = cached_transfer_statuses[str(phedexid)]
        else:
            ## I actually would like to avoid that all I can
            sendLog('stagor','Performing spurious transfer check on %s'% phedexid, level='critical')
            checks = checkTransferStatus(url, phedexid, nocollapse=True)
            if not checks:
                ## this is going to bias quite heavily the rest of the code. we should abort here
                #sendLog('stagor','Ending stagor because of skewed input from checkTransferStatus', level='critical')
                #return False
                sendLog('stagor','Stagor has got a skewed input from checkTransferStatus', level='critical')
                checks = {}
                pass
            #checks = {}
            #not_cached = True

        time_point("Check transfer status %s"% phedexid, sub_lap=True)            

        ## just write this out
        transfer_statuses[str(phedexid)] = copy.deepcopy(checks)

        if not specific:
            for dsname in checks:
                if not dsname in done_by_input: done_by_input[dsname]={}
                if not dsname in completion_by_input: completion_by_input[dsname] = {}
                done_by_input[dsname][phedexid]=all(map(lambda i:i>=good_enough, checks[dsname].values()))
                completion_by_input[dsname][phedexid]=checks[dsname].values()
        if checks:
            sendLog('stagor',"Checks for %s are %s"%( phedexid, [node.values() for node in checks.values()]))
            done = all(map(lambda i:i>=good_enough,list(itertools.chain.from_iterable([node.values() for node in checks.values()]))))
        else:
            ## it is empty, is that a sign that all is done and away ?
            if not_cached:
                print "Transfer status was not cached"
            else:
                print "ERROR with the scubscriptions API of ",phedexid
                print "Most likely something else is overiding the transfer request. Need to work on finding the replacement automatically, if the replacement exists"
            done = False

        transfers_phedexid = session.query(TransferImp).filter(TransferImp.phedexid == phedexid).all()
        for imp in transfers_phedexid:
            tr_wf = imp.workflow
            if tr_wf:# and tr_wf.status == 'staging':  
                if not tr_wf.id in done_by_wf_id: done_by_wf_id[tr_wf.id]={}
                done_by_wf_id[tr_wf.id][phedexid]=done
            if done:
                imp.active = False
                session.commit()

        for ds in checks:
            for s,v in checks[ds].items():
                dataset_endpoints[ds].add( s )

        if done:
            sendLog('stagor',"%s is done"%phedexid)
            cached_transfer_statuses[str(phedexid)] = copy.deepcopy(checks)
        else:
            sendLog('stagor',"%s is not finished %s"%(phedexid, pprint.pformat( checks )))
            ##pprint.pprint( checks )
            ## check if the destination is in down-time
            for ds in checks:
                sites_incomplete = [SI.SE_to_CE(s) for s,v in checks[ds].items() if v<good_enough]
                sites_incomplete_down = [s for s in sites_incomplete if not s in SI.sites_ready]
                ## no space means no transfer should go there : NO, it does not work in the long run
                #sites_incomplete_down = [SI.SE_to_CE(s) for s,v in checks[ds].items() if (v<good_enough and (SI.disk[s]==0 or (not SI.SE_to_CE(s) in SI.sites_ready)))]



                if sites_incomplete_down:
                    sendLog('stagor',"%s are in downtime, while waiting for %s to get there"%( ",".join(sites_incomplete_down), ds))
                #sites_complete = [SI.SE_to_CE(s) for s,v in checks[ds].items() if v>=good_enough]
                #endpoint[ds].update( sites_complete )
                #endpoint[ds].update( sites_incomplete )
                #endpoint_completed[ds].update( sites_complete )
                endpoint_incompleted[ds].update( sites_incomplete )
                endpoint_in_downtime[ds].update( sites_incomplete_down )
            

    time_point("Check on-going transfers")            


    print "End points"
    for k in dataset_endpoints: dataset_endpoints[k] = list(dataset_endpoints[k])
    print json.dumps( dataset_endpoints , indent=2)

    print "End point in down time"
    for k in endpoint_in_downtime: endpoint_in_downtime[k] = list(endpoint_in_downtime[k])
    print json.dumps( endpoint_in_downtime , indent=2)    

    print "End point incomplete in down time"
    for k in endpoint_incompleted: endpoint_incompleted[k] = list(endpoint_incompleted[k])
    print json.dumps( endpoint_incompleted , indent=2)        


    #open('cached_transfer_statuses.json','w').write( json.dumps( cached_transfer_statuses, indent=2))
    open('%s/transfer_statuses.json'%monitor_dir,'w').write( json.dumps( transfer_statuses, indent=2))
    open('%s/dataset_endpoints.json'%monitor_dir,'w').write( json.dumps(dataset_endpoints, indent=2))

    already_stuck = json.loads( open('%s/stuck_transfers.json'%monitor_pub_dir).read() ).keys()
    already_stuck.extend( getAllStuckDataset() )
 
    missing_in_action = defaultdict(list)


    print "-"*10,"Checking on workflows in staging","-"*10
    #forget_about = ['/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-MCRUN2_71_V1-v2/GEN-SIM']
    #for what in forget_about:
    #    if not done_by_input[what]:
    #        done_by_input[what] = {'fake':True}

    ## come back to workflows and check if they can go
    available_cache = defaultdict(lambda : defaultdict(float))
    presence_cache = defaultdict(dict)

    time_point("Preparing for more")
    for wfo,wfi in wfois:
        print "#"*30
        time_point("Forward checking %s"% wfo.name,sub_lap=True)
        ## the site white list takes site, campaign, memory and core information
        (_,primaries,_,secondaries,sites_allowed) = wfi.getSiteWhiteList(verbose=False)
        se_allowed = [SI.CE_to_SE(site) for site in sites_allowed]
        se_allowed.sort()
        se_allowed_key = ','.join(se_allowed)
        readys={}
        for need in list(primaries)+list(secondaries):
            if not need in done_by_input:
                wfi.sendLog('stagor',"missing transfer report for %s"%need)
                readys[need] = False      
                ## should warn someone about this !!!
                ## it cannot happen, by construction
                sendEmail('missing transfer report','%s does not have a transfer report'%(need))
                continue

            if not done_by_input[need] and need in list(secondaries):
                wfi.sendLog('stagor',"assuming it is OK for secondary %s to have no attached transfers"% need)
                readys[need] = True
                done_by_input[need] = { "fake" : True }
                continue

            if len(done_by_input[need]) and all(done_by_input[need].values()):
                wfi.sendLog('stagor',"%s is ready"%need)
                print json.dumps( done_by_input[need] , indent=2)
                readys[need] = True
            else:
                wfi.sendLog('stagor',"%s is not ready \n%s"%(need,json.dumps( done_by_input[need] , indent=2)))
                readys[need] = False

        if readys and all(readys.values()):
            if wfo.status == 'staging':
                wfi.sendLog('stagor',"all needs are fullfilled, setting staged")
                wfo.status = 'staged'
                session.commit()
            else:
                wfi.sendLog('stagor',"all needs are fullfilled, already")
                print json.dumps( readys, indent=2 )
        else:
            wfi.sendLog('stagor',"missing requirements")
            copies_needed,_ = wfi.getNCopies()
            jump_ahead = False
            re_transfer = False
            ## there is missing input let's do something more elaborated
            for need in list(primaries):#+list(secondaries):
                if endpoint_in_downtime[need] == endpoint_incompleted[need]:
                    #print need,"is going to an end point in downtime"
                    wfi.sendLog('stagor',"%s has only incomplete endpoint in downtime"%need)
                    re_transfer=True
                
                if not se_allowed_key in available_cache[need]:
                    available_cache[need][se_allowed_key]  = getDatasetBlocksFraction( url , need, sites=se_allowed )
                    if available_cache[need][se_allowed_key] >= copies_needed:
                        wfi.sendLog('stagor',"assuming it is OK to move on like this already for %s"%need)
                        jump_ahead = True
                    else:
                        wfi.sendLog('stagor',"Available %s times"% available_cache[need][se_allowed_key])
                        missing_and_downtime = list(set(endpoint_in_downtime[need]) & set(endpoint_incompleted[need]))
                        if missing_and_downtime:
                            wfi.sendLog('stagor',"%s is incomplete at %s which is in downtime, trying to move along"%(need, ','.join(missing_and_downtime)))
                            jump_ahead = True
                        else:
                            wfi.sendLog('stagor',"continue waiting for transfers for optimum production performance.")



            ## compute a time since staging to filter jump starting ?                    
            # check whether the inputs is already in the stuck list ...
            for need in list(primaries)+list(secondaries):
                if need in already_stuck: 
                    wfi.sendLog('stagor',"%s is stuck, so try to jump ahead"%need)
                    jump_ahead = True
                    
            if jump_ahead or re_transfer:
                details_text = "checking on availability for %s to jump ahead"%wfo.name
                details_text += '\n%s wants %s copies'%(wfo.name,copies_needed)
                copies_needed = max(1,copies_needed-1)
                details_text += '\nlowering by one unit to %s'%copies_needed
                wfi.sendLog('stagor', details_text)
                all_check = True
                
                prim_where = set()
                for need in list(primaries):
                    if not se_allowed_key in presence_cache[need]:
                        presence_cache[need][se_allowed_key] = getDatasetPresence( url, need , within_sites=se_allowed)
                    presence = presence_cache[need][se_allowed_key]
                    prim_where.update( presence.keys() )
                    available = available_cache[need][se_allowed_key]
                    this_check = (available >= copies_needed)
                    wfi.sendLog('stagor', "%s is available %s times (%s), at %s"%( need, available, this_check, se_allowed_key))
                    all_check &= this_check
                    if not all_check: break

                for need in list(secondaries):
                    ## I do not want to check on the secon
                    ## this below does not function because the primary could be all available, and the secondary not complete at a certain site that does not matter at that point
                    this_check = all(done_by_input[need].values())
                    wfi.sendLog('stagor',"%s is this much transfered %s"%(need, json.dumps(done_by_input[need], indent=2)))
                    all_check&= this_check
                    #if not se_allowed_key in presence_cache[need]:
                    #    presence_cache[need][se_allowed_key] = getDatasetPresence( url, need , within_sites=se_allowed)

                    ## restrict to where the primary is
                    #presence = dict([(k,v) for (k,v) in presence_cache[need][se_allowed_key].items() if k in prim_where])
                    #this_check = all([there for (there,frac) in presence.values()])
                    #print need,"is present at all sites:",this_check
                    #all_check&= this_check

                if all_check and not re_transfer:    
                    wfi.sendLog('stagor',"needs are sufficiently fullfilled, setting staged")
                    wfo.status = 'staged'
                    session.commit()
                else:
                    print wfo.name,"has to wait a bit more"
                    wfi.sendLog('stagor',"needs to wait a bit more")
            else:
                wfi.sendLog('stagor',"not checking availability")

            if re_transfer:
                wfi.sendLog('stagor',"Sending back to considered because of endpoint in downtime")
                if wfo.status == 'staging':
                    wfo.status = 'considered'
                    session.commit()
                    send_back_to_considered.add( wfo.name )


    time_point("Checked affected workflows")

    if send_back_to_considered:
        #sendEmail("transfer to endpoint in downtime","sending back to considered the following workflows \n%s"%('\n'.join( send_back_to_considered)))
        sendLog('stagor', "sending back to considered the following workflows \n%s"%('\n'.join( send_back_to_considered)), level='critical')

    print "-"*10,"Checking on non-available datasets","-"*10    
    ## now check on those that are not fully available
    
    for dsname in available_cache.keys():
        ## squash the se_allowed_key key
        available_cache[dsname] = min( available_cache[dsname].values() )

    really_stuck_dataset = set()

    for dsname,available in available_cache.items():
        using_its = getWorkflowByInput(url, dsname)
        #print using_its
        using_wfos = []
        for using_it in using_its:
            wf = session.query(Workflow).filter(Workflow.name == using_it).first()
            if wf:
                using_wfos.append( wf )

        if not len(done_by_input[dsname]):
            print "For dataset",dsname,"there are no transfer report. That's an issue."
            for wf in using_wfos:
                if wf.status == 'staging':
                    if UC.get("stagor_sends_back"):
                        print "sending",wf.name,"back to considered"
                        wf.status = 'considered'
                        session.commit()
                        #sendEmail( "send back to considered","%s was send back and might be trouble"% wf.name)
                        sendLog('stagor', "%s was send back and might be trouble"% wf.name, level='critical')
                    else:
                        print "would send",wf.name,"back to considered"
                        #sendEmail( "subscription lagging behind","susbscriptions to get %s running are not appearing in phedex. I would have send it back to considered but that's not good."% wf.name)
                        sendLog('stagor', "susbscriptions to get %s running are not appearing in phedex. I would have send it back to considered but that's not good."% wf.name, level='critical')
            continue

        ## not compatible with checking on secondary availability
        #if all([wf.status != 'staging' for wf in using_wfos]):
        #    ## means despite all checks that input is not needed
        #    continue

        if available < 1.:
            print "incomplete",dsname
            ## there is a problem in the method below that it does not account for files stuck in T1*Buffer only
            lost_blocks,lost_files = findLostBlocksFiles( url, dsname ) if (not dsname.endswith('/RAW')) else ([],[])
            lost_block_names = [item['name'] for item in lost_blocks]
            lost_file_names = [item['name'] for item in lost_files]

            if lost_blocks:
                #print json.dumps( lost , indent=2 )
                ## estimate for how much !
                fraction_loss,_,n_missing = getDatasetBlockFraction(dsname, lost_block_names)
                print "We have lost",len(lost_block_names),"blocks",lost_block_names,"for %f%%"%(100.*fraction_loss)
                if fraction_loss > 0.05: ## 95% completion mark
                    #sendEmail('we have lost too many blocks','%s is missing %d blocks, for %d events, %f %% loss'%(dsname, len(lost_block_names), n_missing, fraction_loss))
                    sendLog('stagor', '%s is missing %d blocks, for %d events, %3.2f %% loss'%(dsname, len(lost_block_names), n_missing, 100*fraction_loss), level='critical')
                    ## the workflow should be rejected !
                    for wf in using_wfos: 
                        if wf.status == 'staging':
                            print wf.name,"is doomed. setting to trouble"
                            wf.status = 'trouble'
                            session.commit()
                            sendLog('stagor', '%s has too much loss on the input dataset %s. Missing  %d blocks, for %d events, %3.2f %% loss'%(wf.name, dsname, len(lost_block_names), n_missing, 100*fraction_loss), level='critical')
                else:
                    ## probably enough to make a ggus and remove
                    if not dsname in known_lost_blocks:
                        #sendEmail('we have lost a few blocks', '%s is missing %d blocks, for %d events, %f %% loss\n\n%s'%(dsname, len(lost_block_names), n_missing, fraction_loss, '\n'.join( lost_block_names ) ))
                        sendLog('stagor', '%s is missing %d blocks, for %d events, %f %% loss\n\n%s'%(dsname, len(lost_block_names), n_missing, fraction_loss, '\n'.join( lost_block_names ) ), level='critical')
                        known_lost_blocks[dsname] = [i['name'] for i in lost_blocks]
                really_stuck_dataset.add( dsname )
                  
            if lost_files:
                fraction_loss,_,n_missing = getDatasetFileFraction(dsname, lost_file_names)
                print "We have lost",len(lost_file_names),"files",lost_file_names,"for %f%%"%fraction_loss
                
                if fraction_loss > 0.05:
                    #sendEmail('we have lost too many files','%s is missing %d files, for %d events, %f %% loss'%(dsname, len(lost_file_names),n_missing, fraction_loss))
                    sendLog('stagor', '%s is missing %d files, for %d events, %f %% loss'%(dsname, len(lost_file_names),n_missing, fraction_loss), level='critical')
                    for wf in using_wfos:
                        if wf.status == 'staging':
                            print wf.name,"is doomed. setting to trouble"
                            wf.status = 'trouble'
                            session.commit()
                else:
                    ## probably enough to make a ggus and remove    
                    if not dsname in known_lost_files:
                        #sendEmail('we have lost a few files','%s is missing %d files, for %d events, %f %% loss\n\n%s'%(dsname, len(lost_file_names),n_missing, fraction_loss, '\n'.join(lost_file_names)))
                        sendLog('stagor', '%s is missing %d files, for %d events, %f %% loss\n\n%s'%(dsname, len(lost_file_names),n_missing, fraction_loss, '\n'.join(lost_file_names)), level='critical')
                        known_lost_files[dsname] = [i['name'] for i in lost_files]

                ## should the status be change to held-staging and pending on a ticket



            missings = [pid for (pid,d) in done_by_input[dsname].items() if d==False] 
            print "\t",done_by_input[dsname]
            print "\tneeds",len(done_by_input[dsname])
            print "\tgot",done_by_input[dsname].values().count(True)
            print "\tmissing",missings
            missing_in_action[dsname].extend( missings )
        


    rr= open('%s/lost_blocks_datasets.json'%monitor_dir,'w')
    rr.write( json.dumps( known_lost_blocks, indent=2))
    rr.close()

    rr= open('%s/lost_files_datasets.json'%monitor_dir,'w')
    rr.write( json.dumps( known_lost_files, indent=2))
    rr.close()


    open('%s/incomplete_transfers.json'%monitor_dir,'w').write( json.dumps(missing_in_action, indent=2) )
    print "Stuck transfers and datasets"
    print json.dumps( missing_in_action, indent=2 )


    datasets_by_phid = defaultdict(set)
    for dataset in missing_in_action:
        for phid in missing_in_action[dataset]:
            #print dataset,"stuck through",phid
            datasets_by_phid[phid].add( dataset )

    for k in datasets_by_phid:
        datasets_by_phid[k] = list(datasets_by_phid[k])

    open('datasets_by_phid.json','w').write( json.dumps(datasets_by_phid, indent=2 ))

    open('really_stuck_dataset.json','w').write( json.dumps(list(really_stuck_dataset), indent=2 ))
    print '\n'*2,"Datasets really stuck"
    print '\n'.join( really_stuck_dataset )

    #############
    ## not going further for what matters
    #############
    return 


if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    stagor(url, spec, options)
    htmlor()
