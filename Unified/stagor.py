#!/usr/bin/env python
from assignSession import *
from utils import checkTransferStatus, checkTransferApproval, approveSubscription, getWorkflowByInput, workflowInfo, getDatasetBlocksFraction, findLostBlocks, findLostBlocksFiles, getDatasetBlockFraction, getDatasetFileFraction, getDatasetPresence, reqmgr_url, monitor_dir
from utils import unifiedConfiguration, componentInfo, sendEmail, getSiteWhiteList, checkTransferLag, sendLog
from utils import siteInfo, campaignInfo, global_SI
import json
import sys
import itertools
import pprint
import optparse
from htmlor import htmlor
from collections import defaultdict
import copy 

def stagor(url,specific =None, options=None):
    
    if not componentInfo().check(): return
    SI = global_SI
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
    try:
        transfer_statuses = json.loads(open('%s/transfer_statuses.json'%monitor_dir).read())
    except:
        print "inexisting transfer statuses. starting fresh"
        transfer_statuses = {}

    ## pop all that are now in negative values
    for phedexid in cached_transfer_statuses.keys():
        transfers = session.query(Transfer).filter(Transfer.phedexid==int(phedexid)).all()
        if not transfers:
            print phedexid,"does not look relevant to be in cache anymore. poping"
            print cached_transfer_statuses.pop( phedexid )


            
    ## collect all datasets that are needed for wf in staging, correcting the status of those that are not really in staging
    wfois = []
    needs = defaultdict(list)
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
            wfi.sendLog('stagor', '%s needs %s'%( wfo.name, dataset))

    open('%s/dataset_requirements.json'%monitor_dir,'w').write( json.dumps( needs, indent=2))

    endpoint_in_downtime = defaultdict(set)
    #endpoint_completed = defaultdict(set)
    endpoint_incompleted = defaultdict(set)
    #endpoint = defaultdict(set)
    send_back_to_considered = set()
    ## phedexid are set negative when not relevant anymore
    # probably there is a db schema that would allow much faster and simpler query
    for transfer in session.query(Transfer).filter(Transfer.phedexid>0).all():
        if specific  and str(transfer.phedexid)!=str(specific): continue

        skip=True
        for wfid in transfer.workflows_id:
            tr_wf = session.query(Workflow).get(wfid)
            if tr_wf: 
                if tr_wf.status == 'staging':
                    sendLog('stagor',"\t%s is staging for %s"%(transfer.phedexid, tr_wf.name))
                    skip=False

        if skip: 
            sendLog('stagor',"setting %s to negative value"%transfer.phedexid)
            transfer.phedexid = -transfer.phedexid
            session.commit()
            continue
        if transfer.phedexid<0: continue

        ## check the status of transfers
        checks = checkTransferApproval(url,  transfer.phedexid)
        approved = all(checks.values())
        if not approved:
            sendLog('stagor', "%s is not yet approved"%transfer.phedexid)
            approveSubscription(url, transfer.phedexid)
            continue

        ## check on transfer completion
        if str(transfer.phedexid) in cached_transfer_statuses:
            ### use a cache for transfer that already looked done
            sendLog('stagor',"read %s from cache"%transfer.phedexid)
            checks = cached_transfer_statuses[str(transfer.phedexid)]
        else:
            checks = checkTransferStatus(url, transfer.phedexid, nocollapse=True)
        ## just write this out
        transfer_statuses[str(transfer.phedexid)] = copy.deepcopy(checks)

        if not specific:
            for dsname in checks:
                if not dsname in done_by_input: done_by_input[dsname]={}
                if not dsname in completion_by_input: completion_by_input[dsname] = {}
                done_by_input[dsname][transfer.phedexid]=all(map(lambda i:i>=good_enough, checks[dsname].values()))
                completion_by_input[dsname][transfer.phedexid]=checks[dsname].values()
        if checks:
            sendLog('stagor',"Checks for %s are %s"%( transfer.phedexid, [node.values() for node in checks.values()]))
            done = all(map(lambda i:i>=good_enough,list(itertools.chain.from_iterable([node.values() for node in checks.values()]))))
        else:
            ## it is empty, is that a sign that all is done and away ?
            print "ERROR with the scubscriptions API of ",transfer.phedexid
            print "Most likely something else is overiding the transfer request. Need to work on finding the replacement automatically, if the replacement exists"
            done = False

        ## the thing above is NOT giving the right number
        #done = False

        for wfid in transfer.workflows_id:
            tr_wf = session.query(Workflow).get(wfid)
            if tr_wf:# and tr_wf.status == 'staging':  
                if not tr_wf.id in done_by_wf_id: done_by_wf_id[tr_wf.id]={}
                done_by_wf_id[tr_wf.id][transfer.phedexid]=done
            ## for those that are in staging, and the destination site is in drain
            #if not done and tr_wf.status == 'staging':
                

        if done:
            ## transfer.status = 'done'
            sendLog('stagor',"%s is done"%transfer.phedexid)
            cached_transfer_statuses[str(transfer.phedexid)] = copy.deepcopy(checks)
        else:
            sendLog('stagor',"%s is not finished %s"%(transfer.phedexid, pprint.pformat( checks )))
            pprint.pprint( checks )
            ## check if the destination is in down-time
            for ds in checks:
                sites_incomplete = [SI.SE_to_CE(s) for s,v in checks[ds].items() if v<good_enough]
                sites_incomplete_down = [s for s in sites_incomplete if not s in SI.sites_ready]
                if sites_incomplete_down:
                    sendLog('stagor',"%s are in downtime, while waiting for %s to get there"%( ",".join(sites_incomplete_down), ds))
                #sites_complete = [SI.SE_to_CE(s) for s,v in checks[ds].items() if v>=good_enough]
                #endpoint[ds].update( sites_complete )
                #endpoint[ds].update( sites_incomplete )
                #endpoint_completed[ds].update( sites_complete )
                endpoint_incompleted[ds].update( sites_incomplete )
                endpoint_in_downtime[ds].update( sites_incomplete_down )
            


    print "End point in down time"
    #print json.dumps( endpoint_in_downtime , indent=2)
    for ds in endpoint_in_downtime:
        print json.dumps(list(endpoint_in_downtime[ds]), indent=2)

    open('cached_transfer_statuses.json','w').write( json.dumps( cached_transfer_statuses, indent=2))
    open('%s/transfer_statuses.json'%monitor_dir,'w').write( json.dumps( transfer_statuses, indent=2))

    already_stuck = json.loads( open('%s/stuck_transfers.json'%monitor_dir).read() )
    missing_in_action = defaultdict(list)


    print "-"*10,"Checking on workflows in staging","-"*10
    #forget_about = ['/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-MCRUN2_71_V1-v2/GEN-SIM']
    #for what in forget_about:
    #    if not done_by_input[what]:
    #        done_by_input[what] = {'fake':True}

    ## come back to workflows and check if they can go
    available_cache = defaultdict(lambda : defaultdict(float))
    presence_cache = defaultdict(dict)
    for wfo,wfi in wfois:
        print "#"*30
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
                wfi.sendLog('stagor',"%s is not ready"%need)
                print json.dumps( done_by_input[need] , indent=2)
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
                    wfi.sendLog('stagor', "%s is available %s times %s"%( need, available, this_check))
                    all_check &= this_check
                    if not all_check: break

                for need in list(secondaries):
                    ## I do not want to check on the secon
                    this_check = all(done_by_input[need].values())
                    wfi.sendLog('stagor',"%s is all transfered %s"%(need, json.dumps(done_by_input[need], indent=2)))
                    all_check&= this_check
                    #if not se_allowed_key in presence_cache[need]:
                    #    presence_cache[need][se_allowed_key] = getDatasetPresence( url, need , within_sites=se_allowed)

                    ## restrict to where the primary is
                    #presence = dict([(k,v) for (k,v) in presence_cache[need][se_allowed_key].items() if k in prim_where])
                    #this_check = all([there for (there,frac) in presence.values()])
                    #print need,"is present at all sites:",this_check
                    #all_check&= this_check

                if all_check:    
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



    if send_back_to_considered:
        sendEmail("transfer to endpoint in downtime","sending back to considered the following workflows \n%s"%('\n'.join( send_back_to_considered)))

    print "-"*10,"Checking on non-available datasets","-"*10    
    ## now check on those that are not fully available
    
    for dsname in available_cache.keys():
        ## squash the se_allowed_key key
        available_cache[dsname] = min( available_cache[dsname].values() )
            
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
                        sendEmail( "send back to considered","%s was send back and might be trouble"% wf.name)
                    else:
                        print "would send",wf.name,"back to considered"
                        sendEmail( "subscription lagging behind","susbscriptions to get %s running are not appearing in phedex. I would have send it back to considered but that's not good."% wf.name)
            continue

        ## not compatible with checking on secondary availability
        #if all([wf.status != 'staging' for wf in using_wfos]):
        #    ## means despite all checks that input is not needed
        #    continue

        if available < 1.:
            print "incomplete",dsname
            ## there is a problem in the method below that it does not account for files stuck in T1*Buffer only
            lost_blocks,lost_files = findLostBlocksFiles( url, dsname )
            lost_block_names = [item['name'] for item in lost_blocks]
            lost_file_names = [item['name'] for item in lost_files]

            if lost_blocks:
                #print json.dumps( lost , indent=2 )
                ## estimate for how much !
                fraction_loss,_,n_missing = getDatasetBlockFraction(dsname, lost_block_names)
                print "We have lost",len(lost_block_names),"blocks",lost_block_names,"for %f%%"%fraction_loss
                if fraction_loss > 0.05: ## 95% completion mark
                    sendEmail('we have lost too many blocks','%s is missing %d blocks, for %d events, %f %% loss'%(dsname, len(lost_block_names), n_missing, fraction_loss))
                    ## the workflow should be rejected !
                    for wf in using_wfos: 
                        if wf.status == 'staging':
                            print wf.name,"is doomed. setting to trouble"
                            wf.status = 'trouble'
                            session.commit()
                else:
                    ## probably enough to make a ggus and remove
                    if not dsname in known_lost_blocks:
                        sendEmail('we have lost a few blocks', '%s is missing %d blocks, for %d events, %f %% loss\n\n%s'%(dsname, len(lost_block_names), n_missing, fraction_loss, '\n'.join( lost_block_names ) ))
                        known_lost_blocks[dsname] = [i['name'] for i in lost_blocks]
                                  
            if lost_files:
                fraction_loss,_,n_missing = getDatasetFileFraction(dsname, lost_file_names)
                print "We have lost",len(lost_file_names),"files",lost_file_names,"for %f%%"%fraction_loss
                
                if fraction_loss > 0.05:
                    sendEmail('we have lost too many files','%s is missing %d files, for %d events, %f %% loss'%(dsname, len(lost_file_names),n_missing, fraction_loss))
                    for wf in using_wfos:
                        if wf.status == 'staging':
                            print wf.name,"is doomed. setting to trouble"
                            wf.status = 'trouble'
                            session.commit()
                else:
                    ## probably enough to make a ggus and remove    
                    if not dsname in known_lost_files:
                        sendEmail('we have lost a few files','%s is missing %d files, for %d events, %f %% loss\n\n%s'%(dsname, len(lost_file_names),n_missing, fraction_loss, '\n'.join(lost_file_names)))
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

    print "Going further and make a report of stuck transfers"

    datasets_by_phid = defaultdict(set)
    for dataset in missing_in_action:
        for phid in missing_in_action[dataset]:
            #print dataset,"stuck through",phid
            datasets_by_phid[phid].add( dataset )

    bad_destinations = defaultdict(set)
    bad_sources = defaultdict(set)
    report = ""
    really_stuck_dataset = set()
    transfer_timeout = UC.get("transfer_timeout")
    transfer_lowrate = UC.get("transfer_lowrate")
    for phid,datasets in datasets_by_phid.items():
        issues = checkTransferLag( url, phid , datasets=list(datasets) )
        for dataset in issues:
            for block in issues[dataset]:
                for destination in issues[dataset][block]:
                    (block_size,destination_size,delay,rate,dones) = issues[dataset][block][destination]
                    ## count x_Buffer and x_MSS as one source
                    redones=[]
                    for d in dones:
                        if d.endswith('Buffer') or d.endswith('Export'):
                            if d.replace('Buffer','MSS').replace('Export','MSS') in dones: 
                                continue
                            else: 
                                redones.append( d )
                        else:
                            redones.append( d )
                    dones = list(set( redones ))
                    #dones = filter(lambda s : (s.endswith('Buffer') and not s.replace('Buffer','MSS') in dones) or (not s.endswith('Buffer')) , dones)
                    if delay>transfer_timeout and rate<transfer_lowrate:
                        if len(dones)>1:
                            ## its the destination that sucks
                            bad_destinations[destination].add( block )
                        else:
                            dum=[bad_sources[d].add( block ) for d in dones]
                        really_stuck_dataset.add( dataset )
                        print "add",dataset,"to really stuck"
                        report += "%s is not getting to %s, out of %s faster than %f [GB/s] since %f [d]\n"%(block,destination,", ".join(dones), rate, delay)
    print "\n"*2

    ## create tickets right away ?
    report+="\nbad sources "+",".join(bad_sources.keys())+"\n"
    for site,blocks in bad_sources.items():
        report+="\n\n%s:"%site+"\n\t".join(['']+list(blocks))
    report+="\nbad destinations "+",".join(bad_destinations.keys())+"\n"
    for site,blocks in bad_destinations.items():
        report+="\n\n%s:"%site+"\n\t".join(['']+list(blocks))

    print '\n'*2,"Datasets really stuck"
    print '\n'.join( really_stuck_dataset )

    print '\n'*2,"report written at https://cmst2.web.cern.ch/cmst2/unified/logs/incomplete_transfers.log"
    print report

    stuck_transfers = dict([(k,v) for (k,v) in missing_in_action.items() if k in really_stuck_dataset])
    print '\n'*2,'Stuck dataset transfers'
    print json.dumps(stuck_transfers , indent=2)
    open('%s/stuck_transfers.json'%monitor_dir,'w').write( json.dumps(stuck_transfers , indent=2) )
    open('%s/logs/incomplete_transfers.log'%monitor_dir,'w').write( report )
    #sendEmail('incomplete transfers', report,sender=None, destination=['dc.jorge10@uniandes.edu.co','aram.apyan@cern.ch','sidn@mit.edu'])


if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    stagor(url, spec, options)
    htmlor()
