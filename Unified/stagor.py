#!/usr/bin/env python
from assignSession import *
from utils import checkTransferStatus, checkTransferApproval, approveSubscription, getWorkflowByInput, workflowInfo, getDatasetBlocksFraction, findLostBlocks, findLostBlocksFiles, getDatasetBlockFraction, getDatasetFileFraction, getDatasetPresence
from utils import unifiedConfiguration, componentInfo, sendEmail, getSiteWhiteList, checkTransferLag
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
    
    lost_blocks = json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/lost_blocks_datasets.json').read())
    lost_files = json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/lost_files_datasets.json').read())
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


    cached_transfer_statuses = json.loads(open('cached_transfer_statuses.json').read())
    ## pop all that are now in negative values
    for phedexid in cached_transfer_statuses.keys():
        transfers = session.query(Transfer).filter(Transfer.phedexid==int(phedexid)).all()
        if not transfers:
            print phedexid,"does not look relevant to be in cache anymore. poping"
            print cached_transfer_statuses.pop( phedexid )


            
    ## collect all datasets that are needed for wf in staging, correcting the status of those that are not really in staging
    wfois = []
    for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
        wfi = workflowInfo(url, wfo.name)
        if wfi.request['RequestStatus'] in ['running-open','running-closed','completed']:
            print wfo.name,"is",wfi.request['RequestStatus']
            wfi.status='away'
            session.commit()
            continue

        wfois.append( (wfo,wfi) )            
        _,primaries,_,secondaries = wfi.getIO()
        for dataset in list(primaries)+list(secondaries):
            done_by_input[dataset] = {}
            completion_by_input[dataset] = {}
            print wfo.name,"needs",dataset

    ## phedexid are set negative when not relevant anymore
    # probably there is a db schema that would allow much faster and simpler query
    for transfer in session.query(Transfer).filter(Transfer.phedexid>0).all():
        if specific  and str(transfer.phedexid)!=str(specific): continue

        skip=True
        for wfid in transfer.workflows_id:
            tr_wf = session.query(Workflow).get(wfid)
            if tr_wf: 
                if tr_wf.status == 'staging':
                    print "\t",transfer.phedexid,"is staging for",tr_wf.name
                    skip=False

        if skip: 
            print "setting",transfer.phedexid,"to negative value"
            transfer.phedexid = -transfer.phedexid
            session.commit()
            continue
        if transfer.phedexid<0: continue

        ## check the status of transfers
        checks = checkTransferApproval(url,  transfer.phedexid)
        approved = all(checks.values())
        if not approved:
            print transfer.phedexid,"is not yet approved"
            approveSubscription(url, transfer.phedexid)
            continue

        ## check on transfer completion
        if str(transfer.phedexid) in cached_transfer_statuses:
            ### use a cache for transfer that already looked done
            print "read",transfer.phedexid,"from cache"
            checks = cached_transfer_statuses[str(transfer.phedexid)]
        else:
            checks = checkTransferStatus(url, transfer.phedexid, nocollapse=True)

        if not specific:
            for dsname in checks:
                if not dsname in done_by_input: done_by_input[dsname]={}
                if not dsname in completion_by_input: completion_by_input[dsname] = {}
                done_by_input[dsname][transfer.phedexid]=all(map(lambda i:i>=good_enough, checks[dsname].values()))
                completion_by_input[dsname][transfer.phedexid]=checks[dsname].values()
        if checks:
            print "Checks for",transfer.phedexid,[node.values() for node in checks.values()]
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
            print transfer.phedexid,"is done"
            cached_transfer_statuses[str(transfer.phedexid)] = copy.deepcopy(checks)
        else:
            print transfer.phedexid,"not finished"
            pprint.pprint( checks )



    open('cached_transfer_statuses.json','w').write( json.dumps( cached_transfer_statuses, indent=2))

    already_stuck = json.loads( open('/afs/cern.ch/user/c/cmst2/www/unified/stuck_transfers.json').read() )
    missing_in_action = defaultdict(list)


    print "-"*10,"Checking on workflows in staging","-"*10
    ## come back to workflows and check if they can go
    available_cache = {}
    presence_cache = {}
    for wfo,wfi in wfois:
        ## the site white list takes site, campaign, memory and core information
        (_,primaries,_,secondaries,sites_allowed) = wfi.getSiteWhiteList(verbose=False)
        se_allowed = [SI.CE_to_SE(site) for site in sites_allowed] 
        readys={}
        for need in list(primaries)+list(secondaries):
            if not need in done_by_input:
                print "no report for",need
                readys[need] = False
                continue

            if len(done_by_input[need]) and all(done_by_input[need].values()):
                print need,"is ready for",wfo.name
                readys[need] = True
            else:
                print need,"isn't ready"
                print json.dumps( done_by_input[need] , indent=2)
                readys[need] = False

        if readys and all(readys.values()):
            if wfo.status == 'staging':
                print "all needs for",wfo.name,"are fullfilled, setting staged"
                wfo.status = 'staged'
                session.commit()
            else:
                print "all needs for",wfo.name,"are fullfilled, already ",wfo.status
                print json.dumps( readys, indent=2 )
        else:
            ## there is missing input let's do something more elaborated
            for need in list(primaries)+list(secondaries):
                if not need in available_cache:    
                    available_cache[need]  = getDatasetBlocksFraction( url , need, sites=se_allowed )

            ## compute a time since staging to filter jump starting ?                    
            jump_ahead = False
            # check whether the inputs is already in the stuck list ...
            for need in list(primaries)+list(secondaries):
                if need in already_stuck: 
                    print need,"is stuck, so try to jump ahead"
                    jump_ahead = True
                    
            if jump_ahead:
                print "checking on availability for",wfo.name
                copies_needed,_ = wfi.getNCopies()
                print wfo.name,"wants",copies_needed,"copies"
                copies_needed = max(1,copies_needed-1)
                print wfo.name,"lowering by one unit to",copies_needed

                all_check = True
                for need in list(primaries):
                    available = available_cache[need]
                    print need,"is available",available,"times"
                    all_check &= (available >= copies_needed)
                    if not all_check: break

                for need in list(secondaries):
                    if not need in presence_cache:
                        presence_cache[need] = getDatasetPresence( url, need , within_sites=se_allowed)
                    presence = presence_cache[need]
                    available = available_cache[need]
                    all_check&= all([there for (there,frac) in presence.values()])

                if all_check:    
                    print "needs for",wfo.name,"are sufficiently fullfilled, setting staged"
                    wfo.status = 'staged'
                    session.commit()
                else:
                    print wfo.name,"has to wait a bit more",available
            else:
                print "not checking on availability for",wfo.name


    print "-"*10,"Checking on non-available datasets","-"*10    
    ## now check on those that are not fully available
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
            print "\tgot",[d for (_,d) in done_by_input[dsname].values()].count(True)
            print "\tmissing",missings
            missing_in_action[dsname].extend( missings )
        


    rr= open('/afs/cern.ch/user/c/cmst2/www/unified/lost_blocks_datasets.json','w')
    rr.write( json.dumps( known_lost_blocks, indent=2))
    rr.close()

    rr= open('/afs/cern.ch/user/c/cmst2/www/unified/lost_files_datasets.json','w')
    rr.write( json.dumps( known_lost_files, indent=2))
    rr.close()


    open('/afs/cern.ch/user/c/cmst2/www/unified/incomplete_transfers.json','w').write( json.dumps(missing_in_action, indent=2) )
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
                        report += "%s is not getting to %s, out of %s faster than %f [GB/s] since %f [d]\n"%(block,destination,", ".join(dones), rate, delay)
    print "\n"*2

    ## create tickets right away ?
    report+="\nbad sources "+",".join(bad_sources.keys())+"\n"
    for site,blocks in bad_sources.items():
        report+="\n\n%s:"%site+"\n\t".join(['']+list(blocks))
    report+="\nbad destinations "+",".join(bad_destinations.keys())+"\n"
    for site,blocks in bad_destinations.items():
        report+="\n\n%s:"%site+"\n\t".join(['']+list(blocks))

    print report

    open('/afs/cern.ch/user/c/cmst2/www/unified/stuck_transfers.json','w').write( json.dumps(dict([(k,v) for (k,v) in missing_in_action.items() if k in really_stuck_dataset]), indent=2) )
    open('/afs/cern.ch/user/c/cmst2/www/unified/logs/incomplete_transfers.log','w').write( report )
    #sendEmail('incomplete transfers', report,sender=None, destination=['dc.jorge10@uniandes.edu.co','aram.apyan@cern.ch','sidn@mit.edu'])


if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    UC = unifiedConfiguration()
    parser = optparse.OptionParser()
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    stagor(url, spec, options)
    htmlor()
