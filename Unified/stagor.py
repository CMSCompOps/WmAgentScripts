#!/usr/bin/env python
from assignSession import *
from utils import checkTransferStatus, checkTransferApproval, approveSubscription, getWorkflowByInput, workflowInfo, getDatasetBlocksFraction, findLostBlocks
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
    
    lost = json.loads(open('lost_blocks_datasets.json').read())
    still_lost = []
    for dataset in lost:
        l = findLostBlocks(url ,dataset)
        if not l:
            print dataset,"is not really lost"
        else:
            still_lost.append( dataset )
    open('lost_blocks_datasets.json','w').write( json.dumps( still_lost, indent=2) )

    cached_transfer_statuses = json.loads(open('cached_transfer_statuses.json').read())

    if options.fast:
        print "doing the fast check of staged with threshold:",options.goodavailability
        for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
            if specific and not specific in wfo.name: continue
            wfi = workflowInfo(url, wfo.name)
            (_,primaries,_,secondaries,sites_allowed) = wfi.getSiteWhiteList()
            if 'SiteWhitelist' in CI.parameters(wfi.request['Campaign']):
                sites_allowed = CI.parameters(wfi.request['Campaign'])['SiteWhitelist']
            if 'SiteBlacklist' in CI.parameters(wfi.request['Campaign']):
                sites_allowed = list(set(sites_allowed) - set(CI.parameters(wfi.request['Campaign'])['SiteBlacklist']))
            se_allowed = [SI.CE_to_SE(site) for site in sites_allowed] 
            all_check = True
            n_copies = wfi.getNCopies()
            for dataset in list(primaries):#+list(secondaries) ?
                #print se_allowed
                available = getDatasetBlocksFraction( url , dataset , sites=se_allowed )
                #all_check &= (available >= options.goodavailability)
                all_check &= (available >= n_copies)
                if not all_check: break

            if all_check:
                print "\t\t",wfo.name,"can go staged"
                wfo.status = 'staged'
                session.commit()
            else:
                print "\t",wfo.name,"can wait a bit more"
        return 

    for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
        wfi = workflowInfo(url, wfo.name)
        if wfi.request['RequestStatus'] in ['running-open','running-closed','completed']:
            print wfo.name,"is",wfi.request['RequestStatus']
            wfi.status='away'
            session.commit()
            continue
            
        _,primaries,_,secondaries = wfi.getIO()
        for dataset in list(primaries)+list(secondaries):
            done_by_input[dataset] = {}
            completion_by_input[dataset] = {}
            print wfo.name,"needs",dataset

    ## this loop is very expensive and will not function at some point.
    ## transfer objects should probably be deleted as some point

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

    missing_in_action = defaultdict(list)
    #print done_by_input
    print "\n----\n"
    for dsname in done_by_input:
        fractions = None
        if dsname in completion_by_input:
            fractions = itertools.chain.from_iterable([check.values() for check in completion_by_input.values()])
        
        ## the workflows in the waiting room for the dataset
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

        #need_sites = int(len(done_by_input[dsname].values())*0.7)+1
        need_sites = len(done_by_input[dsname].values())
        #if need_sites > 10:            need_sites = int(need_sites/2.)
        got = done_by_input[dsname].values().count(True)
        if all([wf.status != 'staging' for wf in using_wfos]):
            ## not a single ds-using wf is in staging => moved on already
            ## just forget about it
            print "presence of",dsname,"does not matter anymore"
            print "\t",done_by_input[dsname]
            print "\t",[wf.status for wf in using_wfos]
            print "\tneeds",need_sites
            continue #??
            
        ## should the need_sites reduces with time ?
        # with dataset choping, reducing that number might work as a block black-list.

        if len(done_by_input[dsname].values()) and all(done_by_input[dsname].values()):
            print dsname,"is everywhere we wanted"
            ## the input dataset is fully transfered, should consider setting the corresponding wf to staged
            for wf in using_wfos:
                if wf.status == 'staging':
                    print wf.name,"is with us. setting staged and move on"
                    wf.status = 'staged'
                    session.commit()
        elif fractions and len(list(fractions))>1 and set(fractions)==1:
            print dsname,"is everywhere at the same fraction"
            print "We do not want this in the end. we want the data we asked for"
            continue
            ## the input dataset is fully transfered, should consider setting the corresponding wf to staged
            for wf in using_wfos:
                if wf.status == 'staging':
                    print wf.name,"is with us everywhere the same. setting staged and move on"
                    wf.status = 'staged'
                    session.commit()
        elif got >= need_sites:
            print dsname,"is almost everywhere we wanted"
            #print "We do not want this in the end. we want the data we asked for"
            #continue
            ## the input dataset is fully transfered, should consider setting the corresponding wf to staged
            for wf in using_wfos:
                if wf.status == 'staging':
                    print wf.name,"is almost with us. setting staged and move on"
                    wf.status = 'staged'
                    session.commit()
        else:
            print "incomplete",dsname
            lost = findLostBlocks(url, dsname)
            lost_names = [item['name'] for item in lost]
            try:
                known_lost = json.loads(open('lost_blocks_datasets.json').read())
            except:
                print "enable to get the known_lost from local json file"
                known_lost = []

            if lost:
                print "We have lost",len(lost),"blocks",lost_names
                #print json.dumps( lost , indent=2 )

            if lost and not dsname in known_lost:
                ## make a deeper investigation of the block location to see whether it's really no-where no-where
                sendEmail('we have lost a few blocks', str(len(lost))+" in total.\nDetails \n:"+json.dumps( lost , indent=2 ))
                known_lost.append(dsname)
                rr= open('lost_blocks_datasets.json','w')
                rr.write( json.dumps( known_lost, indent=2))
                rr.close()
                ## should the status be change to held-staging and pending on a ticket



            missings = [pid for (pid,d) in done_by_input[dsname].items() if d==False] 
            print "\t",done_by_input[dsname]
            print "\tneeds",need_sites
            print "\tgot",got
            print "\tmissing",missings
            missing_in_action[dsname].extend( missings )

    open('incomplete_transfers.json','w').write( json.dumps(missing_in_action, indent=2) )
    print "Stuck transfers and datasets"
    print json.dumps( missing_in_action, indent=2 )

    print "Going further and make a report of stuck transfers"

    datasets_by_phid = defaultdict(set)
    for dataset in missing_in_action:
        for phid in missing_in_action[dataset]:
            #print dataset,"stuck through",phid
            datasets_by_phid[phid].add( dataset )

    bad_destinations = defaultdict(list)
    bad_sources = defaultdict(list)
    report = ""
    for phid,datasets in datasets_by_phid.items():
        issues = checkTransferLag( url, phid , datasets=list(datasets) )
        for dataset in issues:
            for block in issues[dataset]:
                for destination in issues[dataset][block]:
                    (block_size,destination_size,delay,rate,dones) = issues[dataset][block][destination]
                    if delay>7 and rate<0.0004:
                        if len(dones)>1:
                            ## its the destination that sucks
                            bad_destinations[destination].append( block )
                        else:
                            dum=[bad_sources[d].append( block ) for d in dones]

                        report += "%s is not getting to %s, out of %s faster than %f [GB/s] since %f [d]\n"%(block,destination,", ".join(dones), rate, delay)
    print "\n"*2

    ## create tickets right away ?
    report+="\nbad sources "+",".join(bad_sources.keys())+"\n"
    for site,blocks in bad_sources.items():
        report+="\n\n%s:"%site+"\n\t".join(['']+blocks)
    report+="\nbad destinations "+",".join(bad_destinations.keys())+"\n"
    for site,blocks in bad_destinations.items():
        report+="\n\n%s:"%site+"\n\t".join(['']+blocks)

    print report

    open('incomplete_transfers.log','w').write( report )
    sendEmail('incomplete transfers', report,sender=None, destination=['dc.jorge10@uniandes.edu.co','aram.apyan@cern.ch','sidn@mit.edu'])


if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    UC = unifiedConfiguration()
    parser = optparse.OptionParser()
    parser.add_option('-f','--fast', help='Make a quick check for available fraction',default=False,action='store_true')
    parser.add_option('-g','--goodavailability', help='The threshold on available fraction',default=UC.get('fast_stagor_pass_availability'),type=float)
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    stagor(url, spec, options)
    htmlor()
