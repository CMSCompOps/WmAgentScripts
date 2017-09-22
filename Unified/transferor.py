#!/usr/bin/env python
from assignSession import *
import reqMgrClient
from McMClient import McMClient
from utils import makeReplicaRequest
from utils import workflowInfo, siteInfo, campaignInfo, userLock
from utils import getDatasetChops, distributeToSites, getDatasetPresence, listSubscriptions, approveSubscription, getDatasetSize, updateSubscription, getWorkflows, componentInfo, getDatasetDestinations, getDatasetBlocks, DSS
from utils import unifiedConfiguration, monitor_dir, reqmgr_url
from utils import lockInfo
from utils import duplicateLock
import json
from collections import defaultdict
import optparse
import time
from htmlor import htmlor
from utils import sendEmail, sendLog
import math
import random
import copy

def transferor(url ,specific = None, talk=True, options=None):
    if userLock():   return
    if duplicateLock():  return

    use_mcm = True
    up = componentInfo(mcm=use_mcm, soft=['mcm'])
    if not up.check(): return
    use_mcm = up.status['mcm']

    if options and options.test:
        execute = False
    else:
        execute = True

    SI = siteInfo()
    CI = campaignInfo()
    #NLI = newLockInfo()
    #if not NLI.free(): return
    LI = lockInfo()
    if not LI.free(): return

    mcm = McMClient(dev=False)
    dss = DSS()

    #allowed_secondary = UC.get('')
    print "counting all being handled..."
    being_handled = len(session.query(Workflow).filter(Workflow.status == 'away').all())
    being_handled += len(session.query(Workflow).filter(Workflow.status.startswith('stag')).all())
    being_transfered = len(session.query(Workflow).filter(Workflow.status == 'staging').all())
    being_handled += len(session.query(Workflow).filter(Workflow.status.startswith('assistance-')).all())

    max_to_handle = options.maxworkflows
    max_to_transfer = options.maxstaging

    allowed_to_handle = max(0,max_to_handle - being_handled)
    allowed_to_transfer = max(0,max_to_transfer - being_transfered)
    wf_buffer = 5
    if allowed_to_handle<=wf_buffer: ## buffer for having several wf per transfer
        print "Not allowed to run more than",max_to_handle,"at a time. Currently",being_handled,"and",wf_buffer,"buffer"
    else:
        print being_handled,"already being handled",max_to_handle,"max allowed,",allowed_to_handle,"remaining","and",wf_buffer,"buffer"

    if allowed_to_transfer <= wf_buffer:
        print "Not allowed to transfer more than",max_to_transfer,"at a time. Currently",being_transfered,"and",wf_buffer,"buffer"
    else:
        print being_transfered,"already being transfered",max_to_transfer,"max allowed,",allowed_to_transfer,"remaining","and",wf_buffer,"buffer"

    print "... done"

    all_transfers=defaultdict(list)
    workflow_dependencies = defaultdict(set) ## list of wf.id per input dataset
    wfs_and_wfh=[]
    print "getting all wf to consider ..."
    cache = getWorkflows(url, 'assignment-approved', details=True)
    for wfo in session.query(Workflow).filter(Workflow.status.startswith('considered')).all():
        print "\t",wfo.name
        if specific and not specific in wfo.name: continue
        cache_r =filter(lambda d:d['RequestName']==wfo.name, cache)
        if len(cache_r):
            wfs_and_wfh.append( (wfo, workflowInfo( url, wfo.name, spec=False, request = cache_r[0]) ) )
        else:
            wfs_and_wfh.append( (wfo, workflowInfo( url, wfo.name, spec=False) ) )
    print "... done"

    transfers_per_sites = defaultdict(int)
    input_sizes = {}
    ignored_input_sizes = {}
    input_cput = {}
    input_st = {}
    ## list the size of those in transfer already
    in_transfer_priority=None
    min_transfer_priority=None
    print "getting all wf in staging ..."
    stucks = json.loads(open('%s/stuck_transfers.json'%monitor_dir).read())
    
    for wfo in session.query(Workflow).filter(Workflow.status=='staging').all():
        wfh = workflowInfo( url, wfo.name, spec=False)
        #(lheinput,primary,parent,secondary) = wfh.getIO()
        #sites_allowed = getSiteWhiteList( (lheinput,primary,parent,secondary) )
        print wfo.name,"staging"
        (lheinput,primary,parent,secondary,sites_allowed) = wfh.getSiteWhiteList()
        for site in sites_allowed: ## we should get the actual transfer destination instead of the full white list
            transfers_per_sites[site] += 1 
        #input_cput[wfo.name] = wfh.getComputingTime()
        #input_st[wfo.name] = wfh.getSystemTime()
        bwl = []
        if 'BlockWhitelist' in wfh.request and wfh.request['BlockWhitelist']:
            bwl = wfh.request['BlockWhitelist']
        rwl = wfh.getRunWhiteList()
        lwl = wfh.getLumiWhiteList()
        for prim in primary:  
            blocks = copy.deepcopy( bwl )
            if rwl: 
                blocks = list(set( blocks + getDatasetBlocks( prim, runs= rwl ) ))
            if lwl:
                blocks = list(set( blocks + getDatasetBlocks( prim, lumis= lwl)))

            ds_s = dss.get( prim, blocks=blocks)
            if prim in stucks: 
                wfh.sendLog('transferor', "%s appears stuck, so not counting it %s [GB]"%( prim, ds_s))
                ignored_input_sizes[prim] = ds_s
            else:
                input_sizes[prim] = ds_s
                wfh.sendLog('transferor', "%s needs %s [GB]"%( wfo.name, ds_s))
        if in_transfer_priority==None:
            in_transfer_priority = int(wfh.request['RequestPriority'])
        else:
            in_transfer_priority = max(in_transfer_priority, int(wfh.request['RequestPriority']))
        if min_transfer_priority==None:
            min_transfer_priority = int(wfh.request['RequestPriority'])
        else:
            min_transfer_priority = min(min_transfer_priority, int(wfh.request['RequestPriority']))



    try:
        print "Ignored input sizes"
        ignored_values = list(ignored_input_sizes.items())
        ignored_values.sort( key = lambda i : i[1] )
        print "\n".join( map(str, ignored_values ) )
        print "Considered input sizes"
        considered_values = list(input_sizes.items())
        considered_values.sort( key = lambda i : i[1] )
        print "\n".join( map(str, considered_values) )
    except Exception as e:
        print "trying to print the summary of input size"
        print str(e)

    print "... done"
    print "Max priority in transfer already",in_transfer_priority
    print "Min priority in transfer already",min_transfer_priority
    print "transfers per sites"
    print json.dumps( transfers_per_sites, indent=2)
    in_transfer_already = sum(input_sizes.values())
    cput_in_transfer_already = sum(input_cput.values())
    st_in_transfer_already = sum(input_st.values())

    ## list the size of all inputs
    primary_input_per_workflow_gb = defaultdict(float)
    print "getting all input sizes ..."
    for (wfo,wfh) in wfs_and_wfh:
        (_,primary,_,_) = wfh.getIO()
        #input_cput[wfo.name] = wfh.getComputingTime()
        #input_st[wfo.name] = wfh.getSystemTime()
        for prim in primary:
            ## do not count it if it appears stalled !
            prim_size = dss.get( prim )
            input_sizes[prim] = prim_size
            primary_input_per_workflow_gb[wfo.name] += prim_size
    print "... done"

    # shuffle first by name
    random.shuffle( wfs_and_wfh )
    # Sort smallest transfers first; allows us to transfer as many as possible workflows.
    def prio_and_size( i, j):
        if int(i[1].request['RequestPriority']) == int(j[1].request['RequestPriority']):
            return cmp(int(primary_input_per_workflow_gb.get(j[0].name, 0)), int(primary_input_per_workflow_gb.get(i[0].name, 0)) )
        else:
            return cmp(int(i[1].request['RequestPriority']),int(j[1].request['RequestPriority']))

    #wfs_and_wfh.sort(cmp = prio_and_size, reverse=True)
    #wfs_and_wfh.sort(cmp = lambda i,j : cmp(int(primary_input_per_workflow_gb.get(i[0].name, 0)), int(primary_input_per_workflow_gb.get(j[0].name, 0)) ))
    #sort by priority higher first
    wfs_and_wfh.sort(cmp = lambda i,j : cmp(int(i[1].request['RequestPriority']),int(j[1].request['RequestPriority']) ), reverse=True)

    if min_transfer_priority==None or in_transfer_priority ==None:
        print "nothing is lining up for transfer"
        sendLog("transferor","No request in staging, using first request to set priority limit")
        if len(wfs_and_wfh):
            min_transfer_priority = wfs_and_wfh[0][1].request['RequestPriority']
            in_transfer_priority = wfs_and_wfh[0][1].request['RequestPriority']
        else:
            return

    cput_grand_total = sum(input_cput.values())
    cput_to_transfer = cput_grand_total - cput_in_transfer_already
    st_grand_total = sum(input_st.values())
    st_to_transfer = st_grand_total - st_in_transfer_already
    print "%15.4f [CPU h] worth already in transfer"%cput_in_transfer_already
    print "%15.4f [CPU h] worth is the current requested transfer load"%cput_to_transfer
    print "%15.4f [h] worth of absolute system time in transfer"%( cput_in_transfer_already / SI.availableSlots())
    print "%15.4f [h] worth of absolute system time is the current requested transfer load"%( cput_to_transfer / SI.availableSlots())
    print "%15.4f [h] worth of theoritical system time in transfer"%( st_in_transfer_already )
    print "%15.4f [h] worth of theoritical system time is the current requested transfer load"%( st_to_transfer )


    grand_total =  sum(input_sizes.values()) 
    to_transfer = grand_total  - in_transfer_already
    grand_transfer_limit = options.maxtransfer
    #grand_transfer_limit = SI.total_disk()*0.25*1024## half of the free sapce in TB->GB
    
    transfer_limit = grand_transfer_limit - in_transfer_already
    print "%15.4f GB already being transfered"%in_transfer_already
    print "%15.4f GB is the current requested transfer load"%to_transfer
    print "%15.4f GB is the global transfer limit"%grand_transfer_limit
    print "%15.4f GB is the available limit"%transfer_limit


    max_staging_per_site = options.maxstagingpersite
                    
    # the max priority value per dataset.
    max_priority = defaultdict(int)
    needs_transfer=0 ## so that we can count'em
    passing_along = 0
    transfer_sizes={}
    went_over_budget=False
    destination_cache = {}
    no_goes = set()

    max_per_round = UC.get('max_per_round').get('transferor',None)
    if max_per_round and not spec:
        wfs_and_wfh = wfs_and_wfh[:max_per_round]
    
    for (wfo,wfh) in wfs_and_wfh:
        print wfo.name,"to be transfered with priority",wfh.request['RequestPriority']

        if wfh.request['RequestStatus']!='assignment-approved':
            if wfh.request['RequestStatus'] in ['aborted','rejected','rejected-archived','aborted-archived']:
                if wfh.isRelval():
                    wfo.status = 'forget'
                else:
                    wfo.status = 'trouble' ## so that we look or a replacement
            else:
                wfo.status = 'away'
            wfh.sendLog('transferor', '%s in status %s, setting %s'%( wfo.name,wfh.request['RequestStatus'],wfo.status))
            continue

        (lheinput,primary,parent,secondary,sites_allowed) = wfh.getSiteWhiteList()
        #(_,primary,_,_) = wfh.getIO()
        this_load=sum([input_sizes[prim] for prim in primary])
        no_budget = False
        if ( this_load and (sum(transfer_sizes.values())+this_load > transfer_limit or went_over_budget ) ):
            if went_over_budget:
                wfh.sendLog('transferor', "Transfer has gone over bubget.")
            else:
                wfh.sendLog('transferor', "Transfer will go over bubget.")
            wfh.sendLog('transferor', "%15.4f GB this load, %15.4f GB already this round, %15.4f GB is the available limit"%(this_load, sum(transfer_sizes.values()), transfer_limit))
            #if sum(transfer_sizes.values()) > transfer_limit:
            went_over_budget = True
            if in_transfer_priority!=None and min_transfer_priority!=None:
                if int(wfh.request['RequestPriority']) >= in_transfer_priority and min_transfer_priority!=in_transfer_priority:
                    wfh.sendLog('transferor',"Higher priority sample %s >= %s go-on over budget"%( wfh.request['RequestPriority'], in_transfer_priority))
                else:
                    if not options.go: 
                        wfh.sendLog('transferor',"%s minimum priority %s < %s : stop"%( min_transfer_priority,wfh.request['RequestPriority'],in_transfer_priority))
                        no_budget = True

        ## throtlle by campaign go
        no_go = False
        if not wfh.go(log=True) and not options.go:
            no_go = True
            no_goes.add( wfo.name )
            
        allowed_secondary = {}
        overide_parameters = {}
        check_secondary = (not wfh.isRelval())
        output_tiers = list(set([o.split('/')[-1] for o in wfh.request['OutputDatasets']]))
        for campaign in wfh.getCampaigns():
            if campaign in CI.campaigns:
                overide_parameters.update( CI.campaigns[campaign] )
            if campaign in CI.campaigns and 'secondaries' in CI.campaigns[campaign]:
                if CI.campaigns[campaign]['secondaries']:
                    allowed_secondary.update( CI.campaigns[campaign]['secondaries'] )
                    check_secondary = True
            if campaign in CI.campaigns and 'banned_tier' in CI.campaigns[campaign]:
                banned_tier = list(set(CI.campaigns[campaign]['banned_tier']) & set(output_tiers))
                if banned_tier:
                    no_go=True
                    wfh.sendLog('transferor','These data tiers %s are not allowed'%(','.join( banned_tier)))
                    sendLog('transferor','These data tiers %s are not allowed in %s'%(','.join( banned_tier), wfo.name), level='critical')

        if secondary and check_secondary:
            if (set(secondary)&set(allowed_secondary.keys())!=set(secondary)):
                wfh.sendLog('transferor','%s is not an allowed secondary'%(', '.join(set(secondary)-set(allowed_secondary.keys()))))
                sendLog('transferor','%s is not an allowed secondary'%(', '.join(set(secondary)-set(allowed_secondary.keys()))), level='critical')
                if not options.go: 
                    no_go = True
            for sec in secondary:
                if sec in allowed_secondary:
                    overide_parameters.update( allowed_secondary[sec] )

        if 'SiteWhitelist' in overide_parameters:
            sites_allowed = list(set(sites_allowed) & set(overide_parameters['SiteWhitelist']))

        if no_go:
            continue
        ## check if the batch is announced

        #def check_mcm(wfn):
        #    announced=False
        #    is_real=False
        #    if not wfn.startswith('pdmvserv'):
        #        is_real = True
        #    try:
        #        for b in mcm.getA('batches',query='contains=%s'% wfo.name):
        #            is_real = True
        #            if b['status']=='announced': 
        #                announced=True 
        #                break
        #    except:
        #        try:
        #            for b in mcm.getA('batches',query='contains=%s'% wfo.name):
        #                is_real = True
        #                if b['status']=='announced': 
        #                    announced=True 
        #                    break
        #        except:
        #            print "could not get mcm batch announcement, assuming not real"
        #    return announced,is_real

        #if not use_mcm:
        #    announced,is_real = False,True
        #else:
        #    if wfh.request['RequestType'] in ['ReReco']:
        #        announced,is_real = True,True
        #    else:
        #        announced,is_real = check_mcm( wfo.name )

        #if not announced:
        #    wfh.sendLog('transferor', "does not look announced.")

            
        #if not is_real:
        #    wfh.sendLog('transferor', "does not appear to be genuine.")

            ## prevent any duplication. if the wf is not mentioned in any batch, regardless of status
        #    continue

        ## check on a grace period
        #injection_time = time.mktime(time.strptime('.'.join(map(str,wfh.request['RequestDate'])),"%Y.%m.%d.%H.%M.%S")) / (60.*60.)
        #now = time.mktime(time.gmtime()) / (60.*60.)
        #if float(now - injection_time) < 4.:
        #    if not options.go and not announced: 
        #        wfh.sendLog('transferor', "It is too soon to start transfer: %3.2fH since injection"%(now - injection_time))
        #        continue


        if passing_along >= allowed_to_handle:
            #if int(wfh.request['RequestPriority']) >= in_transfer_priority and min_transfer_priority!=in_transfer_priority:
            if in_transfer_priority!=None and min_transfer_priority!=None:
                if int(wfh.request['RequestPriority']) >= in_transfer_priority and int(wfh.request['RequestPriority']) !=min_transfer_priority:
                    ## higher priority, and not only this priority being transfered
                    wfh.sendLog('transferor',"Higher priority sample %s >= %s go-on over %s"%( wfh.request['RequestPriority'], in_transfer_priority, max_to_handle))
                else:
                    wfh.sendLog('transferor'," Not allowed to pass more than %s at a time. Currently %s handled, and adding %s"%( max_to_handle, being_handled, passing_along))
                    if not options.go: 
                        ## should not allow to jump that fence
                        break

        if this_load and needs_transfer >= allowed_to_transfer:
            if in_transfer_priority!=None and min_transfer_priority!=None:
                if int(wfh.request['RequestPriority']) >= in_transfer_priority and int(wfh.request['RequestPriority']) !=min_transfer_priority:
                    ## higher priority, and not only this priority being transfered
                    wfh.sendLog('transferor',"Higher priority sample %s >= %s go-on over %s"%(wfh.request['RequestPriority'], in_transfer_priority,max_to_transfer))
                else:
                    wfh.sendLog('transferor',"Not allowed to transfer more than %s at a time. Currently %s transfering, and adding %s"%( max_to_transfer, being_transfered, needs_transfer))
                    if not options.go: 
                        no_budget = True


        if no_budget:
            break ## try this for a while to make things faster
            #continue

        ## the site white list considers site, campaign, memory and core information
        if options and options.tosites:
            sites_allowed = options.tosites.split(',')


        for dataset in list(primary)+list(parent)+list(secondary):
            ## lock everything flat
            #NLI.lock( dataset )
            LI.lock( dataset , reason='staging' )

        if not sites_allowed:
            wfh.sendLog('transferor',"not possible site to run at")
            #sendEmail("no possible sites","%s has no possible sites to run at"%( wfo.name ))
            sendLog('transferor',"%s has no possible sites to run at"%( wfo.name ),level='critical')
            continue

        blocks = []
        if 'BlockWhitelist' in wfh.request and wfh.request['BlockWhitelist']:
            blocks = wfh.request['BlockWhitelist']
        rwl = wfh.getRunWhiteList()
        if rwl:
            ## augment with run white list
            for dataset in primary:
                blocks = list(set( blocks + getDatasetBlocks( dataset, runs= rwl ) ))
        lwl = wfh.getLumiWhiteList()
        if lwl:
            ## augment with the lumi white list
            for dataset in primary:
                blocks = list(set( blocks + getDatasetBlocks( dataset, lumis= lwl ) ))

        if blocks:
            print "Reading only",len(blocks),"blocks in input"

        can_go = True
        staging=False
        allowed=True
        primary_destinations = set()
        if primary:
            
            copies_needed_from_CPUh,CPUh = wfh.getNCopies()

            if talk:
                print wfo.name,'reads',', '.join(primary),'in primary'
            ## chope the primary dataset 
            for prim in primary:
                ## keep track of what needs what
                workflow_dependencies[prim].add( wfo.id )

                max_priority[prim] = max(max_priority[prim],int(wfh.request['RequestPriority']))

                wfh.sendLog('transferor',"Would make %s  from cpu requirement %s"%( copies_needed_from_CPUh, CPUh))
                copies_needed = copies_needed_from_CPUh

                if 'Campaign' in wfh.request and wfh.request['Campaign'] in CI.campaigns and 'maxcopies' in CI.campaigns[wfh.request['Campaign']]:
                    copies_needed_from_campaign = CI.campaigns[wfh.request['Campaign']]['maxcopies']
                    copies_needed = min(copies_needed_from_campaign, copies_needed)
                    
                    wfh.sendLog('transferor',"Maxed to %s by campaign configuration %s"%( copies_needed, wfh.request['Campaign']))


                ### new ways of making the whole thing
                destinations,all_block_names = getDatasetDestinations(url, prim, within_sites = [SI.CE_to_SE(site) for site in sites_allowed], only_blocks=blocks )
                print json.dumps(destinations, indent=2)

                ## get where the dataset is in full and completed
                prim_location = [site for (site,info) in destinations.items() if info['completion']==100 and info['data_fraction']==1]
                ## the rest is places it is going to be
                prim_destination = [site for site in destinations.keys() if not site in prim_location]
                ## veto the site with no current disk space, for things that are not relval
                prim_destination = [site for site in prim_destination if (SI.disk[site] or wfh.isRelval())]
                

                if len(prim_location) >= copies_needed:
                    wfh.sendLog('transferor',"The input is all fully in place at %s sites %s"%( len(prim_location), sorted(prim_location)))
                    continue
                copies_needed = max(0,copies_needed - len(prim_location))
                wfh.sendLog('transferor',"not counting existing copies ; now need %s"% copies_needed)
                copies_being_made = [ sum([info['blocks'].keys().count(block) for site,info in destinations.items() if site in prim_destination]) for block in all_block_names]

                latching_on_transfers = set()
                [latching_on_transfers.update(info['blocks'].values()) for site,info in destinations.items() if site in prim_destination]
                latching_on_transfers = list(latching_on_transfers)
                #print latching_on_transfers

                ## figure out where all this is going to go
                prim_to_distribute = [site for site in sites_allowed if not SI.CE_to_SE(site) in prim_location]
                prim_to_distribute = [site for site in prim_to_distribute if not SI.CE_to_SE(site) in prim_destination]
                ## take out the ones that cannot receive transfers
                potential_destinations = len(prim_to_distribute)
                #prim_to_distribute = [site for site in prim_to_distribute if not SI.CE_to_SE(site) in SI.sites_veto_transfer]
                prim_to_distribute = [site for site in prim_to_distribute if (SI.disk[SI.CE_to_SE(site)] or wfh.isRelval())]

                ## do we want to restrict transfers if the amount of site in vetoe are too large ?
                

                wfh.sendLog('transferor',"Could be going to: %s"% sorted( prim_to_distribute))
                if not prim_to_distribute or any([transfers_per_sites[site] < max_staging_per_site for site in prim_to_distribute]):
                    ## means there is openings let me go
                    print "There are transfer slots available:",[(site,transfers_per_sites[site]) for site in prim_to_distribute]
                    #for site in sites_allowed:
                    #    #increment accross the board, regardless of real destination: could be changed
                    #    transfers_per_sites[site] += 1
                else:
                    if int(wfh.request['RequestPriority']) >= in_transfer_priority and min_transfer_priority!=in_transfer_priority:
                        wfh.sendLog('transferor', "Higher priority sample %s >= %s go-on over transfer slots available"%(wfh.request['RequestPriority'], in_transfer_priority))
                    else:
                        wfh.sendLog('transferor',"Not allowed to transfer more than %s per site at a time. Going overboard for %s"%( max_staging_per_site, sorted([site for site in prim_to_distribute if transfers_per_sites[site]>=max_staging_per_site])))
                        if not options.go:
                            allowed = False
                            break

                for latching in latching_on_transfers:

                    existings = session.query(TransferImp).filter( TransferImp.phedexid == int(latching)).filter( TransferImp.workflow_id == wfo.id ).all()
                    if not existings:
                        tri = TransferImp( phedexid = int(latching),
                                           workflow = wfo )
                        print "adding",wfo.id,"with phedexid",latching
                        session.add( tri )
                    else:
                        for existing in existings:
                            existing.active = True

                    session.flush()
                    #if not options.test:
                    #    #session.commit()
                    #    pass
                    #else:
                    #    session.flush() ## regardless of commit later on, we need to let the next wf feeding on this transfer to see it in query
                    can_go = False
                    transfer_sizes[prim] = input_sizes[prim]
                    staging = True

                # reduce the number of copies required by the on-going full transfer : how do we bootstrap on waiting for them ??
                #copies_needed = max(0,copies_needed - len(prim_destination))
                copies_needed = max(0,copies_needed - min(copies_being_made))
                wfh.sendLog('transferor', "Not counting the copies being made ; then need %s"% copies_needed)                    
                if copies_needed == 0:
                    wfh.sendLog('transferor', "The input is either fully in place or getting in full somewhere with %s"% latching_on_transfers)
                    can_go = True
                    continue
                elif len(prim_to_distribute)==0:
                    wfh.sendLog('transferor', "We are going to need extra copies of %s, but no destinations seems available"%(prim))
                    sendLog('transferor', "We are going to need extra copies of %s, but no destinations seems available"%(prim),level='critical')
                    prim_to_distribute = [site for site in sites_allowed if not SI.CE_to_SE(site) in prim_location]
                    prim_to_distribute = [site for site in prim_to_distribute if not SI.CE_to_SE(site) in SI.sites_veto_transfer]
                    

                if len(prim_to_distribute)>0: ## maybe that a parameter we can play with to limit the 
                    if not options or options.chop:
                        ### hard include the tape disk andpoint ?
                        #tapes = [site for site in  getDatasetPresence( url, prim, vetos=['T0','T2','T3','Disk']) if site.endswith('MSS')]
                        chops,sizes = getDatasetChops(prim, chop_threshold = options.chopsize, only_blocks=blocks)
                        spreading = distributeToSites( chops, prim_to_distribute, n_copies = copies_needed, weights=SI.cpu_pledges, sizes=sizes)
                        ## prune the blocks/destination that are already in the making, so that subscription don't overlap
                        for site in spreading:
                            for block in list(spreading[site]):
                                if site in destinations and block in destinations[site]['blocks'].keys():
                                    ## prune it
                                    spreading[site].remove( block )


                        transfer_sizes[prim] = sum(sizes)
                        if not spreading:
                            sendLog('transferor','cannot send %s to any site, it cannot fit anywhere'% prim, level='critical')
                            wfh.sendLog('transferor', "cannot send to any site. %s cannot seem to fit anywhere"%(prim))
                            staging=False
                            can_go = False
                    
                    else:
                        spreading = {} 
                        for site in prim_to_distribute: 
                            if blocks:
                                spreading[site]=blocks
                            else:
                                spreading[site]=[prim]
                        transfer_sizes[prim] = input_sizes[prim] ## this is approximate if blocks are specified
                    can_go = False
                    wfh.sendLog('transferor', "selected CE destinations %s"%(sorted( spreading.keys())))
                    for (site,items) in spreading.items():
                        all_transfers[site].extend( items )
                        transfers_per_sites[site] += 1
                        primary_destinations.add( site ) 
                else:
                    can_go = False
                    allowed = False

        if not allowed:
            wfh.sendLog('transferor', "Not allowed to move on with")
            continue


        if secondary:

            override_sec_destination = []
            if 'SecondaryLocation' in CI.campaigns[wfh.request['Campaign']]:
                override_sec_destination  = CI.campaigns[wfh.request['Campaign']]['SecondaryLocation']
            if 'SecondaryLocation' in overide_parameters:
                override_sec_destination  = overide_parameters['SecondaryLocation']
            print wfo.name,'reads',', '.join(secondary),'in secondary'
            for sec in secondary:

                workflow_dependencies[sec].add( wfo.id )

                if True:
                    ## new style, failing on minbias
                    if not sec in destination_cache:
                        ## this is barbbaric, and does not show the correct picture on workflow by workflow with different whitelist
                        destination_cache[sec],_ = getDatasetDestinations(url, sec) ## NO SITE WHITE LIST ADDED
                        #destination_cache[sec],_ = getDatasetDestinations(url, sec, within_sites = [SI.CE_to_SE(site) for site in sites_allowed])

                    ## limit to the site whitelist NOW
                    se_allowed = set([SI.CE_to_SE(site) for site in sites_allowed])
                    destinations = dict([(k,v) for (k,v) in destination_cache[sec].items() if k in se_allowed])
                    ## truncate location/destination to those making up for >90% of the dataset
                    bad_destinations = [destinations.pop(site) for (site,info) in destinations.items() if info['data_fraction']<0.9]
                    sec_location = [site for (site,info) in destinations.items() if info['completion']>=95]
                    sec_destination = [site for site in destinations.keys() if not site in sec_location] ## this is in SE
                else:
                    ## old style
                    presence = getDatasetPresence( url, sec )
                    sec_location = [site for site,pres in presence.items() if pres[1]>90.] ## more than 90% of the minbias at sites
                    subscriptions = listSubscriptions( url ,sec )
                    sec_destination = [site for site in subscriptions] 
                    
                
                #sec_to_distribute = [site for site in sites_allowed if not any([osite.startswith(site) for osite in sec_location])]
                sec_to_distribute = [site for site in sites_allowed if not SI.CE_to_SE(site) in sec_location]
                #sec_to_distribute = [site for site in sec_to_distribute if not any([osite.startswith(site) for osite in sec_destination])]
                sec_to_distribute = [site for site in sec_to_distribute if not SI.CE_to_SE(site) in sec_destination]
                #sec_to_distribute = [site for site in sec_to_distribute if not  any([osite.startswith(site) for osite in SI.sites_veto_transfer])]
                #sec_to_distribute = [site for site in sec_to_distribute if not  SI.CE_to_SE(site) in SI.sites_veto_transfer]
                sec_to_distribute = [site for site in sec_to_distribute if (SI.disk[SI.CE_to_SE(site)] or wfh.isRelval())]

                if override_sec_destination:
                    ## intersect with where we want the PU to be
                    not_needed_anymore = list(set(sec_to_distribute) - set(override_sec_destination))
                    #sendEmail("secondary superfluous","the dataset %s could be removed from %s"%( sec, not_needed_anymore ))
                    sendLog('transferor', "the dataset %s could be removed from %s"%( sec, not_needed_anymore ))
                    sec_to_distribute = list(set(sec_to_distribute) & set(override_sec_destination))

                if len( sec_to_distribute )>0:
                    print "secondary could go to",sorted(sec_to_distribute)
                    sec_size = dss.get( sec )
                    for site in sec_to_distribute:
                        site_se =SI.CE_to_SE(site)
                        if (SI.disk[site_se]*1024.) > sec_size or wfh.isRelval():
                            wfh.sendLog('transferor', 'Sending %s to %s'%( sec, site ))
                            all_transfers[site].append( sec )
                            can_go = False
                        else:
                            print "could not send the secondary input to",site_se,"because it is too big for the available disk",SI.disk[site_se]*1024,"GB need",sec_size
                            if primary_destinations and site in primary_destinations:
                                #sendEmail('secondary input too big','%s is too big (%s) for %s (%s)'%( sec, sec_size, site_se, SI.disk[site_se]*1024))
                                sendLog('transferor', '%s is too big (%s) for %s (%s). %s will not be able to run there.'%( sec, sec_size, site_se, SI.disk[site_se]*1024, wfo.name), level='critical')
                                wfh.sendLog('transferor', '%s is too big (%s) for %s (%s). will not be able to run there.'%( sec, sec_size, site_se, SI.disk[site_se]*1024))
                else:
                    print "the secondary input does not have to be send to site"

        ## is that possible to do something more
        if can_go:
            ## no explicit transfer required this time
            if staging:
                ## but using existing ones
                wfh.sendLog('transferor', "latches on existing transfers, and nothing else, settin staging")
                wfo.status = 'staging'
                needs_transfer+=1
            else:
                wfh.sendLog('transferor', "should just be assigned now to %s"%sorted(sites_allowed))
                wfo.status = 'staged'
            passing_along+=1
            wfh.sendLog('transferor', "setting %s status to %s"%(wfo.name,wfo.status))
            #session.commit()
            continue
        else:
            ## there is an explicit transfer required
            if staging:
                ## and also using an existing one
                wfh.sendLog('transferor', "latches on existing transfers")
                if not options.test:
                    wfo.status = 'staging'
                    wfh.sendLog('transferor', "setting %s status to %s"%(wfo.name,wfo.status))
                    #session.commit()
            wfh.sendLog('transferor',"needs a transfer")
            needs_transfer+=1
            passing_along+=1

    if no_goes:
        #sendEmail("no go for managing","No go for \n"+"\n".join( no_goes ))
        sendLog('transferor', "No go for \n"+"\n".join( sorted(no_goes) ), level='critical')

    print "accumulated transfers"
    print json.dumps(all_transfers, indent=2)
    fake_id=-1
    wf_id_in_prestaging=set()

    for (site,items_to_transfer) in all_transfers.iteritems():
        items_to_transfer = list(set(items_to_transfer))

        ## convert to storage element
        site_se = SI.CE_to_SE(site)

        ## site that do not want input datasets
        #if site in SI.sites_veto_transfer: 
        #    print site,"does not want transfers"
        #    continue

        ## throttle the transfer size to T2s ? we'd be screwed by a noPU sample properly configured.

        ## massage a bit the items
        blocks = [it for it in items_to_transfer if '#' in it]
        block_datasets = list(set([it.split('#')[0] for it in blocks]))
        datasets = [it for it in items_to_transfer if not '#' in it]

        details_text = "Making a replica to %s (CE) %s (SE) for"%( site, site_se)
        

        #print "\t",len(blocks),"blocks"
        ## remove blocks if full dataset is send out
        blocks = [block for block in blocks if not block.split('#')[0] in datasets]
        #print "\t",len(blocks),"needed blocks for",list(set([block.split('#')[0] for block in blocks]))
        #print "\t",len(datasets),"datasets"
        #print "\t",datasets
        details_text += '\n\t%d blocks'%len(blocks)
        details_text += '\n\t%d needed blocks for %s'%( len(blocks), sorted(list(set([block.split('#')[0] for block in blocks]))))
        details_text += '\n\t%d datasets'% len(datasets)
        details_text += '\n\t%s'%sorted(datasets)
        
        items_to_transfer = blocks + datasets

        if execute:
            sendLog('transferor', details_text)
        else:
            print "Would make a replica to",site,"(CE)",site_se,"(SE) for"
            print details_text

        ## operate the transfer
        if options and options.stop:
            ## ask to move-on
            answer = raw_input('Continue with that ?')
            if not answer.lower() in ['y','yes','go']:
                continue

        if execute:
            priority = 'normal'
            cds = [ds for ds in datasets+block_datasets if ds in max_priority]
            if cds and False: ## I don't think this is working. subscription should be updated on the fly and regularly for raising the priority if needed
                ## decide on an overall priority : that's a bit too large though
                if any([max_priority[ds]>=90000 for ds in cds]):
                    priority = 'high'
                elif all([max_priority[ds]<80000 for ds in cds]):
                    priority = 'low'
                
            result = makeReplicaRequest(url, site_se, items_to_transfer, 'prestaging', priority=priority, approve=True)
        else:
            result= {'phedex':{'request_created' : []}}
            fake_id-=1



        if not result:
            sendLog('transferor','Could not make a replica request for items %s to site %s'%(items_to_transfer,site),level='critical')
            continue
        for phedexid in [o['id'] for o in result['phedex']['request_created']]:
            print phedexid,"transfer created"
            for transfering in list(set(map(lambda it : it.split('#')[0], items_to_transfer))):
                for wfid in workflow_dependencies[transfering]:
                    new_transfer = session.query(TransferImp).filter(TransferImp.phedexid == int(phedexid)).filter( TransferImp.workflow_id == wfid ).first()
                    if not new_transfer:
                        new_transfer = TransferImp( phedexid = phedexid,
                                                    workflow = session.query(Workflow).get( wfid ))
                        session.add( new_transfer )
                    else:
                        new_transfer.active = True
                    

                    wf_id_in_prestaging.add( wfid )
            #session.commit()

    for wfid in wf_id_in_prestaging:
        tr_wf = session.query(Workflow).get(wfid)
        if tr_wf and tr_wf.status!='staging':
            if execute:
                tr_wf.status = 'staging'
                if talk:
                    print "setting",tr_wf.name,"to staging"
        #session.commit()

    ## one big session commit at the end that everything went fine
    session.commit()

if __name__=="__main__":
    UC = unifiedConfiguration()
    url = reqmgr_url
    parser = optparse.OptionParser()  
    parser.add_option('-t','--test',help="Perform the test and display information",default=False,action='store_true')
    parser.add_option('-s','--stop',help="Stop ask and go",default=False,action='store_true')
    parser.add_option('-n','--nochop',help='Do no chop the input to the possible sites',default=True,dest='chop',action='store_false')
    parser.add_option('-c','--chopsize',help='The threshold for choping input dataset',default=UC.get('chopping_threshold_in_GB'),type=int)
    parser.add_option('--tosites',help='Provide a coma separated list of sites to transfer input to',default=None)
    parser.add_option('--go',help="Overrides no-go from campaign, announced, or grace period",default=False,action='store_true')
    parser.add_option('--maxtransfer',help="The limit in GB of the total size of input that can be transfered", default=UC.get('max_transfer_in_GB'),type=int)
    parser.add_option('--maxworkflows',help="The limit on the number of workflow we want to keep in the system at a time",default=UC.get('max_handled_workflows'),type=int)
    parser.add_option('--maxstaging',help="The limit on the number of workflow we want to keep in staging at a time",default=UC.get('max_staging_workflows'),type=int)
    parser.add_option('--maxstagingpersite',help="The limit on the number of workflow we want to keep in staging at a time per site",default=UC.get('max_staging_workflows_per_site'),type=int)
    parser.add_option('--maxcopy',help="Specify the number of copies of the input we need", default=3,type=int)
    (options,args) = parser.parse_args()

    spec=None
    if len(args):
        spec = args[0]

    transferor(url,spec,options=options)

    htmlor()
