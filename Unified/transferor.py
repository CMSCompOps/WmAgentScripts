#!/usr/bin/env python
from assignSession import *
import reqMgrClient
from McMClient import McMClient
from utils import makeReplicaRequest
from utils import workflowInfo, siteInfo, campaignInfo, userLock
from utils import getDatasetChops, distributeToSites, getDatasetPresence, listSubscriptions, getSiteWhiteList, approveSubscription, getDatasetSize, updateSubscription, getWorkflows
import json
from collections import defaultdict
import optparse
import time
from htmlor import htmlor

class DSS:
    def __init__(self):
        self.db = json.loads(open('dss.json').read())
    
    def get(self, dataset ):
        if not dataset in self.db:
            self.db[dataset] = getDatasetSize( dataset )
        return self.db[dataset]

    def __del__(self):
        open('dss.json','w').write( json.dumps( self.db ))
                                    

def transferor(url ,specific = None, talk=True, options=None):
    if userLock('transferor'):   return

    if options and options.test:
        execute = False
    else:
        execute = True

    SI = siteInfo()
    CI = campaignInfo()
    mcm = McMClient(dev=False)
    dss = DSS()

    print "counting all being handled..."
    being_handled = len(session.query(Workflow).filter(Workflow.status == 'away').all())
    being_handled += len(session.query(Workflow).filter(Workflow.status.startswith('stag')).all())
    being_handled += len(session.query(Workflow).filter(Workflow.status.startswith('assistance')).all())
    max_to_handle = options.maxworkflows
    allowed_to_handle = max(0,max_to_handle - being_handled)
    wf_buffer = 5
    if allowed_to_handle<=wf_buffer: ## buffer for having several wf per transfer
        print "Not allowed to run more than",max_to_handle,"at a time. Currently",being_handled,"and",wf_buffer,"buffer"
    else:
        print being_handled,"already being handled",max_to_handle,"max allowed,",allowed_to_handle,"remaining","and",wf_buffer,"buffer"
    print "... done"

    all_transfers=defaultdict(list)
    workflow_dependencies = defaultdict(set) ## list of wf.id per input dataset
    wfs_and_wfh=[]
    print "getting all wf to consider ..."
    cache = getWorkflows(url, 'assignment-approved', details=True)
    for wfo in session.query(Workflow).filter(Workflow.status=='considered').all():
        if specific and not specific in wfo.name: continue
        cache_r =filter(lambda d:d['RequestName']==wfo.name, cache)
        if len(cache_r):
            wfs_and_wfh.append( (wfo, workflowInfo( url, wfo.name, spec=False, request = cache_r[0]) ) )
        else:
            wfs_and_wfh.append( (wfo, workflowInfo( url, wfo.name, spec=False) ) )
    print "... done"

    input_sizes = {}
    ## list the size of those in transfer already
    in_transfer_priority=0
    min_transfer_priority=100000000
    print "getting all wf in staging ..."
    for wfo in session.query(Workflow).filter(Workflow.status=='staging').all():
        wfh = workflowInfo( url, wfo.name, spec=False)
        (_,primary,_,_) = wfh.getIO()
        for prim in primary: 
            input_sizes[prim] = dss.get( prim )
        in_transfer_priority = max(in_transfer_priority, int(wfh.request['RequestPriority']))
        min_transfer_priority = min(min_transfer_priority, int(wfh.request['RequestPriority']))
    print "... done"
    print "Max priority in transfer already",in_transfer_priority
    print "Min priority in transfer already",min_transfer_priority
    in_transfer_already = sum(input_sizes.values())


    #sort by priority higher first
    wfs_and_wfh.sort(cmp = lambda i,j : cmp(int(i[1].request['RequestPriority']),int(j[1].request['RequestPriority']) ), reverse=True)


    ## list the size of all inputs
    print "getting all input sizes ..."
    for (wfo,wfh) in wfs_and_wfh:
        (_,primary,_,_) = wfh.getIO()
        for prim in primary:
            input_sizes[prim] = dss.get( prim )
    print "... done"

    grand_total =  sum(input_sizes.values()) 
    to_transfer = grand_total  - in_transfer_already
    grand_transfer_limit = options.maxtransfer 
    transfer_limit = grand_transfer_limit - in_transfer_already
    print "%15.4f GB already being transfered"%in_transfer_already
    print "%15.4f GB is the current requested transfer load"%to_transfer
    print "%15.4f GB is the global transfer limit"%grand_transfer_limit
    print "%15.4f GB is the available limit"%transfer_limit

    # the max priority value per dataset.
    max_priority = defaultdict(int)
    needs_transfer=0 ## so that we can count'em
    passing_along = 0
    transfer_sizes={}
    went_over_budget=False
    for (wfo,wfh) in wfs_and_wfh:
        print wfh.request['RequestPriority']
        print wfo.name,"to be transfered"
        #wfh = workflowInfo( url, wfo.name)

        (_,primary,_,_) = wfh.getIO()
        this_load=sum([input_sizes[prim] for prim in primary])
        if ( this_load and (sum(transfer_sizes.values())+this_load > transfer_limit or went_over_budget ) ):
            if went_over_budget:
                print "Transfer has gone over bubget."
            else:
                print "Transfer will go over bubget."
            print "%15.4f GB this load"%this_load
            print "%15.4f GB already this round"%sum(transfer_sizes.values())
            print "%15.4f GB is the available limit"%transfer_limit
            went_over_budget=True
            if int(wfh.request['RequestPriority']) >= in_transfer_priority and min_transfer_priority!=in_transfer_priority:
                print "Higher priority sample",wfh.request['RequestPriority'],">=",in_transfer_priority,"go-on"
            else:
                if not options.go: 
                    print min_transfer_priority,"minimum priority",wfh.request['RequestPriority'],"<",in_transfer_priority,"stop"
                    continue


        ## throtlle by campaign go
        if not CI.go( wfh.request['Campaign'] ):
            print "No go for",wfh.request['Campaign']
            if not options.go: continue

        ## check if the batch is announced
        announced=False
        is_real=False
        for b in mcm.getA('batches',query='contains=%s'% wfo.name):
            is_real = True
            if b['status']=='announced': 
                announced=True 
                break

        if not announced:
            print wfo.name,"does not look announced."# skipping?, rejecting?, reporting?"
            
        if not is_real:
            print wfo.name,"does not appear to be genuine."
            ## prevent any duplication. if the wf is not mentioned in any batch, regardless of status
            continue

        ## check on a grace period
        injection_time = time.mktime(time.strptime('.'.join(map(str,wfh.request['RequestDate'])),"%Y.%m.%d.%H.%M.%S")) / (60.*60.)
        now = time.mktime(time.gmtime()) / (60.*60.)
        if float(now - injection_time) < 4.:
            if not options.go and not announced: 
                print "It is too soon to start transfer: %3.2fH remaining"%(now - injection_time)
                continue


        passing_along += 1
        if passing_along >= allowed_to_handle:
            print "Not allowed to pass more than",max_to_handle,"at a time. Currently",being_handled,"handled, and adding",passing_along
            break

        (lheinput,primary,parent,secondary) = wfh.getIO()
        if options and options.tosites:
            sites_allowed = options.tosites.split(',')
        else:
            sites_allowed = getSiteWhiteList( (lheinput,primary,parent,secondary) )

        if 'SiteWhitelist' in CI.parameters(wfh.request['Campaign']):
            sites_allowed = CI.parameters(wfh.request['Campaign'])['SiteWhitelist']

        blocks = []
        if 'BlockWhitelist' in wfh.request and wfh.request['BlockWhitelist']:
            blocks = wfh.request['BlockWhitelist']

        can_go = True
        staging=False
        if primary:
            if talk:
                print wfo.name,'reads',', '.join(primary),'in primary'
            ## chope the primary dataset 
            for prim in primary:
                max_priority[prim] = max(max_priority[prim],int(wfh.request['RequestPriority']))
                sites_really_allowed = [site for site in sites_allowed if not any([osite.startswith(site) for osite in SI.sites_veto_transfer])]
                print "Sites allowed minus the vetoed transfer"
                print sites_really_allowed
                copies_needed = int(0.35*len(sites_really_allowed))+1 ## should just go for a fixed number based if the white list grows that big
                print "Would make",copies_needed,"copies"
                if options.maxcopy>0:
                    copies_needed = min(options.maxcopy,copies_needed)

                ## remove the sites that do not want transfers                
                print "need",copies_needed
                workflow_dependencies[prim].add( wfo.id )
                presence = getDatasetPresence( url, prim )
                prim_location = [site for site,pres in presence.items() if pres[0]==True]
                if len(prim_location) >= copies_needed:
                    print "The output is all fully in place at",len(prim_location),"sites"
                    continue
                # reduce the number of copies required by existing full copies
                copies_needed = max(0,copies_needed - len(prim_location))
                print "now need",copies_needed
                subscriptions = listSubscriptions( url , prim )
                prim_destination = list(set([site for (site,(tid,decision)) in subscriptions.items() if decision and not any([site.endswith(veto) for veto in ['MSS','Export','Buffer']])]))
                ## need to reject from that list the ones with a full copy already: i.e the transfer corresponds to the copy in place
                prim_destination = [site for site in prim_destination if not site in prim_location]
                ## add transfer dependencies
                latching_on_transfers =  list(set([ tid for (site,(tid,decision)) in subscriptions.items() if decision and site in prim_destination and not any([site.endswith(veto) for veto in ['MSS','Export','Buffer']])]))
                print latching_on_transfers
                for latching in latching_on_transfers:
                    tfo = session.query(Transfer).filter(Transfer.phedexid == latching).first()
                    if not tfo:
                        tfo = Transfer( phedexid = latching)
                        tfo.workflows_id = []
                        session.add(tfo)
                    if not wfo.id in tfo.workflows_id:
                        print "adding",wfo.id,"to",tfo.id,"with phedexid",latching
                        l = copy.deepcopy( tfo.workflows_id )
                        l.append( wfo.id )
                        tfo.workflows_id = l
                    if not options.test:
                        session.commit()
                    else:
                        session.flush() ## regardless of commit later on, we need to let the next wf feeding on this transfer to see it in query
                    can_go = False
                    transfer_sizes[prim] = input_sizes[prim]
                    staging = True

                # reduce the number of copies required by the on-going full transfer : how do we bootstrap on waiting for them ??
                copies_needed = max(0,copies_needed - len(prim_destination))
                print "then need",copies_needed
                if copies_needed == 0:
                    print "The output is either fully in place or getting in full somewhere with",latching_on_transfers
                    can_go = True
                    continue
                prim_to_distribute = [site for site in sites_allowed if not any([osite.startswith(site) for osite in prim_location])]
                prim_to_distribute = [site for site in prim_to_distribute if not any([osite.startswith(site) for osite in prim_destination])]
                ## take out the ones that cannot receive transfers
                prim_to_distribute = [site for site in prim_to_distribute if not any([osite.startswith(site) for osite in SI.sites_veto_transfer])]
                if len(prim_to_distribute)>0: ## maybe that a parameter we can play with to limit the 
                    if not options or options.chop:
                        spreading = distributeToSites( getDatasetChops(prim), prim_to_distribute, n_copies = copies_needed, weights=SI.cpu_pledges)
                    else:
                        spreading = {} 
                        for site in prim_to_distribute: spreading[site]=[prim]
                    can_go = False
                    transfer_sizes[prim] = input_sizes[prim]
                    for (site,items) in spreading.items():
                        all_transfers[site].extend( items )




        if secondary:
            if talk:
                print wfo.name,'reads',', '.join(secondary),'in secondary'
            for sec in secondary:
                workflow_dependencies[sec].add( wfo.id )
                presence = getDatasetPresence( url, sec )
                sec_location = [site for site,pres in presence.items() if pres[1]>90.] ## more than 90% of the minbias at sites
                subscriptions = listSubscriptions( url ,sec )
                sec_destination = [site for site in subscriptions] 
                sec_to_distribute = [site for site in sites_allowed if not any([osite.startswith(site) for osite in sec_location])]
                sec_to_distribute = [site for site in sec_to_distribute if not any([osite.startswith(site) for osite in sec_destination])]
                sec_to_distribute = [site for site in sec_to_distribute if not  any([osite.startswith(site) for osite in SI.sites_veto_transfer])]
                if len( sec_to_distribute )>0:
                    for site in sec_to_distribute:
                        all_transfers[site].append( sec )
                        can_go = False
        
        ## is that possible to do something more
        if can_go:
            ## no explicit transfer required this time
            if staging:
                ## but using existing ones
                print wfo.name,"latches on existing transfers, and nothing else"
                wfo.status = 'staging'
            else:
                print wfo.name,"should just be assigned NOW to",sites_allowed
                wfo.status = 'staged'
            print "setting status to",wfo.status
            session.commit()
            continue
        else:
            ## there is an explicit transfer required
            if staging:
                ## and also using an existing one
                print wfo.name,"latches on existing transfers"
                if not options.test:
                    wfo.status = 'staging'
                    print "setting status to",wfo.status
                    session.commit()
            print wfo.name,"needs a transfer"
            needs_transfer+=1

    #print json.dumps(all_transfers)
    fake_id=-1
    wf_id_in_prestaging=set()

    for (site,items_to_transfer) in all_transfers.iteritems():
        items_to_transfer = list(set(items_to_transfer))
        ## convert to storage element
        site_se = SI.CE_to_SE(site)

        ## site that do not want input datasets
        if site in SI.sites_veto_transfer: 
            print site,"does not want transfers"
            continue

        ## throttle the transfer size to T2s ? we'd be screwed by a noPU sample properly configured.

        ## massage a bit the items
        blocks = [it for it in items_to_transfer if '#' in it]
        datasets = [it for it in items_to_transfer if not '#' in it]

        if execute:
            print "Making a replica to",site,"(CE)",site_se,"(SE) for"
        else:
            print "Would make a replica to",site,"(CE)",site_se,"(SE) for"

        print "\t",len(blocks),"blocks"
        ## remove blocks if full dataset is send out
        blocks = [block for block in blocks if not block.split('#')[0] in datasets]
        print "\t",len(blocks),"needed blocks for",list(set([block.split('#')[0] for block in blocks]))
        print "\t",len(datasets),"datasets"
        print "\t",datasets
        items_to_transfer = blocks + datasets

        ## operate the transfer
        if options and options.stop:
            ## ask to move-on
            answer = raw_input('Continue with that ?')
            if not answer.lower() in ['y','yes','go']:
                continue

        if execute:
            result = makeReplicaRequest(url, site_se, items_to_transfer, 'prestaging', priority='normal')
            ## make use of max_priority dataset:priority to set the subscriptions priority
            """
            ## does not function
            once = True
            for item in items_to_transfer:
                bds = item.split('#')[0]
                if max_priority[bds] >= 90000:
                    if once:
                        w=10
                        print "waiting",w,"s before raising priority"
                        time.sleep(w)
                        once=False
                    ## raise it to high priority
                    print item,"subscription priority raised to high at",site_se
                    #print "This does not work yet properly it seems"
                    print updateSubscription(url, site_se, item, priority='high')
            """
        else:
            #result= {'phedex':{'request_created' : [{'id' : fake_id}]}}
            result= {'phedex':{'request_created' : []}}
            fake_id-=1



        if not result:
            print "ERROR Could not make a replica request for",site,items_to_transfer,"pre-staging"
            continue
        for phedexid in [o['id'] for o in result['phedex']['request_created']]:
            new_transfer = session.query(Transfer).filter(Transfer.phedexid == phedexid).first()
            print phedexid,"transfer created"
            if not new_transfer:
                new_transfer = Transfer( phedexid = phedexid)
                session.add( new_transfer )                
            new_transfer.workflows_id = set()
            for transfering in list(set(map(lambda it : it.split('#')[0], items_to_transfer))):
                new_transfer.workflows_id.update( workflow_dependencies[transfering] )
            new_transfer.workflows_id = list(new_transfer.workflows_id)
            wf_id_in_prestaging.update(new_transfer.workflows_id)
            session.commit()
            ## auto approve it
            if execute:
                approved = approveSubscription(url, phedexid, [site_se])

    for wfid in wf_id_in_prestaging:
        tr_wf = session.query(Workflow).get(wfid)
        if tr_wf and tr_wf.status!='staging':
            if execute:
                tr_wf.status = 'staging'
                if talk:
                    print "setting",tr_wf.name,"to staging"
        session.commit()

if __name__=="__main__":
    url = 'cmsweb.cern.ch'

    parser = optparse.OptionParser()  
    parser.add_option('-t','--test',help="Perform the test and display information",default=False,action='store_true')
    parser.add_option('-s','--stop',help="Stop ask and go",default=False,action='store_true')
    parser.add_option('-n','--nochop',help='Do no chop the input to the possible sites',default=True,dest='chop',action='store_false')
    parser.add_option('--tosites',help='Provide a coma separated list of sites to transfer input to',default=None)
    parser.add_option('--go',help="Overrides no-go from campaign, announced, or grace period",default=False,action='store_true')
    parser.add_option('--maxtransfer',help="The limit in GB of the total size of input that can be transfered", default=80000,type=int)
    parser.add_option('--maxworkflows',help="The limit on the number of workflow we want to keep in the system at a time",default=150,type=int)
    parser.add_option('--maxcopy',help="Specify the number of copies of the input we need", default=3,type=int)
    (options,args) = parser.parse_args()

    spec=None
    if len(args):
        spec = args[0]

    transferor(url,spec,options=options)

    htmlor()
