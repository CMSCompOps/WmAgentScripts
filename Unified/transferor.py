#!/usr/bin/env python
from assignSession import *
import reqMgrClient
from McMClient import McMClient
from utils import makeReplicaRequest
from utils import workflowInfo, siteInfo, campaignInfo, userLock
from utils import getDatasetChops, distributeToSites, getDatasetPresence, listSubscriptions, getSiteWhiteList, approveSubscription
import json
from collections import defaultdict
import optparse
import time
from htmlor import htmlor

def transferor(url ,specific = None, talk=True, options=None):
    if userLock('transferor'):   return

    if options and options.test:
        execute = False
    else:
        execute = True

    SI = siteInfo()
    CI = campaignInfo()
    mcm = McMClient(dev=False)

    all_transfers=defaultdict(list)
    workflow_dependencies = defaultdict(set) ## list of wf.id per input dataset
    data_to_wf = {}
    for wfo in session.query(Workflow).filter(Workflow.status=='considered').all():
        if specific and not specific in wfo.name: continue

        print wfo.name,"to be transfered"
        wfh = workflowInfo( url, wfo.name)

        ## throtlle by campaign go
        if not CI.go( wfh.request['Campaign'] ):
            print "No go for",wfh.request['Campaign']
            if not options.go: continue

        ## check if the batch is announced
        announced=False
        for b in mcm.getA('batches',query='contains=%s'% wfo.name):
            if b['status']=='announced': 
                announced=True 
                break

        if not announced:
            print wfo.name,"does not look announced. skipping?, rejecting?, reporting?"
            if not options.go: continue

        ## check on a grace period
        injection_time = time.mktime(time.strptime('.'.join(map(str,wfh.request['RequestDate'])),"%Y.%m.%d.%H.%M.%S")) / (60.*60.)
        now = time.mktime(time.gmtime()) / (60.*60.)
        if float(now - injection_time) < 4.:
            print "It is too soon to inject: %3.2fH remaining"%(now - injection_time)
            if not options.go: continue

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
                copies_needed = int(0.35*len(sites_allowed))+1
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
                latching_on_transfers = list(set([ tid for (site,(tid,decision)) in subscriptions.items() if decision and not any([site.endswith(veto) for veto in ['MSS','Export','Buffer']])]))
                print latching_on_transfers
                for latching in latching_on_transfers:
                    tfo = session.query(Transfer).filter(Transfer.phedexid == latching).first()
                    if not tfo:
                        tfo = Transfer( phedexid = latching)
                        tfo.workflows_id = []
                        if not options.test:
                            session.add(tfo)
                    if wfo.id not in tfo.workflows_id:
                        tfo.workflows_id.append( wfo.id )
                        tfo.workflows_id = copy.deepcopy( tfo.workflows_id )
                        if not options.test:
                            session.commit()
                    staging = True

                # reduce the number of copies required by the on-going full transfer : how do we bootstrap on waiting for them ??
                copies_needed = max(0,copies_needed - len(prim_destination))
                print "then need",copies_needed
                if copies_needed == 0:
                    print "The output is either fully in place or getting in full somewhere with",latching_on_transfers
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
            print wfo.name,"should just be assigned NOW to",sites_allowed
            wfo.status = 'staged'
            session.commit()
            continue
        else:
            if staging:
                print wfo.name,"latches on existing transfers"
                wfo.status = 'staging'
                if not options.test:
                    session.commit()
                continue
            else:
                print wfo.name,"needs a transfer"

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
            result = makeReplicaRequest(url, site_se, items_to_transfer, 'prestaging')
        else:
            #result= {'phedex':{'request_created' : [{'id' : fake_id}]}}
            result= {'phedex':{'request_created' : []}}
            fake_id-=1



        if not result:
            print "ERROR Could not make a replica request for",site,items_to_transfer,"pre-staging"
            continue
        for phedexid in [o['id'] for o in result['phedex']['request_created']]:
            new_transfer = session.query(Transfer).filter(Transfer.phedexid == phedexid).first()
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
    (options,args) = parser.parse_args()

    spec=None
    if len(args):
        spec = args[0]

    transferor(url,spec,options=options)

    htmlor()
