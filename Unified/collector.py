#!/usr/bin/env python
import optparse
from McMClient import McMClient
from collections import defaultdict
from utils import makeReplicaRequest, siteInfo, getDatasetDestinations, getDatasetPresence, unifiedConfiguration, getDatasetChops, distributeToSites, DSS, componentInfo
import itertools
import json 
import random 

def collector(url, specific, options):
    up = componentInfo(mcm=False, soft=['mcm'])
    if not up.check(): return 

    SI = siteInfo()
    dss = DSS()
    #NL = newLockInfo()
    mcm = McMClient(dev=False)
    fetch_in_campaigns = ['RunIISummer15GS']
    mcm_statuses=['new']#,'validation','defined','approved']

    will_be_used = defaultdict(list)
    secondary_used = defaultdict(list)
    for campaign,status in itertools.product( fetch_in_campaigns, mcm_statuses):
        queries=[]
        if campaign:
            print "getting for",campaign
            queries.append('member_of_campaign=%s'%campaign)
        if status:
            print "getting for",status
            queries.append('status=%s'%status)
        rs = mcm.getA('requests', query='&'.join(queries))
        for r in rs:
            #if r['type'] != 'Prod': continue
            dataset = r['input_dataset']
            if dataset:
                #print r['prepid'],dataset
                will_be_used[dataset].append( r )
            pileup = r['pileup_dataset_name']
            if pileup:
                secondary_used['pileup'].append( r )

    all_transfers = defaultdict(list)
    print len(will_be_used),"datasets that can be pre-fetched"
    ## for secondary we really need to have the campaign right
    print len(secondary_used),"pileup will be used"

    datasets = will_be_used.keys()
    if options.limit:
        print "Restricting to randomly picked",options.limit
        random.shuffle( datasets )
        datasets = datasets[:options.limit]
    
    for dataset in datasets:
        print "\tlooking at",dataset
        #presence = getDatasetPresence(url, dataset)#, within_sites=['T2_CH_CERN'])
        ## lock all those, and pre-fecth them
        #NL.lock( dataset )
        ## we could get the reqmgr dictionnary from McM if it was implemented and use standard workflowInfo !!!
        for request in will_be_used[dataset]:
            print "will be used by",request['prepid']
            campaign = request['member_of_campaign']
            ## based on the campaign, pre-fetch a site list
            sites_allowed = SI.sites_T1s + SI.sites_with_goodIO
            if options.spread:
                ## pick up the number of copies from campaign
                copies_needed = 1 ## hard coded for now
            else:
                copies_needed = 1 ## hard coded for now        

            print "Will look for",copies_needed,"of",dataset
            ## figure out where it is and going
            destinations, all_block_names = getDatasetDestinations(url, dataset, within_sites = [SI.CE_to_SE(site) for site in sites_allowed])
            print json.dumps( destinations, indent=2)
            prim_location = [site for (site,info) in destinations.items() if info['completion']==100 and info['data_fraction']==1]
            prim_destination = [site for site in destinations.keys() if not site in prim_location]
            prim_destination = [site for site in prim_destination if not any([osite.startswith(site) for osite in SI.sites_veto_transfer])]
            copies_needed = max(0,copies_needed - len(prim_location))
            copies_being_made = [ sum([info['blocks'].keys().count(block) for site,info in destinations.items() if site in prim_destination]) for block in all_block_names]
            
            prim_to_distribute = [site for site in sites_allowed if not SI.CE_to_SE(site) in prim_location]
            prim_to_distribute = [site for site in prim_to_distribute if not SI.CE_to_SE(site) in prim_destination]
                ## take out the ones that cannot receive transfers
            prim_to_distribute = [site for site in prim_to_distribute if not any([osite.startswith(site) for osite in SI.sites_veto_transfer])]
            copies_needed = max(0,copies_needed - min(copies_being_made))
            spreading = {}
            if copies_needed:
                print "needing",copies_needed 
                chops,sizes = getDatasetChops(dataset, chop_threshold = options.chopsize)
                spreading = distributeToSites( chops, prim_to_distribute, n_copies = copies_needed, weights=SI.cpu_pledges, sizes=sizes)
            else:
                print "no copy needed for",dataset
            for (site,items) in spreading.items():
                all_transfers[site].extend( items )
    
    print "accumulated transfers"
    print json.dumps(all_transfers, indent=2)
    if not options.test:
        sendEmail('dataset to be fetched',
                  'the following datasets and location were figured from mcm up-coming requests\n%s'%( json.dumps(all_transfers, indent=2) ),
                  destination=['srimanob@mail.cern.ch'])
    
    ## now collect and make transfer request
    for (site,items_to_transfer) in all_transfers.iteritems():
        print "Directing at",site
        items_to_transfer = list(set(items_to_transfer))

        site_se = SI.CE_to_SE(site)
        blocks = [it for it in items_to_transfer if '#' in it]
        datasets = [it for it in items_to_transfer if not '#' in it]

        print "\t",len(blocks),"blocks"
        ## remove blocks if full dataset is send out                                                                                                 
        blocks = [block for block in blocks if not block.split('#')[0] in datasets]
        blocks_dataset = list(set([block.split('#')[0] for block in blocks]))
        print "\t",len(blocks),"needed blocks for",blocks_dataset
        print "\t",len(datasets),"datasets"
        print "\t",datasets
        items_to_transfer = blocks + datasets
        total_size = 0
        for dataset in datasets:
            ds_size,_ = dss.get_block_size( dataset )
            total_size += ds_size
        for dataset in blocks_dataset:
            _,bs_size = dss.get_block_size( dataset )
            total_size += sum([ s for b,s in bs_size if b in blocks ])

        print "For a total of",total_size,"[GB]"

        if options.test:
            result= {'phedex':{'request_created' : []}}
        else:
            ##result = makeReplicaRequest(url, site_se, items_to_transfer, 'fetching pre-production', priority='normal', approve=True)
            ## should make sure there is something in it
            pass

            
    
        

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'

    UC = unifiedConfiguration()
    parser = optparse.OptionParser()
    parser.add_option('--test','-t',help='only test and see what is going out',default=True,action='store_true')
    parser.add_option('--spread','-s',help='use the campaign to try and preplace properly the dataset',default=False,action='store_true')
    parser.add_option('-c','--chopsize',help='The threshold for choping input dataset',default=UC.get('chopping_threshold_in_GB'),type=int)
    parser.add_option('--limit','-l', help='limit the amount of dataset to browse for', default=200, type=int)
    (options,args) = parser.parse_args()
    specific = None
    if len(args)!=0:
        specific = args[0]

    collector(url, specific, options)
