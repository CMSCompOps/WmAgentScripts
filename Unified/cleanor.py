#!/usr/bin/env python
from assignSession import *
from utils import getWorkLoad, getDatasetPresence, makeDeleteRequest, getDatasetSize, siteInfo, findCustodialLocation, getWorkflowByInput, campaignInfo, approveSubscription
from utils import lockInfo, duplicateLock
import json
import time
import random

def cleanor(url, specific=None):
    print "Deprecated"
    return

    if duplicateLock() : return 

    delete_per_site = {}
    do_not_autoapprove = []#'T2_FR_CCIN2P3']
    SI = siteInfo()
    CI = campaignInfo()
    LI = lockInfo()

    counts=0
    for wfo in session.query(Workflow).filter(Workflow.status == 'done').all():
        keep_a_copy = False
        if specific and not specific in wfo.name: continue
        ## what was in input 
        wl = getWorkLoad(url,  wfo.name )

        if 'Campaign' in wl and wl['Campaign'] in CI.campaigns and 'clean-in' in CI.campaigns[wl['Campaign']] and CI.campaigns[wl['Campaign']]['clean-in']==False:
            print "Skipping cleaning on input for campaign",wl['Campaign'], "as per campaign configuration"
            continue

        dataset= 'N/A'
        if 'InputDataset' in wl:
            dataset = wl['InputDataset']

        print dataset,"in input"
        #print json.dumps(wl, indent=2)
        announced_log = filter(lambda change : change["Status"] in ["closed-out","normal-archived","announced"],wl['RequestTransition'])
        if not announced_log: 
            print "Cannot figure out when",wfo.name,"was finished"
            continue
        now = time.mktime(time.gmtime()) / (60*60*24.)
        then = announced_log[-1]['UpdateTime'] / (60.*60.*24.)
        if (now-then) <2:
            print "workflow",wfo.name, "finished",now-then,"days ago. Too fresh to clean"
            continue
        else:
            print "workflow",wfo.name,"has finished",now-then,"days ago."

        if not 'InputDataset' in wl: 
            ## should we set status = clean ? or something even further
            print "passing along",wfo.name,"with no input"
            wfo.status = 'clean'
            session.commit()
            continue

        if 'MinBias' in dataset:
            print "Should not clean anything using",dataset,"setting status further"
            wfo.status = 'clean'
            session.commit()
            continue

        total_size = getDatasetSize( dataset ) ## in Gb        
        #if counts> 20:            break
        counts+=1
        ## find any location it is at
        our_presence = getDatasetPresence(url, dataset, complete=None, group="DataOps")
        also_our_presence = getDatasetPresence(url, dataset, complete=None, group="")

        ## is there a custodial !!!
        custodials = findCustodialLocation(url, dataset)
        if not len(custodials):
            print dataset,"has no custodial site yet, excluding from cleaning"
            continue

        ## find out whether it is still in use
        using_the_same = getWorkflowByInput(url, dataset, details=True)
        conflict=False
        for other in using_the_same:
            if other['RequestName'] == wfo.name: continue
            if other['RequestType'] == 'Resubmission': continue
            if not other['RequestStatus'] in ['announced','normal-archived','aborted','rejected','aborted-archived','aborted-completed','rejected-archived','closed-out','None',None,'new']:
                print other['RequestName'],'is in status',other['RequestStatus'],'preventing from cleaning',dataset
                conflict=True
                break
            if 'Campaign' in other and other['Campaign'] in CI.campaigns and 'clean-in' in CI.campaigns[other['Campaign']] and CI.campaigns[other['Campaign']]['clean-in']==False:
                print other['RequestName'],'is in campaign',other['Campaign']
                conflict = True
                break
        if conflict: continue
        print "other statuses:",[other['RequestStatus'] for other in using_the_same if other['RequestName'] != wfo.name]


        ## find all disks
        to_be_cleaned = filter(lambda site : site.startswith('T2') or site.endswith('Disk') ,our_presence.keys())
        to_be_cleaned.extend( filter(lambda site : site.startswith('T2') or site.endswith('Disk') ,also_our_presence.keys()))
        print to_be_cleaned,"for",total_size,"GB"

        anaops_presence = getDatasetPresence(url, dataset, complete=None, group="AnalysisOps")
        own_by_anaops = anaops_presence.keys()
        print "Own by analysis ops and vetoing"
        print own_by_anaops
        ## need to black list the sites where there is a copy of analysis ops
        to_be_cleaned = [site for site in to_be_cleaned if not site in own_by_anaops ]

        ## keep one copy out there
        if 'Campaign' in wl and wl['Campaign'] in CI.campaigns and 'keep-one' in CI.campaigns[wl['Campaign']] and CI.campaigns[wl['Campaign']]['keep-one']==True:
            print "Keeping a copy of input for",wl['Campaign']
            keep_a_copy = True
            
        if keep_a_copy:
            keep_at = None
            full_copies = [site for (site,(there,_)) in our_presence.items() if there and site.startswith('T1')]
            full_copies.extend( [site for (site,(there,_)) in also_our_presence.items() if there and site.startswith('T1')] )
            if not full_copies:
                full_copies = [site for (site,(there,_)) in our_presence.items() if there and site.startswith('T2')]
                full_copies.extend( [site for (site,(there,_)) in also_our_presence.items() if there and site.startswith('T2')] )

            if full_copies:
                keep_at = random.choice( full_copies )
                
            if not keep_at:
                print "We are enable to find a place to keep a full copy of",dataset,"skipping"
                continue
            else:
                ## keeping that copy !
                print "Keeping a full copy of",dataset,"at",keep_at,"not setting the status further"
                to_be_cleaned.remove( keep_at )
        else:
            wfo.status = 'clean'

        ## collect delete request per site
        for site in to_be_cleaned :
            if not site in delete_per_site: delete_per_site[site] = []
            if not dataset in [existing[0] for existing in delete_per_site[site]]:
                delete_per_site[site].append( (dataset, total_size) )
        
        session.commit()

    #open('deletes.json','w').write( json.dumps(delete_per_site,indent=2) )

    print json.dumps(delete_per_site, indent=2)
    print "\n\n ------- \n\n"
    ## unroll the deletion per site
    ## maybe find the optimum site/dataset dataset/site to limit the number of ph requests
    for site in delete_per_site:
        dataset_list = [info[0] for info in delete_per_site[site]]
        size_removal = sum([info[1] for info in delete_per_site[site]]) / 1024.
        if site in SI.disk:
            free = SI.disk[site]
            print site,"has",size_removal,"TB of potential cleanup.",free,"TB available."
        else:
            print site,"has",size_removal,"TB of potential cleanup. no info on available."

        print "\t",','.join(dataset_list)
    
    ## make deletion requests
    for site in delete_per_site:
        site_datasets = [info[0] for info in delete_per_site[site]]
        is_tape = any([v in site for v in ['MSS','Export','Buffer'] ])
        #comments="Cleanup input after production. DataOps will take care of approving it."
        #if is_tape:
        #    comments="Cleanup input after production."
        for item in site_datasets:
            LI.release( item, site, 'cleanup of input after production')
        #result = makeDeleteRequest(url ,site , site_datasets, comments=comments)
        #print result
        #for phedexid in [o['id'] for o in result['phedex']['request_created']]:
        #    if not is_tape:
        #        print "auto-approving to",site,"?"
        #        if not site in do_not_autoapprove: approved = approveSubscription(url, phedexid, nodes = [site], comments = 'Production cleaning by data ops, auto-approved')
        #        pass
        
if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec = None
    if len(sys.argv)>1:
        spec = sys.argv[1]
    cleanor(url,spec)
