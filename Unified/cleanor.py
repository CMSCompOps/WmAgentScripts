#!/usr/bin/env python
from assignSession import *
from utils import getWorkLoad, getDatasetPresence, makeDeleteRequest, getDatasetSize, siteInfo
import json
import time

def cleanor(url, specific=None):
    delete_per_site = {}
    SI = siteInfo()
    for wfo in session.query(Workflow).filter(Workflow.status == 'done').all():
        ## what was in input 
        wl = getWorkLoad(url,  wfo.name )
        if not 'InputDataset' in wl: continue
        dataset = wl['InputDataset']
        print dataset,"in input"
        #print json.dumps(wl, indent=2)
        announced_log = filter(lambda change : change["Status"] in ["closed-out","normal-archived","announced"],wl['RequestTransition'])
        if not announced_log: 
            print "Cannot figure out when",wfo.name,"was finished"
            continue
        now = time.mktime(time.gmtime()) / (60*60*24.)
        then = announced_log[-1]['UpdateTime'] / (60.*60.*24.)
        total_size = getDatasetSize( dataset ) ## in Gb
        if (now-then) <2:
            print "workflow",wfo.name, "finished",now-then,"days ago. Too fresh to clean"
        else:
            print "workflow",wfo.name,"has finished",now-then,"days ago."

        ## find any location it is at
        presence = getDatasetPresence(url, dataset, complete=None)
        ## find all disks
        to_be_cleaned = filter(lambda site : site.startswith('T2') or site.endswith('Disk') ,presence.keys())
        print to_be_cleaned,"for",total_size,"GB"
        ## collect delete request per site
        for site in to_be_cleaned :
            if not site in delete_per_site: delete_per_site[site] = []
            if not dataset in delete_per_site[site]:
                delete_per_site[site].append( (dataset, total_size) )
        


    #print json.dumps(delete_per_site)

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
        result = {'phedex': {'request_created':[]}}
        #result = makeDeleteRequest(url, site, delete_per_site[site], comments="cleanup after production")
        for phedexid in [o['id'] for o in result['phedex']['request_created']]:
            #approved = approveSubscription(url, phedexid, [site])
            pass
    
        
    

        
    """
    for transfer in session.query(Transfer).all():
        if specific and str(specific) != str(transfer.phedexid): continue
        is_clean=True
        for served_id in transfer.workflows_id:
            served = session.query(Workflow).get(served_id)
            if not served.status in ['forget','done']:
                is_clean=False
                break

        if is_clean:
            ## make the deletion request
            print "we should clean data from",transfer.phedexid
        else:
            print "we still need",transfer.phedexid
    """
if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec = None
    if len(sys.argv)>1:
        spec = sys.argv[1]
    cleanor(url,spec)
