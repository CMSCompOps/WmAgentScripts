#!/usr/bin/env python
from assignSession import *
from utils import getWorkLoad, getDatasetPresence, makeDeleteRequest, getDatasetSize, siteInfo, findCustodialLocation, getWorkflowByInput
import json
import time

def cleanor(url, specific=None):
    delete_per_site = {}
    SI = siteInfo()
    counts=0
    for wfo in session.query(Workflow).filter(Workflow.status == 'done').all():
        if specific and not specific in wfo.name: continue
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
            continue
        else:
            print "workflow",wfo.name,"has finished",now-then,"days ago."
        
        #if counts> 20:            break
        counts+=1
        ## find any location it is at
        our_presence = getDatasetPresence(url, dataset, complete=None, group="DataOps")

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
            if not other['RequestStatus'] in ['announced','normal-archived','aborted','rejected','aborted-archived','rejected-archived','closed-out','None',None]:
            #if other['RequestStatus'] in ['running-open','running-closed','new','assignment-approved','acquired','assigned','completed']:
                print other['RequestName'],'is in status',other['RequestStatus'],'preventing from cleaning',dataset
                conflict=True
                break
        if conflict: continue
        print "other statuses:",[other['RequestStatus'] for other in using_the_same if other['RequestName'] != wfo.name]

        ## find all disks
        to_be_cleaned = filter(lambda site : site.startswith('T2') or site.endswith('Disk') ,our_presence.keys())

        print to_be_cleaned,"for",total_size,"GB"

        anaops_presence = getDatasetPresence(url, dataset, complete=None, group="AnalysisOps")
        own_by_anaops = anaops_presence.keys()
        print own_by_anaops


        ## collect delete request per site
        for site in to_be_cleaned :
            if not site in delete_per_site: delete_per_site[site] = []
            if not dataset in [existing[0] for existing in delete_per_site[site]]:
                delete_per_site[site].append( (dataset, total_size) )
        
        wfo.status = 'clean'
        session.commit()

    open('deletes.json','w').write( json.dumps(delete_per_site,indent=2) )

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
    
    ## make a one for all deletion request to save on phedex id
    sites = set()
    datasets = set()
    for site in delete_per_site:
        datasets.update([info[0] for info in delete_per_site[site]])
        sites.add(site)
    
    sites = map(str, sites)
    datasets = map(str, datasets)
    result = makeDeleteRequest(url ,sites ,datasets, comments="cleanup after production") 
    for phedexid in [o['id'] for o in result['phedex']['request_created']]:
        for site in sites:
            #approved = approveSubscription(url, phedexid, [site])
            pass

        
if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec = None
    if len(sys.argv)>1:
        spec = sys.argv[1]
    cleanor(url,spec)
