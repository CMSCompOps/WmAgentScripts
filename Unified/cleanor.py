from assignSession import *
from utils import getWorkLoad, getDatasetPresence, makeDeleteRequest

def cleanor(url, specific=None):
    delete_per_site = {}
    for wfo in session.query(Workflow).filter(Workflow.status == 'done').all():
        ## what was in input 
        wl = getWorkLoad(url,  wfo.name )
        print wl['InputDataset']
        ## find any location it is at
        presence = getDatasetPresence(url, wl['InputDataset'], complete=None)
        ## find all disks
        to_be_cleaned = filter(lambda site : site.startswith('T2') or site.endswith('Disk') ,presence.keys())
        print to_be_cleaned
        ## collect delete request per site
        for site in to_be_cleaned :
            if not site in delete_per_site: delete_per_site[site] = []
            delete_per_site[site].append( wl['InputDataset'] )


    ## unroll the deletion per site
    ## maybe find the optimum site/dataset dataset/site to limit the number of ph requests
    for site in delete_per_site:
        #results = makeDeleteRequest(url, site, delete_per_site[site], comments="cleanup after production")
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
