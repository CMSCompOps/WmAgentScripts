from assignSession import *

def cleanor(url, specific=None):

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

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec = None
    if len(sys.argv)>1:
        spec = sys.argv[1]
    cleanor(url,spec)
