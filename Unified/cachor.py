from assignSession import *
from utils import checkTransferStatus, duplicateLock
import json
import random
import sys

url = 'cmsweb.cern.ch'

def cachor(spec=None):
    if duplicateLock('cachor', silent=True): 
        print "currently running"
        return
    try:
        all_checks = json.loads(open('cached_transfer_statuses.json').read())
    except:
        all_checks = {}

    all_transfers = [transfer for transfer in session.query(Transfer).filter(Transfer.phedexid>0).all()]
    random.shuffle( all_transfers )

    existing = all_checks.keys() ## strng keys
    new = (set([str(transfer.phedexid) for transfer in all_transfers]) - set(existing))

    print len(new),"transfers not look out at all, will do those first"
    if spec: new = [spec]

    for transfer in all_transfers:
        if new and str(transfer.phedexid)!=sorted(new)[0]: continue
        print "running the check on",transfer.phedexid
        new_check = checkTransferStatus(url, transfer.phedexid, nocollapse=True)
        if new_check : 
            all_checks[str(transfer.phedexid)] = new_check
        else:
            print "withouth an update, we are in deep shit"
        #do only one
        break

    for pid in sorted(all_checks.keys()):
        if not all_checks[pid]:
            all_checks.pop(pid)
    open('cached_transfer_statuses.json','w').write(json.dumps( all_checks , indent=2))
    
if __name__ == "__main__":
    spec = sys.argv[1] if len(sys.argv)>1 else None
    cachor(spec)
