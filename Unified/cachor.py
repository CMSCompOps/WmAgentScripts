#!/usr/bin/env python
from assignSession import *
from utils import checkTransferStatus, moduleLock, sendLog, transferStatuses
import json
import random
import sys

url = 'cmsweb.cern.ch'

def cachor(spec=None):
    mlock = moduleLock(silent=True)
    if mlock(): 
        print "currently running"
        return
    TS = transferStatuses()
    print sorted(TS.all()),"cached transfers"
    ## pop all that are now in inactive
    for phedexid in TS.all():
        transfers = session.query(TransferImp).filter(TransferImp.phedexid==int(phedexid)).filter(TransferImp.active==True).all()
        if not transfers:
            print phedexid,"does not look relevant to be in cache anymore. poping"
            TS.pop( phedexid )


    all_transfers = set()
    for imp in session.query(TransferImp).filter(TransferImp.active==True).all():
        all_transfers.add( imp.phedexid ) 
    all_transfers = list(all_transfers)

    random.shuffle( all_transfers )

    existing = map(int,TS.all())
    new = (set(all_transfers) - set(existing))

    print len(new),"transfers not look out at all, will do those first",new
    if spec: new = [int(spec)]
    new = list(new)
    random.shuffle( new )
    
    #for transfer in all_transfers:
    for phedexid in all_transfers:    
        #print phedexid
        if new and phedexid!=new[0]: continue
        print "running the check on",phedexid
        new_check = checkTransferStatus(url, phedexid, nocollapse=True)
        if new_check : 
            print json.dumps( new_check ,indent=2)
            TS.add( phedexid, new_check)
        else:
            print "withouth an update, we are in some trouble."
            sendLog('cachor','Failed transfer status check on %s'% phedexid, level='critical')
        #do only one
        break

if __name__ == "__main__":
    spec = sys.argv[1] if len(sys.argv)>1 else None
    cachor(spec)
