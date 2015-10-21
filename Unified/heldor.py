#!/usr/bin/env python  
from assignSession import *
import httplib
import os
from utils import 

def heldor(url, specific=None, options=None):
    
    if specific:
        wfs = session.query(Workflow).filter(Workflow.name == specific).all()
    else:
        wfs = session.query(Workflow).filter(Workflow.status.startswith('held-')).all()

    for wfo in wfs:
        ##
        if not wfo.status.startswith('held-'):
            ## reason can be ggus:115991
            wfo.status = 'held-'+reason+'-'+wfo.status
        else:
            ## verify if the reason for holding is solved
            _,item,next_status = wfo.status.split('-',2)[1]
            source=None
            item_id=None
            if ':' in item:
                source,item_id = item.split(':')

            if source == 'ggus':
                if check_ggus( item_id ):
                    print "the issue is resolved, going back to",next_status
                    wfo.status = next_status
                    session.commit()
