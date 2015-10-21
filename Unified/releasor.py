#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo
import time

def releasor():
    if duplicateLock() : return

    SI = siteInfo()
    CI = campaignInfo()
    LI = lockInfo()

    tiers_no_custodial = ['MINIADOSIM']
    wfs = []
    for fetch in ['done','forget']:
        wfs.extend( session.query(Workflow).filter(Workflow.status==fetch).all() )
    
    
    for wfo in wfs:
        wfi = workflowInfo(url, wfo.name )
        announced_log = filter(lambda change : change["Status"] in ["closed-out","normal-archived","announced"],wfi.request['RequestTransition'])
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

        (_,primaries,_,secondaries) = wfi.getIO()
        outputs = wfi.request['OutputDatasets']
        datasets_to_check = list(primaries)+list(secondaries)+outputs
        for dataset in datasets_to_check:
            (_,_,_,tier) = dataset.split('/')
            ## check custodial if required
            if tier not in tiers_no_custodial:
            ## check not used anymore by anything
            ## unlock output and input everywhere if so
            pass
