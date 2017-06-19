#!/usr/bin/env python
from assignSession import *
from McMClient import McMClient
from utils import workflowInfo, getWorkflowById
import reqMgrClient
from utils import componentInfo, setDatasetStatus, sendLog
from collections import defaultdict
import time

def invalidator(url, invalid_status='INVALID'):
    use_mcm = True
    up = componentInfo(mcm=use_mcm)
    if not up.check(): return
    mcm = McMClient(dev=False)

    invalids = mcm.getA('invalidations',query='status=announced')
    if not invalids: return

    print len(invalids),"Object to be invalidated"
    text_to_batch = defaultdict(str)
    text_to_request = defaultdict(str)
    for invalid in invalids:
        acknowledge= False
        pid = invalid['prepid']
        batch_lookup = invalid['prepid']
        text = ""
        if invalid['type'] == 'request':
            wfn = invalid['object']
            print "need to invalidate the workflow",wfn
            wfo = session.query(Workflow).filter(Workflow.name == wfn).first()
            if wfo:
                ## set forget of that thing (although checkor will recover from it)
                print "setting the status of",wfo.status,"to forget"
                wfo.status = 'forget'
                session.commit()
            else:
                ## do not go on like this, do not acknoledge it
                print wfn,"is set to be rejected, but we do not know about it yet"
                #continue
            wfi = workflowInfo(url, wfn)
            success = "not rejected"
            ## to do, we should find a way to reject the workflow and any related acdc
            success = reqMgrClient.invalidateWorkflow(url, wfn, current_status = wfi.request['RequestStatus'])
            ## need to find the whole familly and reject the whole gang
            familly = getWorkflowById( url, wfi.request['PrepID'] , details=True)
            for fwl in familly:
                ## take out all acdc
                if fwl['RequestDate'] < wfi.request['RequestDate']:continue
                if fwl['RequestType']!='Resubmission': continue
                print "rejecting",fwl['RequestName']
                success = reqMgrClient.invalidateWorkflow(url, fwl['RequestName'], current_status=fwl['RequestStatus'])
                print success
            wfi.sendLog('invalidator',"rejection is performed from McM invalidations request")
            acknowledge= True
            text = "The workflow %s (%s) was rejected due to invalidation in McM" % ( wfn, pid )
            batch_lookup = wfn ##so that the batch id is taken as the one containing the workflow name
        elif invalid['type'] == 'dataset':
            dataset = invalid['object']

            if '?' in dataset: continue
            if 'None' in dataset: continue
            if 'None-' in dataset: continue
            if 'FAKE-' in dataset: continue

            print "setting",dataset,"to",invalid_status
            success = setDatasetStatus(dataset , invalid_status )
            if success:
                acknowledge= True
                text = "The dataset %s (%s) was set INVALID due to invalidation in McM" % ( dataset, pid )
            else:
                print "invalidation of",dataset,"did not go so well"
        else:
            print "\t\t",invalid['type']," type not recognized"

        if acknowledge:
            ## acknoldge invalidation in mcm, provided we can have the api
            print "acknowledgment to mcm"
            mcm.get('/restapi/invalidations/acknowledge/%s'%( invalid['_id'] ))
            # prepare the text for batches
            batches = []
            batches.extend(mcm.getA('batches',query='contains=%s'%batch_lookup))
            batches = filter(lambda b : b['status'] in ['announced','done','reset'], batches)
            if len(batches):
                bid = batches[-1]['prepid']
                print "batch nofication to",bid
                text_to_batch[bid] += text+"\n\n"
            # prepare the text for requests
            text_to_request[pid] += text+"\n\n"

    for bid,text in text_to_batch.items():    
        if not text: continue
        text += '\n This is an automated message'
        mcm.put('/restapi/batches/notify',{ "notes" : text, "prepid" : bid})
        pass
    for pid,text in text_to_request.items():
        if not text: continue
        text += '\n This is an automated message'
        mcm.put('/restapi/requests/notify',{ "message" : text, "prepids" : [pid]})

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    invalidator(url)
