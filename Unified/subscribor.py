#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, getDatasetBlockAndSite, getWorkLoad, makeReplicaRequest, sendEmail, getDatasetOnGoingDeletion, componentInfo, reqmgr_url, getWorkflows
import json
from collections import defaultdict
import random
import sys

url = reqmgr_url

up = componentInfo(mcm=False, soft=['mcm'])
if not up.check(): sys.exit(0)

status = sys.argv[1]
max_wf = 0

print "Picked status",status

wfs = []
if status == 'wmagent':
    register=['assigned','acquired','running-open','running-closed','force-complete','completed','closed-out']
    for r in register:
        wfs.extend( getWorkflows(url, r) )

elif status.endswith('*'):
    wfs.extend([wfo.name for wfo in  session.query(Workflow).filter(Workflow.status.startswith(status[:-1])).all() ])
else:
    wfs.extend([wfo.name for wfo in  session.query(Workflow).filter(Workflow.status==status).all() ])



if max_wf: wfs = wfs[:max_wf]

random.shuffle( wfs )
all_blocks_at_sites = defaultdict(set)

#done = json.loads(open('myblock_done.json').read())
done = {}

print len(wfs),"to look the output of"

for iw,wfn in enumerate(wfs):
    print "%s/%s:"%(iw,len(wfs)),wfn
    #wfi = workflowInfo(url, wfo.name)
    #outs= wfi.request['OutputDatasets']
    wl = getWorkLoad(url, wfn)
    outs= wl['OutputDatasets']
    for out in outs:
        blocks_at_sites = getDatasetBlockAndSite(url, out, group="")
        deletions = getDatasetOnGoingDeletion(url, out)
        if len(deletions):
            print "\t\tshould not subscribe with on-going deletions",out
            continue
        for site,blocks in blocks_at_sites.items():
            if 'Buffer' in site or 'Export' in site or 'MSS' in site: continue
            all_blocks_at_sites[site].update( blocks )
        print "\t",out
        print "\t\t",len(blocks_at_sites),"sites",sorted(blocks_at_sites.keys()),"with unsubscribed blocks"

if len(all_blocks_at_sites.keys())==0 and len(wfs):
    ## no subscription to be done at this time, let me know
    #sendEmail('no unsubscribed blocks','while catching up %s does not need to be there anymore'%( one_status ))
    pass

print len(all_blocks_at_sites.keys()),"sites to subscribe things at"
for site,blocks in all_blocks_at_sites.items():
    if 'Buffer' in site or 'Export' in site or 'MSS' in site: continue

    if not site in done: done[site] = []
    blocks = [block for block in blocks if not block in done[site]]
    print "Would subscribe",len(blocks),"blocks to",site
    print "\tSubscribe",len(blocks),"blocks to",site    
    done[site].extend( blocks )
    if blocks:
        print makeReplicaRequest(url, site, list(blocks), "Production blocks", priority="low", approve=True,mail=False)
        time.sleep(1)

#open('myblock_done.json','w').write( json.dumps( done, indent=2 ))
