#!/usr/bin/env python

import json
import os
from collections import defaultdict
from utils import lockInfo, siteInfo, getDatasetBlocksFraction, getDatasetChops, distributeToSites, makeReplicaRequest
url = 'cmsweb.cern.ch'


act = False

LI = lockInfo()
SI = siteInfo()

full_spread = defaultdict(set)

dss = set()
to_lock = set()

### should be done in input
prim_to_distribute = [site for site in SI.sites_ready if site.startswith('T1')]
goods = ['T2_CH_CERN','T2_US','T2_DE']
for g in goods:
    prim_to_distribute.extend([site for site in SI.sites_ready if site.startswith(g)])
    
prim_to_distribute = [SI.SE_to_CE(site) for site in [SI.CE_to_SE(site) for site in prim_to_distribute]]
prim_to_distribute = list(set(prim_to_distribute))
    

print "will use these sites as destinations"
print sorted( prim_to_distribute )

copies_needed=2
print "will make",copies_needed,"of all"

read_file = 'legacy-reco-raw_prio.txt'
for l in open(read_file).read().split('\n'):
    if not l: continue
    if l.startswith('#'): continue
    all_ds = filter(lambda w : w.count('/')==3 and w.startswith('/'), l.split())
    dss.update( all_ds )

print sorted(dss)

for ds in dss:
    availability = getDatasetBlocksFraction(url, ds )
    if availability>=1:  continue
    blocks= []
    chops,sizes = getDatasetChops(ds, chop_threshold = 4000, only_blocks=blocks)
    spreading = distributeToSites( chops, prim_to_distribute, n_copies = copies_needed, weights=SI.cpu_pledges, sizes=sizes)
    print json.dumps(spreading, indent=2)
    for site,items in spreading.items():
        full_spread[site].update( items )

    to_lock.add( ds )

addHocLocks = set(json.loads( open('addhoc_lock.json').read()))
addHocLocks.update( to_lock )
if act:
    open('addhoc_lock.json','w').write( json.dumps( sorted(addHocLocks), indent=2 ) )

for l in to_lock:
    if act:
        LI.lock(l, 'add HOC')

for site,items in full_spread.items():
    print site
    se = SI.CE_to_SE(site)
    print se
    print len(items)
    print json.dumps(sorted(items), indent=2)
    if act:
        print makeReplicaRequest(url, se , list(set(items)), 'pre-staging', priority='high', approve=True, mail=False)

