from assignSession import *
from utils import workflowInfo, getDatasetBlockAndSite, getWorkLoad, makeReplicaRequest
import json
from collections import defaultdict
import random

url ='cmsweb.cern.ch'

wfs = []
wfs.extend( session.query(Workflow).filter(Workflow.status=='away').all() )
wfs.extend( session.query(Workflow).filter(Workflow.status=='assistance').all() )
wfs.extend( session.query(Workflow).filter(Workflow.status=='close').all() )

### should not be necessary at some point
wfs.extend( session.query(Workflow).filter(Workflow.status=='done').all() )
wfs.extend( session.query(Workflow).filter(Workflow.status=='clean').all() )

random.shuffle( wfs )
all_blocks_at_sites = defaultdict(set)

done = json.loads(open('myblock_done.json').read())

print len(wfs),"to look the output of"
for iw,wfo in enumerate(wfs[:800]):
    print "%s/%s:"%(iw,len(wfs)),wfo.name
    #wfi = workflowInfo(url, wfo.name)
    #outs= wfi.request['OutputDatasets']
    wl = getWorkLoad(url, wfo.name)
    outs= wl['OutputDatasets']
    for out in outs:
        blocks_at_sites = getDatasetBlockAndSite(url, out, group="")
        for site,blocks in blocks_at_sites.items():
            all_blocks_at_sites[site].update( blocks )
        print out
        print "\t",len(blocks_at_sites),"sites",sorted(blocks_at_sites.keys()),"with unsubscribed blocks"

print len(all_blocks_at_sites.keys()),"sites to subscribe things at"
for site,blocks in all_blocks_at_sites.items():
    if 'Buffer' in site or 'Export' in site: continue

    if not site in done:
        done[site] = []
    print "Would subscribe",len(blocks),"blocks to",site
    blocks = [block for block in blocks if not block in done[site]]
    print "\tSubscribe",len(blocks),"blocks to",site    
    done[site].extend( blocks )
    if blocks:
        print makeReplicaRequest(url, site, list(blocks), "Production blocks", priority="low", approve=True,mail=False)
        pass

open('myblock_done.json','w').write( json.dumps( done, indent=2 ))
