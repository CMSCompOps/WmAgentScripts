from utils import getDatasetBlockAndSite, siteInfo, getWorkflows, workflowInfo, monitor_dir
from collections import defaultdict
import time
import json

url = 'cmsweb.cern.ch'

wfs = getWorkflows(url, 'acquired', details=True)
wfs.extend( getWorkflows(url, 'running-open', details=True) )
wfs.extend( getWorkflows(url, 'running-closed', details=True) )
jobs_for = defaultdict(lambda : defaultdict(int))
wf_for = defaultdict(lambda : defaultdict(set))
agent_for = defaultdict(lambda : defaultdict(set))
s_block_locations = {}
block_locations = defaultdict(lambda : defaultdict(list))
si = siteInfo()  
bad_blocks = defaultdict( set )
unprocessable = set()
for wf in wfs:
    wfi = workflowInfo(url, wf['RequestName'], request=wf)
    sitewhitelist = wfi.request['SiteWhitelist']
    wq = wfi.getWorkQueue()
    wqes = [w[w['type']] for w in wq]
    for wqe in wqes:
        if wqe['Status'] != 'Available': 
            #print  wqe['Status']
            continue
        try:
            pid = filter(lambda w : w.count('-')==2, wqe['RequestName'].split('_'))[0]
            camp = pid.split('-')[1]
        except: 
            camp = None
        if not camp: continue
        #print json.dumps( wqe, indent=2)
        for b in wqe['Inputs']:
            if not '#' in b: continue
            #b is the block
            ds = b.split('#')[0]
            if not ds in block_locations:
                s_block_locations[ds] = getDatasetBlockAndSite(url, ds, complete='y')
                for s in s_block_locations[ds]:
                    for bl in s_block_locations[ds][s]:
                        block_locations[ds][bl].append( s )

            if not b in block_locations[ds]:
                print b,"is not to be found in phedex"
                continue
            block_ce = [si.SE_to_CE(s) for s in block_locations[ds][b]]
            wqe_ce = [s for s in wqe['Inputs'][b]]
            ## the ones in wqe with no true locations
            no_true_location = list(set(wqe_ce)- set(block_ce))
            ## the ones with intersecting locations
            site_with_data_and_listed = list(set(block_ce) & set(wqe_ce))
            if no_true_location:
                print b,"sites in wqe without actually holding the block",",".join( no_true_location )
                bad_blocks[b].update( no_true_location )
            for s in site_with_data_and_listed:
                jobs_for[s][camp] += wqe['Jobs']
                wf_for[s][camp].add( wqe['RequestName'] )
                agent_for[s][camp].add(wqe['ChildQueueUrl'])
            if not site_with_data_and_listed:
                unprocessable.add( b )
    report= "updated %s \n"%time.asctime(time.gmtime())
    print "="*20
    for site in sorted(jobs_for.keys()):
        report += '-'*10+site+'-'*10+'\n'
        for camp in sorted(jobs_for[site].keys()):
            report += "%s @ %s : %d potential jobs\n"%(camp,site,int(jobs_for[site][camp]))
            for wf in sorted(wf_for[site][camp]):
                report +="\t %s \n"%wf
    #print report


report += '\n\n in wqe but not holding a complete block\n\n'
for b in bad_blocks:
    report += "For %s\n"%b
    for s in bad_blocks[b]:
        report += "\t %s is not actually holding it\n"%s

report += "\n\nUnprocessable blocks : i.e no overlap of the site whitelist and the location\n\n"
report += '\n'.join(unprocessable)

open('%s/GQ.json'%monitor_dir,'w').write( json.dumps( jobs_for, indent=2) )
open('%s/GQ.txt'%monitor_dir,'w').write( report )

