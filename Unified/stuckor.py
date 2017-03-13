import json
from collections import defaultdict
from utils import unifiedConfiguration, checkTransferLag, reqmgr_url, monitor_dir, duplicateLock, unified_url, monitor_pub_dir

def stuckor(url = reqmgr_url):
    
    if duplicateLock(): return
    datasets_by_phid = json.loads(open('datasets_by_phid.json').read())
    really_stuck_dataset = set(json.loads(open('really_stuck_dataset.json').read()))

    UC = unifiedConfiguration()

    print "make a report of stuck transfers"
    bad_destinations = defaultdict(set)
    bad_sources = defaultdict(set)
    report = ""
    transfer_timeout = UC.get("transfer_timeout")
    transfer_lowrate = UC.get("transfer_lowrate")
    for phid,datasets in datasets_by_phid.items():
        issues = checkTransferLag( url, phid , datasets=list(datasets) )
        for dataset in issues:
            for block in issues[dataset]:
                for destination in issues[dataset][block]:
                    (block_size,destination_size,delay,rate,dones) = issues[dataset][block][destination]
                    ## count x_Buffer and x_MSS as one source
                    redones=[]
                    for d in dones:
                        if d.endswith('Buffer') or d.endswith('Export'):
                            if d.replace('Buffer','MSS').replace('Export','MSS') in dones: 
                                continue
                            else: 
                                redones.append( d )
                        else:
                            redones.append( d )
                    dones = list(set( redones ))
                    #dones = filter(lambda s : (s.endswith('Buffer') and not s.replace('Buffer','MSS') in dones) or (not s.endswith('Buffer')) , dones)
                    if delay>transfer_timeout and rate<transfer_lowrate:
                        if len(dones)>1:
                            ## its the destination that sucks
                            bad_destinations[destination].add( block )
                        else:
                            dum=[bad_sources[d].add( block ) for d in dones]
                        really_stuck_dataset.add( dataset )
                        print "add",dataset,"to really stuck"
                        report += "%s is not getting to %s, out of %s faster than %f [GB/s] since %f [d]\n"%(block,destination,", ".join(dones), rate, delay)
    print "\n"*2

    ## create tickets right away ?
    report+="\nbad sources "+",".join(bad_sources.keys())+"\n"
    for site,blocks in bad_sources.items():
        report+="\n\n%s:"%site+"\n\t".join(['']+list(blocks))
    report+="\nbad destinations "+",".join(bad_destinations.keys())+"\n"
    for site,blocks in bad_destinations.items():
        report+="\n\n%s:"%site+"\n\t".join(['']+list(blocks))

    print '\n'*2,"Datasets really stuck"
    print '\n'.join( really_stuck_dataset )

    print '\n'*2,"report written at %s/logs/incomplete_transfers.log"%unified_url
    print report

    missing_in_action = json.loads(open('%s/incomplete_transfers.json'%monitor_dir).read())
    stuck_transfers = dict([(k,v) for (k,v) in missing_in_action.items() if k in really_stuck_dataset])
    print '\n'*2,'Stuck dataset transfers'
    print json.dumps(stuck_transfers , indent=2)
    open('%s/stuck_transfers.json'%monitor_pub_dir,'w').write( json.dumps(stuck_transfers , indent=2) )
    open('%s/logs/incomplete_transfers.log'%monitor_dir,'w').write( report )


if __name__ == "__main__":
    stuckor()
