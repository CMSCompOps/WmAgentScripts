from utils import getDatasetBlockAndSite, siteInfo, getWorkflows, workflowInfo, monitor_dir, sendLog, sendEmail, makeReplicaRequest
from collections import defaultdict
import time
import json
import sys
import random
from assignSession import *
spec=None
if len(sys.argv) >1:
    spec = sys.argv[1]

url = 'cmsweb.cern.ch'

wfs = getWorkflows(url, 'acquired', details=True)
wfs.extend( getWorkflows(url, 'running-open', details=True) )
wfs.extend( getWorkflows(url, 'running-closed', details=True) )
jobs_for = defaultdict(lambda : defaultdict(int))
wf_for = defaultdict(lambda : defaultdict(set))
agent_for = defaultdict(lambda : defaultdict(set))
s_block_locations = {}
block_locations = defaultdict(lambda : defaultdict(list))
wfs_no_location_in_GQ = defaultdict(list)
si = siteInfo()  
#bad_blocks = defaultdict( set )
unprocessable = set()

try:
    replaced = json.loads(open('replaced_blocks.json').read())
except:
    replaced = []

not_runable_acdc=set()
for wf in wfs:
    if spec and not spec in wf['RequestName']: continue

    wfi = workflowInfo(url, wf['RequestName'], request=wf)
    sitewhitelist = wfi.request['SiteWhitelist']
    wqs = wfi.getWorkQueue()
    
    ## skip wf that unified does not know about, leaves acdc
    wfo = session.query(Workflow).filter(Workflow.name == wf['RequestName']).first()
    if not (wfo or wf['RequestType']=='Resubmission'): 
        print "not knonw or not acdc"
        continue

    #wqes = [w[w['type']] for w in wqs]
    print wf['RequestName'],len(wqs),"elements"

    for wq in wqs:
        wqe = wq[wq['type']]
        if not wqe['Status'] in ['Available', 'Acquired']:#, 'Running']: 
            #print  wqe['Status']
            continue
        camp =wfi.getCampaigns()[0]
        if not camp: continue
        #print json.dumps( wqe, indent=2)
        if wqe['NoInputUpdate']: 
            ## input is remote: one day we'd like to move it to disk automatically, but not now
            print "input on aaa for",wfi.request['RequestName']
            continue
        ## where the wf is set to be run at and site ready
        swl = [si.CE_to_SE(s) for s in wqe['SiteWhitelist'] if s in si.sites_ready]

        if not swl:
            print "There is no site at which the workflow can run"
            continue

        not_processable = set()
        for b in wqe['Inputs']:
            if not '#' in b: 
                ## this would be an ACDC document input
                acdc_location = [si.CE_to_SE(s) for s in wqe['Inputs'][b]]
                can_run_at = list(set(acdc_location)&set(swl))
                if not can_run_at:
                    print b,"is at",acdc_location,"and wf set to run from",swl
                    not_runable_acdc.add( wf['RequestName'] )
                    not_processable.add( b )
                continue
            #b is the block
            ds = b.split('#')[0]
            if not ds in block_locations:
                s_block_locations[ds] = getDatasetBlockAndSite(url, ds, complete='y', vetoes=[])
                for s in s_block_locations[ds]:
                    for bl in s_block_locations[ds][s]:
                        block_locations[ds][bl].append( s )

            if not b in block_locations[ds]:
                print b,"is not to be found in phedex, needed be",wfi.request['RequestName']
                ## should send a critical log
                continue

            #block_ce = [si.SE_to_CE(s) for s in block_locations[ds][b]]
            #wqe_ce = [s for s in wqe['Inputs'][b]]
            ## true location of the data
            block_se = block_locations[ds][b] 
            ## what the global queue thinks about the block location
            wqe_se = [si.CE_to_SE(s) for s in wqe['Inputs'][b]]


            ## the ones in wqe with no true locations
            #no_true_location = list(set(wqe_se)- set(block_se))
            #if no_true_location:
            #    ## this is minor
            #    print b,"sites in wqe without actually holding the block",",".join( no_true_location )
            #    bad_blocks[b].update( no_true_location )

            ## the ones with intersecting locations
            site_with_data_and_listed = list(set(block_se) & set(swl))
            for s in site_with_data_and_listed:
                jobs_for[s][camp] += wqe['Jobs']
                wf_for[s][camp].add( wqe['RequestName']+' '+str(wqe['Priority']) )
                agent_for[s][camp].add(wqe['ChildQueueUrl'])

            ## in case there is an element for which the intersection of actual location and site listed is empty
            if not site_with_data_and_listed:
                wfs_no_location_in_GQ[wqe['RequestName']].append( (wq['_id'], b , swl) )
                unprocessable.add( b )
                not_processable.add( b )
                print "Block",b,"is at",",".join(sorted(block_se)),"while asked to run at",",".join(sorted(swl))
        if not_processable:
            wfi.sendLog('GQ','The following blocks need to be put back on disk \n%s'%(','.join( not_processable )))

report = "updated %s \n"%time.asctime(time.gmtime())
print "="*20
for site in sorted(jobs_for.keys()):
    report += '-'*10+site+'-'*10+'\n'
    for camp in sorted(jobs_for[site].keys()):
        report += "%s @ %s : %d potential jobs\n"%(camp,site,int(jobs_for[site][camp]))
        for wf in sorted(wf_for[site][camp]):
            report +="\t %s \n"%wf
    #print report

if not_runable_acdc:
    sendLog('GQ','These %s ACDC cannot run \n%s'%( len(not_runable_acdc),
                                                   '\n'.join(not_runable_acdc)
                                                   ),level='critical')
    

#report += '\n\n in wqe but not holding a complete block\n\n'
#for b in bad_blocks:
#    report += "For %s\n"%b
#    for s in bad_blocks[b]:
#        report += "\t %s is not actually holding it\n"%s

unproc = "\n\nUnprocessable blocks : i.e no overlap of the site whitelist and the location\n\n"
unproc += '\n'.join(sorted(unprocessable))
report += unproc
if unprocessable:
    sendLog('GQ',unproc, level='critical')
    #sendEmail('unprocessable blocks',"Sending a notification of this new feature until this gets understood. transfering block automatically back to  processing location. \n"+unproc)

try_me = defaultdict(list)
for wf in wfs_no_location_in_GQ:
    print wf,"has problematic blocks"
    for (el,b, swl) in wfs_no_location_in_GQ[wf]:
        print "\t%s in element %s"%( b ,el )
        sswl = [si.SE_to_CE(s) for s in swl]
        
        if not sswl: 
            print "\tno site to replicate to"
            continue

        print "\tshould be replicated to %s"%(','.join(sswl))
        wfi = workflowInfo(url, wf)
        copies_wanted,cpuh = wfi.getNCopies()
        
        go_to = si.CE_to_SE(si.pick_CE(sswl))
        #go_to = random.choice( swl )

        try_me[go_to].append( b )
        ## pick a site that should host this !
        

for site,blocks in try_me.items():
    blocks = list(set(blocks)-set(replaced))
    if False:
        if blocks:
            sendLog('GQ','replacing %s at %s'%( '\n,'.join(blocks), site),level='warning')
            result = makeReplicaRequest(url, site, blocks, 'item relocation', priority='normal', approve=True, mail=False)
            replaced = list(set(blocks+replaced))
    else:
        sendLog('GQ','tempting to put %s at %s'%( '\n,'.join(blocks), site),level='critical')

open('%s/GQ.json'%monitor_dir,'w').write( json.dumps( jobs_for, indent=2) )
open('%s/GQ.txt'%monitor_dir,'w').write( report )

open('replaced_blocks.json','w').write( json.dumps( replaced, indent=2) )

