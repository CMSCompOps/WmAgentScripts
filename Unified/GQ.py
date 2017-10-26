from utils import getDatasetBlockAndSite, siteInfo, getWorkflows, workflowInfo, monitor_dir, sendLog, sendEmail, makeReplicaRequest, unifiedConfiguration, getDatasetFileLocations, getAgentInfo
from collections import defaultdict
import time
import json
import sys
import random
from assignSession import *


UC = unifiedConfiguration()
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
    replaced = set(json.loads(open('replaced_blocks.json').read()))
except:
    replaced = set()

not_runable_acdc=set()
agents_down = defaultdict(set)
failed_workflow = set()
files_locations = {}
stuck_all_done = set()
heavy_duty = {}

for wf in wfs:
    if spec and not spec in wf['RequestName']: continue

    wfi = workflowInfo(url, wf['RequestName'], request=wf)
    sitewhitelist = wfi.request['SiteWhitelist']
    wqs = wfi.getWorkQueue()
    
    stats = wfi.getWMStats()
    if not 'AgentJobInfo' in stats: stats['AgentJobInfo'] = {}

    ## skip wf that unified does not know about, leaves acdc
    wfo = session.query(Workflow).filter(Workflow.name == wf['RequestName']).first()
    if not (wfo or wf['RequestType']=='Resubmission'): 
        print "not knonw or not acdc : %s"%(wf['RequestName'])
        continue

    ## test the heavyness
    if 'TotalInputLumis' in wf and 'TotalEstimatedJobs' in wf and wf['TotalEstimatedJobs']:
        heavy = (wf['TotalInputLumis'] / float(wf['TotalEstimatedJobs']))
        ## large ratio and no intpu dataset
        if heavy > 5000. and not 'InputDataset' in wf:
            heavy_duty[wf['RequestName']] = { 'TotalInputLumis' : wf['TotalInputLumis'],
                                              'TotalEstimatedJobs' : wf['TotalEstimatedJobs'],
                                              'ratio' : heavy,
                                              }


    #wqes = [w[w['type']] for w in wqs]
    print wf['RequestName'],len(wqs),"elements"

    agent_info = {}
    all_wqe_done = True
    for wq in wqs:
        wqe = wq[wq['type']]
        if not wqe['ChildQueueUrl']:
            agent = 'None'
        else:
            agent = wqe['ChildQueueUrl'].split('/')[-1].split(':')[0]
        ## where the wf is set to be run at and site ready
        wl = [s for s in wqe['SiteWhitelist'] if s in si.sites_ready]
        #wqe_by_agent[agent].append( wqe )

        all_wqe_done &= (wqe['Status']=='Done')
        if wqe['Status'] in ['Failed']:
            failed_workflow.add( wf['RequestName'] )

        if wqe['Status'] in ['Running']:
            all_input_loc = set()
            for b in wqe['Inputs']:
                all_input_loc.update( wqe['Inputs'][b] ) ## intersection maybe ?

            ## find out the numbe of jobs running on that workflow, from that agent
            for running_agent,info in stats['AgentJobInfo'].iteritems():
                if not agent in running_agent: continue ## look only at the agent with running blocks
                for task,tinfo in info['tasks'].iteritems():
                    if 'LogCollect' in task:continue
                    if 'Cleanup' in task:continue
                    if not 'status' in tinfo:
                        ## should also look for the previous step that has nothing running
                        if task in agents_down[agent]: continue
                        print task,"stalled in the agent:",agent
                        a_stall=False
                        for s in all_input_loc:
                            if not s in si.sites_pressure: continue
                            matching,running,_ = si.sites_pressure[s]
                            maxcpu = si.cpu_pledges.get(s, -1)
                            one_stall = ((running+matching) < maxcpu)
                            print "%20s idled %10d running %10d : available %10d %s"%(s,matching,running, maxcpu,'READY' if one_stall else '')
                            if one_stall: a_stall=True
                        if a_stall:
                            print json.dumps(tinfo, indent=2)
                            agents_down[agent].add( task )
                    else:
                        #print task,agent,tinfo['status']
                        pass

        if not wqe['Status'] in ['Available', 'Acquired']:#,'Running']: 
            #print  wqe['Status']
            continue
        camp =wfi.getCampaigns()[0] if len(wfi.getCampaigns()) else None
        if not camp: continue
        #print json.dumps( wqe, indent=2)
        if wqe['NoInputUpdate']: 
            ## input is remote: one day we'd like to move it to disk automatically, but not now
            print "input on aaa for",wfi.request['RequestName']
            continue


        pileup_location = None
        for secondary in wqe['PileupData']:
            if pileup_location == None:
                pileup_location = wqe['PileupData'][secondary]
            else:
                pileup_location = list(set(pileup_location) & set(wqe['PileupData'][secondary]))
            #print pileup_location,"secondary"
        ## wqe site whitelist in terms of SE
        swl = [si.CE_to_SE(s) for s in wl]
        if not swl:
            sendLog('GQ',"There is no site at which the workflow %s can run Was provided with %s"%(wf['RequestName'], ','.join(wqe['SiteWhitelist'])), level='critical')
            wfi.sendLog('GQ',"There is not site at which the workflow can run. Was provided with %s"%( ','.join(wqe['SiteWhitelist'])))
            continue

        not_processable = set()

        wqe_ce = None
        for b in wqe['Inputs']:
            ## what the global queue thinks about the block location
            wqe_se = [si.CE_to_SE(s) for s in wqe['Inputs'][b]]
            
            if not '#' in b: 
                print "acdc doc input",b
                ## this would be an ACDC document input
                #retrieve it and check data file location ?
                original = workflowInfo(url, wfi.request['OriginalRequestName'])
                doc = original.getRecoveryDoc()
                _,prim,_,sec = original.getIO()
                in_files = set()

                for rdoc in doc:
                    in_files.update( rdoc['files'].keys() )
                print len(in_files),"files in ACDC doc"
                ## find all files locations if reading a dataset
                dataset = None
                for dataset in prim:
                    if not dataset in files_locations:
                        files_locations[dataset] = getDatasetFileLocations(url, dataset)
                    print "ACDC reads",dataset
                
                if dataset:
                    ### check all input file
                    for in_file in in_files:
                        files_se=[]
                        if in_file in files_locations[dataset]:
                            files_se= files_locations[dataset][in_file]
                            site_with_data_and_listed = list(set(files_se) & set(swl))
                            if not site_with_data_and_listed:
                                print "File",in_file,"is at",",".join( files_se ),"while asked to run at",",".join(sorted(swl))
                                not_processable.add( in_file )
                            #else:                                print in_file,"checks out"
                else:
                    print "original workflow does not read from any dataset"
                    
                acdc_location = sorted(set([si.CE_to_SE(s) for s in wqe['Inputs'][b]]))
                if pileup_location != None and not wqe['NoPileupUpdate']: ## meaning we have a secondary and we care about it
                    print "intersecting with secondary",','.join(sorted(pileup_location))
                    acdc_location = list(set(acdc_location) & set(pileup_location))
                    
                can_run_at = list(set(acdc_location)&set(swl))
                if not can_run_at:
                    print b,"is at",acdc_location,"and wf set to run from",swl
                    not_runable_acdc.add( wf['RequestName'] )
                    #not_processable.add( b )
                se_whitelist = sorted(set([si.CE_to_SE(s) for s in wqe['SiteWhitelist'] if s in si.sites_ready]))
                missing_in_whitelist = sorted([si.SE_to_CE(s) for s in (set(acdc_location) - set(se_whitelist))])
                if wqe['NoInputUpdate']==False and missing_in_whitelist: #(se_whitelist>=acdc_location):
                    #missing_in_whitelist = sorted([si.SE_to_CE(s) for s in (set(acdc_location) - set(se_whitelist))])
                    print "Should have",missing_in_whitelist,"also in the whitelist, or have xrootd enabled"
                    print sorted(acdc_location),"for the ACDC location"
                    print sorted(se_whitelist),"for the whitelist"
                    not_runable_acdc.add( wf['RequestName'] )
                
                continue
            #b is the block
            ds = b.split('#')[0]
            if not ds in block_locations:
                s_block_locations[ds] = getDatasetBlockAndSite(url, ds, complete='y', vetoes=[])
                for s in s_block_locations[ds]:
                    for bl in s_block_locations[ds][s]:
                        block_locations[ds][bl].append( s )

            if not b in block_locations[ds]:
                print b,"is not to be found in phedex, needed by",wfi.request['RequestName']
                ## should send a critical log
                continue

            #block_ce = [si.SE_to_CE(s) for s in block_locations[ds][b]]
            #wqe_ce = [s for s in wqe['Inputs'][b]]
            ## true location of the data
            block_se = block_locations[ds][b] 


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

        #if wqe_ce == None: wqe_ce = wqe['SiteWhitelist']
        ## check in the agent why it is not running !
        #if agent!='None' and False:
        #    if not agent in agent_info:
        #        agent_info[agent] = getAgentInfo(url, agent)
        #    pending = dict([(site,p) for site,p in agent_info[agent]['sitePendCountByPrio'].items() if site in wqe_ce])
        #    thresholds = dict([(site,p) for site,p in agent_info[agent]['thresholds'].items() if site in wqe_ce])
        #    print json.dumps( pending, indent=2)
        #    print json.dumps( thresholds, indent=2)
                

        if not_processable:
            wfi.sendLog('GQ','The following blocks/files need to be put back on disk \n%s'%('\n'.join( not_processable )))
    if all_wqe_done:
        stuck_all_done.add( wf['RequestName'] )


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
                                                   '\n'.join(sorted(not_runable_acdc))
                                                   ),level='critical')


old_stuck_all_done = set(json.loads(open('stuck_all_done.json').read()))
really_stuck_all_done = old_stuck_all_done & stuck_all_done
if really_stuck_all_done:
    sendLog('GQ','These %d workflows have not toggled further to completed while all WQE are done\n%s'%( len(really_stuck_all_done),'\n'.join(sorted(really_stuck_all_done))),
            level='critical')
open('stuck_all_done.json','w').write( json.dumps( sorted( stuck_all_done), indent=2))

#for agent in wqe_by_agent:
    ## sort by priority
    #work = wqe_by_agent
    #work.sort( key= lambda i : i['Priority'], reversed=True )
    ## now you can check whether this is activity on-going for that block

if failed_workflow:
    sendLog('GQ','These workflows have failed wqe and will stay stuck:\n%s'%('\n'.join( failed_workflow)))
    pass

if agents_down:
    for agent,tasks in agents_down.iteritems():
        if not tasks: continue
        #sendLog('GQ','These tasks look stalled in agent %s \n%s'%( agent, '\n'.join(sorted(tasks))),level='critical')
        pass

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
    open('%s/missing_blocks.json'%monitor_dir,'w').write( json.dumps( sorted(unprocessable), indent=2) )
    #sendEmail('unprocessable blocks',"Sending a notification of this new feature until this gets understood. transfering block automatically back to  processing location. \n"+unproc)

try_me = defaultdict(set)
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

        try_me[go_to].add( b )
        ## pick a site that should host this !
        #wfi.sendLog('GQ','Sending %s to %s'%( b, go_to))

for site,blocks in try_me.items():
    blocks = blocks - replaced 
    if UC.get('block_repositionning'):
        if blocks:
            result = makeReplicaRequest(url, site, list(blocks), 'item relocation', priority='high', approve=True, mail=False)
            replaced.update( blocks )
            sendLog('GQ','replacing %s at %s \n%s'%( '\n,'.join(blocks), site, result),level='warning')
    else:
        sendLog('GQ','tempting to put %s at %s'%( '\n,'.join(blocks), site),level='warning')

open('%s/GQ.json'%monitor_dir,'w').write( json.dumps( jobs_for, indent=2) )
open('%s/GQ.txt'%monitor_dir,'w').write( report )

open('replaced_blocks.json','w').write( json.dumps( sorted(replaced), indent=2) )

if heavy_duty:
    sendLog('GQ','There are some heavy duty workflows in the system, likely to cause damage\n%s\n%s'%(sorted(heavy_duty.keys()), json.dumps( heavy_duty, indent=2) ),level='critical')
