#!/usr/bin/env python
from assignSession import *
from utils import componentInfo, sendEmail, setDatasetStatus, unifiedConfiguration, workflowInfo, siteInfo, sendLog, reqmgr_url, monitor_dir, moduleLock, userLock, global_SI, do_html_in_each_module, getWorkflows, closeoutInfo, batchInfo
from utils import ThreadHandler
import threading
import reqMgrClient
import json
import time
import sys
import os
from utils import getDatasetEventsAndLumis, campaignInfo, getDatasetPresence, getWorkflowByCampaign
from collections import defaultdict
import re
import copy
import random
import optparse
import sqlalchemy 
from campaignAPI import deleteCampaignConfig


def spawn_harvesting(url, wfi , sites_for_DQMHarvest):
    SI = global_SI()

    all_OK = {}
    requests = []
    outputs = wfi.request['OutputDatasets'] 
    if ('EnableHarvesting' in wfi.request and not wfi.request['EnableHarvesting']) and ('DQMConfigCacheID' in wfi.request and wfi.request['DQMConfigCacheID']):
        if not 'MergedLFNBase' in wfi.request:
            print("fucked up")
            sendEmail('screwed up wl cache','%s wl cache is bad'%(wfi.request['RequestName']))
            all_OK['fake'] = False
            return all_OK,requests

        wfi = workflowInfo(url, wfi.request['RequestName'])
        dqms = [out for out in outputs if '/DQM' in out]

        for dqm_input in dqms:
            ## handle it properly
            harvesting_schema = {
                'Requestor': os.getenv('USER'),
                'RequestType' : 'DQMHarvest',
                'Group' : 'DATAOPS'
                }
            copy_over = [
                'AcquisitionEra',
                'ProcessingString',
                'DQMUploadUrl',
                'CMSSWVersion',
                'CouchDBName',
                'CouchWorkloadDBName',
                'ConfigCacheUrl',
                'DbsUrl',
                'inputMode',
                'DQMConfigCacheID',
                'OpenRunningTimeout',
                'ScramArch',
                'CMSSWVersion',
                'Campaign',
                'Memory', #dummy
                'SizePerEvent', #dummy
                'GlobalTag', #dummy
                ]
            for item in copy_over:
                if item in wfi.request:
                    harvesting_schema[item] = copy.deepcopy(wfi.request[item])
                else:
                    print((item,"is not in initial schema"))

            harvesting_schema['InputDataset'] = dqm_input
            harvesting_schema['TimePerEvent'] = 1
            harvesting_schema['PrepID'] = 'Harvest-'+wfi.request['PrepID']
            if len(wfi.request['RequestString'])>60:
                wfi.request['RequestString']= wfi.request['RequestString'][:60]
                print(("truncating request string",wfi.request['RequestString']))
                
            harvesting_schema['RequestString'] = 'HARVEST-'+wfi.request['RequestString']
            harvesting_schema['DQMHarvestUnit'] = 'byRun'
            harvesting_schema['RequestPriority'] = min(wfi.request['RequestPriority']*10,999999)

            harvest_request = reqMgrClient.submitWorkflow(url, harvesting_schema)
            if not harvest_request:
                print(("Error in making harvesting for",wfi.request['RequestName']))
                print("schema")
                print((json.dumps( harvesting_schema, indent = 2)))
                harvest_request = reqMgrClient.submitWorkflow(url, harvesting_schema)
                if not harvest_request:
                    print(("Error twice in harvesting for",wfi.request['RequestName']))
                    print("schema")
                    print((json.dumps( harvesting_schema, indent = 2)))

            if harvest_request:
                requests.append( harvest_request )
                ## should we protect for setting approved ? no, it's notified below, assignment will fail, likely
                data = reqMgrClient.setWorkflowApproved(url, harvest_request)
                print(("created",harvest_request,"for harvesting of",dqm_input))
                wfi.sendLog('closor',"created %s for harvesting of %s"%( harvest_request, dqm_input))
                ## assign it directly
                team = wfi.request['Team']
                parameters={
                    'SiteWhitelist' : [SI.SE_to_CE(se) for se in wfi.request['NonCustodialSites']],
                    'AcquisitionEra' : wfi.acquisitionEra(),
                    'ProcessingString' : wfi.processingString(),
                    'MergedLFNBase' : wfi.request['MergedLFNBase'], 
                    'ProcessingVersion' : wfi.request['ProcessingVersion'],
                    'execute' : True
                    }

                parameters['SiteWhitelist'] = sites_for_DQMHarvest

                result = reqMgrClient.assignWorkflow(url, harvest_request, team, parameters)
                if not result:
                    wfi.sendLog('closor','%s was created at announcement of %s in %s, failed to assign'%(harvest_request, dqm_input, wfi.request['RequestName']))
                    sendLog('closor','%s was created at announcement of %s in %s, failed to assign'%(harvest_request, dqm_input, wfi.request['RequestName']), level='critical')
                else:
                    wfi.sendLog('closor','%s was created at announcement of %s in %s, and assigned'%(harvest_request, dqm_input, wfi.request['RequestName']))

            else:
                wfi.sendLog('closor',"could not make the harvesting request")
                sendLog('closor',"could not make the harvesting request for %s"% wfi.request['RequestName'], level='critical')
                all_OK[dqm_input]=False                    
    return (all_OK, requests)

def closor(url, specific=None, options=None):
    if userLock(): return
    mlock  = moduleLock()
    if mlock() and not options.manual: return
    up = componentInfo(soft=['mcm','wtc'])
    if not up.check(): return


    UC = unifiedConfiguration()
    CI = campaignInfo()
    BI = batchInfo()
    CloseI = closeoutInfo()

    all_late_files = []

    jump_the_line = options.announce if options else False
    if jump_the_line:
        print("announce option is on. Checking on things on-going ready to be announced")
        wfs = session.query(Workflow).filter(Workflow.status.contains('announce')).filter(sqlalchemy.not_(Workflow.status.contains('announced'))).all()
    else:
        print("regular option. Checking on things done and to be announced")
        wfs = session.query(Workflow).filter(Workflow.status=='close').all()

    if specific:
        wfs = [wfo for wfo in wfs if specific in wfo.name]
    wfs_n = [w.name for w in wfs]

    print("unique names?")
    print((len(set(wfs_n)) == len(wfs_n)))
    
    held = set()

    print((len(wfs),"closing"))
    random.shuffle( wfs )    
    max_per_round = UC.get('max_per_round').get('closor',None)
    if options.limit: max_per_round = options.limit

    if max_per_round: 
        ## order them by priority
        all_closedout = sorted(getWorkflows(url, 'closed-out', details=True), key = lambda r : r['RequestPriority'])
        all_closedout = [r['RequestName'] for r in all_closedout]
        def rank( wfn ):
            return all_closedout.index( wfn ) if wfn in all_closedout else 0

        wfs = sorted( wfs, key = lambda wfo : rank( wfo.name ),reverse=True)
        wfs = wfs[:max_per_round]

    batch_go = {}
    batch_warnings = defaultdict(set)
    batch_extreme_warnings = defaultdict(set)
    batch_goodness = UC.get("batch_goodness")

    closers = []

    print((len(wfs),"closing"))
    th_start = time.mktime(time.gmtime())

    for iwfo,wfo in enumerate(wfs):
        if specific and not specific in wfo.name: continue
        if not options.manual and ('cmsunified_task_HIG-RunIIFall17wmLHEGS-05036__v1_T_200712_005621_4159'.lower() in (wfo.name).lower() or 'pdmvserv_task_HIG-RunIISummer16NanoAODv7-03979__v1_T_200915_013748_1986'.lower() in (wfo.name).lower()): continue
        closers.append( CloseBuster(
            wfo = wfo,
            url = url,
            CI = CI,
            UC = UC,
            jump_the_line = jump_the_line,
            batch_goodness = batch_goodness,
            batch_go = batch_go,
            #stats = stats,
            batch_warnings = batch_warnings,
            batch_extreme_warnings = batch_extreme_warnings,
            all_late_files = all_late_files,
            held = held,
            ))

    
    run_threads = ThreadHandler( threads = closers,
                                 n_threads = options.threads,
                                 sleepy = 10,
                                 timeout = None,
                                 verbose = True,
                                 label = 'closor')

    run_threads.start()


    ## waiting on all to complete
    while run_threads.is_alive():
        #print "Waiting on closing threads",time.asctime(time.gmtime())
        time.sleep(5)

    print((len(run_threads.threads),"finished thread to gather information from"))
    failed_threads = 0
    for to in run_threads.threads:
        if to.failed:
            failed_threads += 1
            continue
        if to.outs:
            for outO in to.outs:
                out = outO.datasetname
                odb = session.query(Output).filter(Output.datasetname==out).first()
                if not odb:
                    print(("adding an output object",out))
                    session.add( outO )
                else:
                    odb.date = outO.date
                
        if to.to_status:
            to.wfo.status = to.to_status

        if to.to_wm_status:
            to.wfo.wm_status = to.to_wm_status
        if to.closing:
            CloseI.pop( to.wfo.name )

        session.commit()

    th_stop = time.mktime(time.gmtime())

    if wfs:
        time_spend_per_workflow = (th_stop-th_start) / float(len(wfs))
        print(("Average time spend per workflow is", time_spend_per_workflow))

    if float(failed_threads/run_threads.n_threads) > 0:
        sendLog('checkor','%d/%d threads have failed, better check this out'% (failed_threads, run_threads.n_threads), level='critical')
        sendEmail('checkor','%d/%d threads have failed, better check this out'% (failed_threads,run_threads.n_threads))

    days_late = 0.
    retries_late = 10

    really_late_files = [info for info in all_late_files if info['retries']>=retries_late]
    really_late_files = [info for info in really_late_files if info['delay']/(60*60*24.)>=days_late]

    if really_late_files:
        subject = 'These %d files are lagging for %d days and %d retries announcing dataset \n%s'%(len(really_late_files), days_late, retries_late, json.dumps( really_late_files , indent=2) )
        #sendEmail('waiting for files to announce', subject)
        sendLog('closor', subject, level='warning')
        sendLog('closor',subject)
        print(subject)
        open('%s/stuck_files.json'%monitor_dir,'w').write( json.dumps( really_late_files , indent=2))

    if held:
        sendLog('closor',"the workflows below are held up \n%s"%("\n".join( sorted(held) )), level='critical')

    for bname,go in list(batch_go.items()):
        if go:
            subject = "Release Validation Samples Batch %s"% bname
            issues=""
            if batch_extreme_warnings[ bname ]:
                subject = "Low Statistics for %s"% bname
                issues="The following datasets have outstanding completion (<50%%) issues:\n\n"
                issues+="\n".join( sorted( batch_extreme_warnings[ bname ] ))
                issues+="\n\n"
            elif batch_warnings[ bname ]:
                issues="The following datasets have outstanding completion (<%d%%) issues:\n\n"% batch_goodness
                issues+="\n".join( sorted( batch_warnings[ bname ] ))
                issues+="\n\n"    
            text = ""
            text+= "Dear all,\n\n"
            text+= "A batch of release validation workflows has finished.\n\n"
            text+= "Batch ID:\n\n"
            text+= "%s\n\n"%( bname )
            text+= "Detail of the workflows\n\n"
            text+= "https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?campaign=%s\n\n"%( bname )
            text+= "%s\n\n"%(issues) 
            text+= "This is an automated message.\n\n"
            text+= ""
            #to = ['hn-cms-relval@cern.ch']
            to = ['cmstalk+relval@dovecotmta.cern.ch']
            sendEmail(subject, text, destination=to )
            ## just announced ; take it out now.
            BI.pop( bname )
            deleteCampaignConfig(bname)


    if os.path.isfile('.closor_stop'):
        print("The loop on workflows was shortened")
        sendEmail('closor','Closor loop was shortened artificially using .closor_stop')
        os.system('rm -f .closor_stop')
        



class CloseBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        ## a bunch of other things
        for k,v in list(args.items()):
            setattr(self, k, v)

        self.failed = False
        self.closing = False
        self.to_status = None
        self.to_wm_status = None
        self.outs = []
        self.wfi = None

    def run(self):
        try:
            self.close()
        except Exception as e:
            import traceback
            sendLog('closor','failed on %s due to %s and %s'%( self.wfo.name, str(e), traceback.format_exc()), level='critical')
            self.failed = True

    def close(self):
        if os.path.isfile('.closor_stop'):
            print("The closing of workflows is shortened")
            return 

        url = self.url
        batch_go = self.batch_go
        CI = self.CI
        UC = self.UC
        wfo = self.wfo

        jump_the_line = self.jump_the_line
        batch_goodness = self.batch_goodness
        check_parentage_to_announce = UC.get('check_parentage_to_announce')
        check_fullcopy_to_announce = UC.get('check_fullcopy_to_announce')

        ## what is the expected #lumis 
        self.wfi = workflowInfo(url, wfo.name )
        wfi = self.wfi
        wfo.wm_status = wfi.request['RequestStatus']

        if wfi.isRelval():
            has_batch_go = False
            batch_name = wfi.getCampaign()
            if not batch_name in batch_go:
                ## do the esimatation whethere this can be announced : only once per batch
                in_batches = getWorkflowByCampaign(url , batch_name, details=True)
                batch_go[ batch_name ]  = all([not s in ['completed','running-open','running-closed','acquired','staged','staging','assigned','assignment-approved'] for s in [r['RequestStatus'] for r in in_batches]])
            ## already verified
            has_batch_go = batch_go[batch_name]
            if not has_batch_go:
                wfi.sendLog('closor', 'Cannot close for now because the batch <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?campaign=%s>%s</a> is not all close'%( batch_name, batch_name))
                return


        if wfi.request['RequestStatus'] in  ['announced','normal-archived'] and not options.force:
            ## manually announced ??
            self.to_status = 'done'
            self.to_wm_status = wfi.request['RequestStatus']
            wfi.sendLog('closor','%s is announced already : %s'%( wfo.name,self.to_wm_status))
            return 

        if jump_the_line:
            wfi.sendLog('closor','Announcing while completing')

        expected_lumis = 1
        if not 'TotalInputLumis' in wfi.request:
            print((wfo.name,"has not been assigned yet, or the database is corrupted"))
        elif wfi.request['TotalInputLumis']==0:
            print((wfo.name,"is corrupted with 0 expected lumis"))
        else:
            expected_lumis = wfi.request['TotalInputLumis']

        ## what are the outputs
        outputs = wfi.request['OutputDatasets']
        ## check whether the number of lumis is as expected for each
        all_OK = defaultdict(lambda : False)
        stats = defaultdict(int)
        if len(outputs): 
            print((wfo.name,wfi.request['RequestStatus']))
        for out in outputs:
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=out)
            self.outs.append( Output( datasetname = out ))
            odb = self.outs[-1]
            odb.workflow = wfo
            odb.nlumis = lumi_count
            odb.nevents = event_count
            odb.workfow_id = wfo.id
            odb.expectedlumis = expected_lumis
            odb.date = time.mktime(time.gmtime())

            fraction = lumi_count/float(expected_lumis)*100.

            completion_line = "%60s %d/%d = %3.2f%%"%(out,lumi_count,expected_lumis,fraction)
            wfi.sendLog('closor',"\t%s"% completion_line)
            if wfi.isRelval() and fraction < batch_goodness:
                self.batch_warnings[ wfi.getCampaign()].add( completion_line )
                if fraction < 50:
                    self.batch_extreme_warnings[ wfi.getCampaign()].add( completion_line )
            stats[out] = lumi_count
            all_OK[out] = True 


        ## check for at least one full copy prior to moving on
        for out in outputs:
            all_OK[out] = True

        ## only that status can let me go into announced
        if all(all_OK.values()) and ((wfi.request['RequestStatus'] in ['closed-out']) or options.force or jump_the_line):
            print((wfo.name,"to be announced"))
            results=[]
            if not results:
                for out in outputs:
                    print(("dealing with",out))
                    if out in stats and not stats[out]: 
                        continue
                    _,dsn,process_string,tier = out.split('/')

                    if all_OK[out]:
                        print("setting valid")
                        results.append(setDatasetStatus(out, 'VALID', withFiles=False))
                    if all_OK[out] and wfi.isRelval():
                        ## make the specific relval rules and the replicas
                        ## figure the destination(s) out
                        destinations = set()
                        if tier in UC.get("tiers_to_rucio_relval"):
                            wfi.sendLog('closor', "Data Tier: %s is blacklisted, so skipping dataset placement for: %s" % (tier,out))
                            continue
                        if tier != "RECO" and tier != "ALCARECO":
                            destinations.add('T2_CH_CERN')
                        if tier == "GEN-SIM":
                            destinations.add('T1_US_FNAL_Disk')
                        if tier == "GEN-SIM-DIGI-RAW":
                            destinations.add('T1_US_FNAL_Disk')
                        if tier == "GEN-SIM-RECO":
                            destinations.add('T1_US_FNAL_Disk')

                        if "RelValTTBar" in dsn and "TkAlMinBias" in process_string and tier != "ALCARECO":
                            destinations.add('T2_CH_CERN')

                        if "MinimumBias" in dsn and "SiStripCalMinBias" in process_string and tier != "ALCARECO":
                            destinations.add('T2_CH_CERN')
                        
                        if destinations:
                            wfi.sendLog('closor', '%s to go to %s'%(out, ', '.join( sorted( destinations ))))
                        
                    elif all_OK[out]:

                        campaign = None
                        try:
                            campaign = out.split('/')[2].split('-')[0]
                        except:
                            if 'Campaign' in wfi.request and wfi.request['Campaign']:
                                campaign = wfi.request['Campaign']
                        to_DDM = False
                        ## campaign override
                        if campaign and campaign in CI.campaigns and 'toDDM' in CI.campaigns[campaign] and tier in CI.campaigns[campaign]['toDDM']:
                            to_DDM = True

                        ## by typical enabling
                        if tier in UC.get("tiers_to_rucio_nonrelval"):
                            wfi.sendLog('closor',"Data Tier: %s is blacklisted, so skipping dataset placement for: %s" % (tier,out))
                            continue

                        if tier in UC.get("tiers_to_DDM"):
                            to_DDM = True
                        ## check for unitarity
                        if not tier in UC.get("tiers_no_DDM")+UC.get("tiers_to_DDM"):
                            print(("tier",tier,"neither TO or NO DDM for",out))
                            results.append('Not recognitized tier %s'%tier)
                            #sendEmail("failed DDM injection","could not recognize %s for injecting in DDM"% out)
                            sendLog('closor', "could not recognize %s for injecting in DDM"% out, level='critical')
                            continue

                        n_copies = 1
                        destinations=[]
                        if to_DDM and campaign and campaign in CI.campaigns and 'DDMcopies' in CI.campaigns[campaign]:
                            ddm_instructions = CI.campaigns[campaign]['DDMcopies']
                            if type(ddm_instructions) == int:
                                n_copies = CI.campaigns[campaign]['DDMcopies']
                            elif type(ddm_instructions) == dict:
                                ## a more fancy configuration
                                for ddmtier,indication in list(ddm_instructions.items()):
                                    if ddmtier==tier or ddmtier in ['*','all']:
                                        ## this is for us
                                        if 'N' in indication:
                                            n_copies = indication['N']
                                        if 'host' in indication:
                                            destinations = indication['host']
                                            
                        destination_spec = ""
                        if destinations:
                            destination_spec = "--destination="+",".join( destinations )
                        group_spec = "" ## not used yet
                    else:
                        print((wfo.name,"no stats for announcing",out))
                        results.append('No Stats')

                # adding check for PrentageResolved flag from ReqMgr:
                if wfi.request['RequestType'] == 'StepChain' and check_parentage_to_announce:
                    if wfi.request['ParentageResolved']:
                        results.append(True)
                    else:
                        wfi.sendLog('closor',"Delayed announcement of %s due to unresolved Parentage dependencies" % wfi.request['RequestName'])
                        results.append('No ParentageResolved')

                if all([result in ['None',None,True,'No ParentageResolved'] for result in results]):
                    if not jump_the_line:
                        ## only announce if all previous are fine
                        res = reqMgrClient.announceWorkflowCascade(url, wfo.name)
                        if not res in ['None',None]:
                            ## check the status again, it might well have toggled
                            wl_bis = workflowInfo(url, wfo.name)
                            self.to_wm_status = wl_bis.request['RequestStatus']
                            if wl_bis.request['RequestStatus'] in  ['announced','normal-archived']:
                                res = None
                            else:
                                res = reqMgrClient.announceWorkflowCascade(url, wfo.name) 

                        results.append( res )
                                
            print(results)
            if all([result in ['None',None,True,'No ParentageResolved'] for result in results]):
                if jump_the_line:
                    if not 'announced' in wfo.status:
                        self.to_status = wfo.status.replace('announce','announced')
                else:
                    self.to_status = 'done'
                    self.closing = True
                
                    
                wfi.sendLog('closor',"workflow outputs are announced")
            else:
                wfi.sendLog('closor',"Error with %s to be announced \n%s"%( wfo.name, json.dumps( results )))
            
        elif wfi.request['RequestStatus'] in ['failed','aborted','aborted-archived','rejected','rejected-archived','aborted-completed']:
            if wfi.isRelval():
                self.to_status = 'forget'
                self.to_wm_status = wfi.request['RequestStatus']
                wfi.sendLog('closor',"%s is %s, but will not be set in trouble to find a replacement."%( wfo.name, self.to_wm_status))
            else:
                self.to_status = 'trouble'
                self.to_wm_status = wfi.request['RequestStatus']
        else:
            print((wfo.name,"not good for announcing:",wfi.request['RequestStatus']))
            wfi.sendLog('closor',"cannot be announced")
            self.held.add( wfo.name )




    
if __name__ == "__main__":
    url = reqmgr_url
    parser = optparse.OptionParser()
    parser.add_option('--no_harvest',help='Bypass the harvesting',default=False,action='store_true')
    parser.add_option('--limit',help="Number of workflow to pass",default=0, type=int)
    parser.add_option('--force', help="Force pushing the workflow through", default=False,action='store_true')
    parser.add_option('--announce', help="Announce the outputs that should be announced", default=False,action='store_true')
    parser.add_option('--threads',help='Number of threads for processing workflows',default=5, type=int)
    parser.add_option('-m','--manual', help='Manual close, bypassing lock check',action='store_true',dest='manual',default=False)
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    closor(url,spec, options=options)
