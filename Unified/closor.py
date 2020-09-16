#!/usr/bin/env python
from assignSession import *
from utils import componentInfo, sendEmail, setDatasetStatus, unifiedConfiguration, workflowInfo, siteInfo, sendLog, reqmgr_url, monitor_dir, moduleLock, userLock, global_SI, do_html_in_each_module, getWorkflows, pass_to_dynamo, closeoutInfo, batchInfo
from utils import ThreadHandler
import threading
import reqMgrClient
import json
import time
import sys
import os
from utils import getDatasetEventsAndLumis, campaignInfo, getDatasetPresence, findLateFiles, updateSubscription, makeReplicaRequest, getWorkflowByCampaign
from htmlor import htmlor
from collections import defaultdict
import reqMgrClient
import re
import copy
import random
import optparse
import sqlalchemy 
from JIRAClient import JIRAClient
from campaignAPI import deleteCampaignConfig

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
        print "announce option is on. Checking on things on-going ready to be announced"
        wfs = session.query(Workflow).filter(Workflow.status.contains('announce')).filter(sqlalchemy.not_(Workflow.status.contains('announced'))).all()
    else:
        print "regular option. Checking on things done and to be announced"
        wfs = session.query(Workflow).filter(Workflow.status=='close').all()

    if specific:
        wfs = [wfo for wfo in wfs if specific in wfo.name]
    wfs_n = [w.name for w in wfs]

    print "unique names?"
    print len(set(wfs_n)) == len(wfs_n)
    
    held = set()

    print len(wfs),"closing"
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

    print len(wfs),"closing"
    th_start = time.mktime(time.gmtime())

    for iwfo,wfo in enumerate(wfs):
        if specific and not specific in wfo.name: continue
        if not options.manual and 'rucio' in (wfo.name).lower(): continue
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

    JC = JIRAClient() if up.status.get('jira',False) else None
    print len(run_threads.threads),"finished thread to gather information from"
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
                    print "adding an output object",out
                    session.add( outO )
                else:
                    odb.date = outO.date
                
        if to.to_status:
            to.wfo.status = to.to_status
            if JC and to.to_status == "done" and to.wfi:
                jiras = JC.find({"prepid" : to.wfi.request['PrepID']})
                for jira in jiras:
                    JC.close(jira.key)

        if to.to_wm_status:
            to.wfo.wm_status = to.to_wm_status
        if to.closing:
            CloseI.pop( to.wfo.name )

        session.commit()

    th_stop = time.mktime(time.gmtime())

    if wfs:
        time_spend_per_workflow = (th_stop-th_start) / float(len(wfs))
        print "Average time spend per workflow is", time_spend_per_workflow

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
        print subject
        open('%s/stuck_files.json'%monitor_dir,'w').write( json.dumps( really_late_files , indent=2))

    if held:
        sendLog('closor',"the workflows below are held up \n%s"%("\n".join( sorted(held) )), level='critical')

    for bname,go in batch_go.items():
        if go:
            subject = "Release Validation Samples Batch %s"% bname
            issues=""
            #if batch_warnings[ bname ]:
            #    issues="The following datasets have outstanding completion (<%d%%) issues:\n\n"% batch_goodness
            #    issues+="\n".join( sorted( batch_warnings[ bname ] ))
            #    issues+="\n\n"
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
            to = ['hn-cms-relval@cern.ch']
            sendEmail(subject, text, destination=to )
            ## just announced ; take it out now.
            BI.pop( bname )
            deleteCampaignConfig(bname)


    if os.path.isfile('.closor_stop'):
        print "The loop on workflows was shortened"
        sendEmail('closor','Closor loop was shortened artificially using .closor_stop')
        os.system('rm -f .closor_stop')
        



class CloseBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        ## a bunch of other things
        for k,v in args.items():
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
            print "The closing of workflows is shortened"
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
                batch_go[ batch_name ]  = all(map(lambda s : not s in ['completed','running-open','running-closed','acquired','staged','staging','assigned','assignment-approved'], [r['RequestStatus'] for r in in_batches]))
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
            print wfo.name,"has not been assigned yet, or the database is corrupted"
        elif wfi.request['TotalInputLumis']==0:
            print wfo.name,"is corrupted with 0 expected lumis"
        else:
            expected_lumis = wfi.request['TotalInputLumis']

        ## what are the outputs
        outputs = wfi.request['OutputDatasets']
        ## check whether the number of lumis is as expected for each
        all_OK = defaultdict(lambda : False)
        stats = defaultdict(int)
        #print outputs
        if len(outputs): 
            print wfo.name,wfi.request['RequestStatus']
        for out in outputs:
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=out)
            self.outs.append( Output( datasetname = out ))
            odb = self.outs[-1]
            odb.workflow = wfo
            odb.nlumis = lumi_count
            odb.nevents = event_count
            odb.workfow_id = wfo.id
            if odb.expectedlumis < expected_lumis:
                odb.expectedlumis = expected_lumis
            else:
                expected_lumis = odb.expectedlumis
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

    
        ## only that status can let me go into announced
        if all(all_OK.values()) and ((wfi.request['RequestStatus'] in ['closed-out']) or options.force or jump_the_line):
            print wfo.name,"to be announced"
            results=[]
            if not results:
                for out in outputs:
                    print "dealing with",out
                    if out in stats and not stats[out]: 
                        continue
                    _,dsn,process_string,tier = out.split('/')

                    if all_OK[out]:
                        print "setting valid"
                        results.append(setDatasetStatus(out, 'VALID', withFiles=False))
                    else:
                        print wfo.name,"no stats for announcing",out
                        results.append('No Stats')

                # adding check for PrentageResolved flag from ReqMgr:
                if wfi.request['RequestType'] == 'StepChain' and check_parentage_to_announce:
                    if wfi.request['ParentageResolved']:
                        results.append(True)
                    else:
                        wfi.sendLog('closor',"Delayed announcement of %s due to unresolved Parentage dependencies" % wfi.request['RequestName'])
                        results.append('No ParentageResolved')

                if all(map(lambda result : result in ['None',None,True],results)):
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
                                
            print results
            if all(map(lambda result : result in ['None',None,True],results)):
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
            print wfo.name,"not good for announcing:",wfi.request['RequestStatus']
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

    if (not spec) and (not options.limit) and do_html_in_each_module:
        htmlor()
