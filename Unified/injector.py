#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, getWorkflowById, getWorkLoad, componentInfo, sendEmail, workflowInfo, sendLog, reqmgr_url, getDatasetStatus, unifiedConfiguration, duplicateLock
import sys
import copy
import os
from htmlor import htmlor
from invalidator import invalidator 
import optparse
import json
import time
from collections import defaultdict

def injector(url, options, specific):
    if duplicateLock(): return 

    use_mcm = True
    up = componentInfo( mcm = use_mcm, soft=['mcm'] )
    if not up.check(): return
    use_mcm = up.status['mcm']

    UC = unifiedConfiguration()

    workflows = getWorkflows(url, status=options.wmstatus, user=options.user)
    for user in UC.get("user_rereco"):
        workflows.extend( getWorkflows(url, status=options.wmstatus, user=user, rtype="ReReco")) 
    for user in (options.user_relval.split(',') if options.user_relval else UC.get("user_relval")) :
        workflows.extend( getWorkflows(url, status=options.wmstatus, user=user, rtype="TaskChain")) 

    print len(workflows),"in line"
    cannot_inject = set()
    to_convert = set()
    status_cache = defaultdict(str)

    ## browse for assignment-approved requests, browsed for ours, insert the diff
    for wf in workflows:
        if specific and not specific in wf: continue
        exists = session.query(Workflow).filter(Workflow.name == wf ).first()
        if not exists:
            wfi = workflowInfo(url, wf)
            #wl = getWorkLoad(url, wf)
            ## check first that there isn't related here with something valid
            can_add = True
            ## first try at finding a match
            #            print wfi.request
            familly = session.query(Workflow).filter(Workflow.name.contains(wfi.request['PrepID'])).all()
            if not familly:
                #req_familly = getWorkflowById( url, wl['PrepID'])
                #familly = [session.query(Workflow).filter(Workflow.name == member).first() for member in req_familly]
                pids = wfi.getPrepIDs()
                req_familly = []
                for pid in pids:
                    req_familly.extend( getWorkflowById( url, pid, details=True) )
                    
                familly = []
                print len(req_familly),"members"
                for req_member in req_familly:
                    #print "member",req_member['RequestName']
                    owfi = workflowInfo(url, req_member['RequestName'], request=req_member)
                    other_pids = owfi.getPrepIDs()
                    if set(pids) == set(other_pids):
                        ## this is a real match
                        familly.extend( session.query(Workflow).filter(Workflow.name == req_member['RequestName']).all() )

            for lwfo in familly:
                if lwfo:
                    ## we have it already
                    if not lwfo.status in ['forget','trouble','forget-unlock','forget-out-unlock']:
                        wfi.sendLog('injector',"Should not put %s because of %s %s"%( wf, lwfo.name,lwfo.status ))
                        sendLog('injector',"Should not put %s because of %s %s"%( wf, lwfo.name,lwfo.status ), level='critical')
                        print "Should not put",wf,"because of",lwfo.name,lwfo.status
                        cannot_inject.add( wf )
                        can_add = False
            ## add a check on validity of input datasets
            _,prim,par,sec = wfi.getIO()
            for d in list(prim)+list(par)+list(sec):
                if not d in status_cache:
                    status_cache[d] = getDatasetStatus(d)
                if status_cache[d] != 'VALID':
                    wfi.sendLog('injector',"One of the input is not VALID. %s : %s"%( d, status_cache[d]))
                    sendLog('injector',"One of the input of %s is not VALID. %s : %s"%( wf, d, status_cache[d]), level='critical')
                    can_add = False
            if not can_add: continue

            ## temporary hack to transform specific taskchain into stepchains
            all_tiers = map(lambda o : o.split('/')[-1], wfi.request['OutputDatasets'])
            duplicate_tier = (len(all_tiers) != len(set(all_tiers)))
            transform_keywords = [
                #'RunIISummer15wmLHE',
                #'RunIISummer15GS',
                'RunIIWinter15GenOnly',
                #'RunIIWinter15wmLHE'
                ]
            if (not options.no_convert) and wfi.request['RequestType'] == 'TaskChain' and wfi.request['TaskChain']>1 and any([keyword in wf for keyword in transform_keywords]) and not duplicate_tier:
                to_convert.add( wf )
                wfi.sendLog('injector','Transforming %s TaskChain into StepChain'%wf)

            wfi.sendLog('injector',"considering %s"%wf)

            new_wf = Workflow( name = wf , status = options.setstatus, wm_status = options.wmstatus) 
            session.add( new_wf )
            session.commit()
            time.sleep(0.5)
        else:
            #print "already have",wf
            pass
    

    if cannot_inject:
        #sendEmail('workflow duplicates','These workflow cannot be added in because of duplicates \n\n %s'%( '\n'.join(cannot_inject)))
        sendLog('injector','These workflow cannot be added in because of duplicates \n\n %s'%( '\n'.join(cannot_inject)), level='warning')
        
    for wf in to_convert:
        os.system('./Unified/rejector.py --clone --to_step --comments \"Transform to StepChain\" %s'% wf)

    ## passing a round of invalidation of what needs to be invalidated
    if use_mcm and (options.invalidate or True):
        invalidator(url)

    no_replacement = set()

    #print "getting all transfers"
    #all_transfers=session.query(Transfer).all()
    #print "go!"

    ## pick up replacements
    for wf in session.query(Workflow).filter(Workflow.status == 'trouble').all():
        print wf.name
        if specific and not specific in wf.name: continue
        print wf.name
        wfi = workflowInfo(url, wf.name )
        wl = wfi.request #getWorkLoad(url, wf.name)
        familly = getWorkflowById( url, wl['PrepID'] )
        true_familly = []
        for member in familly:
            if member == wf.name: continue
            fwl = getWorkLoad(url , member)
            if options.replace:
                if member != options.replace: continue
            else:
                if fwl['RequestDate'] < wl['RequestDate']: continue
                if fwl['RequestType']=='Resubmission': continue
                if fwl['RequestStatus'] in ['None',None,'new']: continue
                if fwl['RequestStatus'] in ['rejected','rejected-archived','aborted','aborted-archived']: continue
            true_familly.append( fwl )

        if len(true_familly)==0:
            #sendLog('injector','%s had no replacement'%wf.name, level='critical')
            if wfi.isRelval():
                #wfi.sendLog('injector','the workflow was found in trouble with no replacement. As a relval, there is no clean way to handle this.')
                wfi.sendLog('injector','the workflow was found in trouble with no replacement. As a relval, there is no clean way to handle this. Setting forget')
                wf.status = 'forget'
                session.commit()
            else:
                wfi.sendLog('injector','the workflow was found in trouble with no replacement')
                no_replacement.add( wf.name )
            continue
        else:
            wfi.sendLog('injector','the workflow was found in trouble and has a replacement')
                    
        print wf.name,"has",len(familly),"familly members"
        print wf.name,"has",len(true_familly),"true familly members"

        ##we cannot have more than one of them !!! pick the last one
        if len(true_familly)>1:
            #sendEmail('multiple wf','please take a look at injector for %s'%wf.name)
            sendLog('injector','Multiple wf in line, will take the last one for %s \n%s'%( wf.name, ', '.join(fwl['RequestName'] for fwl in true_familly)), level='critical')

        for fwl in true_familly[-1:]:
            member = fwl['RequestName']
            new_wf = session.query(Workflow).filter(Workflow.name == member).first()
            if not new_wf:
                sendLog('injector',"putting %s as replacement of %s"%( member, wf.name))
                status = 'away'
                if fwl['RequestStatus'] in ['assignment-approved']:
                    status = 'considered'
                new_wf = Workflow( name = member, status = status, wm_status = fwl['RequestStatus'])
                wf.status = 'forget'
                session.add( new_wf ) 
            else:
                if new_wf.status == 'forget': continue
                sendLog('injector',"getting %s as replacement of %s"%( new_wf.name, wf.name ))
                wf.status = 'forget'

            for tr in session.query(TransferImp).filter( TransferImp.workflow_id == wf.id).all():
                ## get all transfer working for the old workflow
                existing = session.query(TransferImp).filter( TransferImp.phedexid == tr.phedexid).filter( TransferImp.workflow_id == new_wf.id).all()
                tr.active = False ## disable the old one
                if not existing:
                    ## create the transfer object for the new dependency
                    tri = TransferImp( phedexid = tr.phedexid,
                                       workflow = new_wf)
                    session.add( tri )
                session.commit()


        ## don't do that automatically
        #wf.status = 'forget'
        session.commit()
    if no_replacement:
        #sendEmail('workflow with no replacement','%s \n are dangling there'%( '\n'.join(no_replacement)))
        sendLog('injector','workflow with no replacement\n%s \n are dangling there'% ( '\n'.join(no_replacement)), level='critical')

if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
    parser.add_option('-i','--invalidate',help="fetch invalidations from mcm",default=False,action='store_true')
    parser.add_option('-w','--wmstatus',help="from which status in req-mgr",default="assignment-approved")
    parser.add_option('-s','--setstatus',help="What status to set locally",default="considered")
    parser.add_option('-u','--user',help="What user to fetch workflow from",default="pdmvserv")
    parser.add_option('-r','--replace',help="the workflow name that should be used for replacement",default=None)
    parser.add_option('--user_relval',help="The user that can inject workflows for relvals", default=None)
    parser.add_option('--no_convert',help="Prevent the conversion to stepchain", default=False)
    (options,args) = parser.parse_args()
    
    spec = None
    if len(args)!=0:
        spec = args[0]
    injector(url,options,spec)
    if not spec:
        htmlor()
    
