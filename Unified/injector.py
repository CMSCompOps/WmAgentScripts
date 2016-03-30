#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, getWorkflowById, getWorkLoad, componentInfo, sendEmail, workflowInfo, sendLog, reqmgr_url
import sys
import copy
from htmlor import htmlor
from invalidator import invalidator 
import optparse
import json
import time

def injector(url, options, specific):

    use_mcm = True
    up = componentInfo( mcm = use_mcm, soft=['mcm'] )
    if not up.check(): return
    use_mcm = up.status['mcm']

    workflows = getWorkflows(url, status=options.wmstatus, user=options.user)
    workflows.extend( getWorkflows(url, status=options.wmstatus, user='fabozzi', rtype="ReReco")) ## regardless of users, pick up all ReReco on the table

    print len(workflows),"in line"
    cannot_inject = set()
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
                        sendLog('injector',"Should not put %s because of %s %s"%( wf, lwfo.name,lwfo.status ))
                        print "Should not put",wf,"because of",lwfo.name,lwfo.status
                        cannot_inject.add( wf )
                        can_add = False
            if not can_add: continue
            wfi.sendLog('injector',"considering %s"%wf)
            new_wf = Workflow( name = wf , status = options.setstatus, wm_status = options.wmstatus) 
            session.add( new_wf )
            session.commit()
            time.sleep(0.5)
        else:
            #print "already have",wf
            pass
    
    if cannot_inject:
        sendEmail('workflow duplicates','These workflow cannot be added in because of duplicates \n\n %s'%( '\n'.join(cannot_inject)))

    ## passing a round of invalidation of what needs to be invalidated
    if use_mcm and (options.invalidate or True):
        invalidator(url)

    no_replacement = set()

    ## pick up replacements
    for wf in session.query(Workflow).filter(Workflow.status == 'trouble').all():
        if specific and wf.name != specific:
            continue
        print wf.name
        wl = getWorkLoad(url, wf.name)
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
                if fwl['RequestStatus'] in ['None',None]: continue
                if fwl['RequestStatus'] in ['rejected','rejected-archived','aborted','aborted-archived']: continue
            true_familly.append( fwl )

        if len(true_familly)==0:
            sendLog('injector','%s had no replacement'%wf.name)
            no_replacement.add( wf.name )
            continue
        print wf.name,"has",len(familly),"familly members"
        print wf.name,"has",len(true_familly),"true familly members"

        for fwl in true_familly:
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

            for tr in session.query(Transfer).all():
                if wf.id in tr.workflows_id:
                    sw = copy.deepcopy(tr.workflows_id)
                    sw.remove( wf.id)
                    sw.append(new_wf.id)
                    tr.workflows_id = sw
                    print tr.phedexid,"got",new_wf.name
                    if new_wf.status != 'away':
                        print "\t setting it considered"
                        new_wf.status = 'considered'
                    if tr.phedexid<0: ## set it back to positive
                        tr.phedexid = -tr.phedexid
                    session.commit()
                        

        ## don't do that automatically
        #wf.status = 'forget'
        session.commit()
    if no_replacement:
        sendEmail('workflow with no replacement','%s \n are dangling there'%( '\n'.join(no_replacement)))

if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
    parser.add_option('-i','--invalidate',help="fetch invalidations from mcm",default=False,action='store_true')
    parser.add_option('-w','--wmstatus',help="from which status in req-mgr",default="assignment-approved")
    parser.add_option('-s','--setstatus',help="What status to set locally",default="considered")
    parser.add_option('-u','--user',help="What user to fetch workflow from",default="pdmvserv")
    parser.add_option('-r','--replace',help="the workflow name that should be used for replacement",default=None)
    (options,args) = parser.parse_args()
    
    spec = None
    if len(args)!=0:
        spec = args[0]
    injector(url,options,spec)

    htmlor()
    
