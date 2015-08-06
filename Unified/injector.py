#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, getWorkflowById, getWorkLoad, componentInfo
import sys
import copy
from htmlor import htmlor
from invalidator import invalidator 
import optparse

def injector(url, options, specific):

    use_mcm = True
    up = componentInfo( mcm = use_mcm, soft=['mcm'] )
    use_mcm = up.status['mcm']

    workflows = getWorkflows(url, status=options.wmstatus,user=options.user)
    existing = [wf.name for wf in session.query(Workflow).all()]
    ## browse for assignment-approved requests, browsed for ours, insert the diff
    for wf in workflows:
        if wf not in existing:
            print "putting",wf
            new_wf = Workflow( name = wf , status = options.setstatus, wm_status = options.wmstatus) 
            session.add( new_wf )
            session.commit()


    existing = [wf.name for wf in session.query(Workflow).all()]

    ## passing a round of invalidation of what needs to be invalidated
    if use_mcm and (options.invalidate or True):
        invalidator(url)


    ## pick up replacements
    for wf in session.query(Workflow).filter(Workflow.status == 'trouble').all():
        if specific and wf.name != specific:
            continue
        print wf.name
        wl = getWorkLoad(url, wf.name)
        familly = getWorkflowById( url, wl['PrepID'] )
        if len(familly)==1:
            print wf.name,"ERROR has no replacement"
            continue
        print wf.name,"has",len(familly),"familly members"
        for member in familly:
            if member != wf.name:
                fwl = getWorkLoad(url , member)
                if options.replace:
                    if member != options.replace: continue
                else:
                    if fwl['RequestDate'] < wl['RequestDate']: continue
                    if fwl['RequestType']=='Resubmission': continue
                    if fwl['RequestStatus'] in ['None',None]: continue

                new_wf = session.query(Workflow).filter(Workflow.name == member).first()
                if not new_wf:
                    print "putting",member,"as replacement of",wf.name
                    status = 'away'
                    if fwl['RequestStatus'] in ['assignment-approved']:
                        status = 'considered'
                    new_wf = Workflow( name = member, status = status, wm_status = fwl['RequestStatus'])
                    wf.status = 'forget'
                    session.add( new_wf ) 
                else:
                    if new_wf.status == 'forget': continue
                    print "getting",new_wf.name,"as replacement of",wf.name
                    wf.status = 'forget'

                for tr in session.query(Transfer).all():
                    if wf.id in tr.workflows_id:
                        sw = copy.deepcopy(tr.workflows_id)
                        sw.remove( wf.id)
                        sw.append(new_wf.id)
                        tr.workflows_id = sw
                        print tr.phedexid,"got",new_wf.name
                        if new_wf.status != 'away':
                            print "\t setting it staging"
                            new_wf.status = 'staging'
                        session.commit()
                        

        ## don't do that automatically
        #wf.status = 'forget'
        session.commit()

        
if __name__ == "__main__":
    url = 'cmsweb.cern.ch'

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
    
