#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, getWorkflowById, getWorkLoad
import sys
import copy
from htmlor import htmlor

def injector(url,wm_status = 'assignment-approved', set_status='considered', talk = False):
    workflows = getWorkflows(url, status=wm_status,user='pdmvserv')
    existing = [wf.name for wf in session.query(Workflow).all()]
    ## browse for assignment-approved requests, browsed for ours, insert the diff
    for wf in workflows:
        if wf not in existing:
            print "putting",wf
            new_wf = Workflow( name = wf , status = set_status, wm_status = wm_status) 
            session.add( new_wf )
            session.commit()


    existing = [wf.name for wf in session.query(Workflow).all()]


    for wf in session.query(Workflow).filter(Workflow.status == 'trouble').all():
        print wf.name
        wl = getWorkLoad(url, wf.name)
        familly = getWorkflowById( url, wl['PrepID'] )
        if len(familly)==1:
            print wf.name,"ERROR has no replacement"
            continue
        if talk:
            print wf.name,"has",len(familly)
        for member in familly:
            if member != wf.name:
                #print member
                fwl = getWorkLoad(url , member)
                #print member
                #print fwl['RequestDate']
                #print wl['RequestDate']

                if fwl['RequestDate'] < wl['RequestDate']: continue
                if fwl['RequestType']=='Resubmission': continue
                if fwl['RequestStatus'] in ['None',None]: continue
                new_wf = session.query(Workflow).filter(Workflow.name == member).first()
                if not new_wf:
                    print "putting",member
                    status = 'away'
                    if fwl['RequestStatus'] in ['assignment-approved']:
                        status = 'considered'
                    new_wf = Workflow( name = member, status = status, wm_status = fwl['RequestStatus'])
                    session.add( new_wf ) 
                    session.commit()
                else:
                    if new_wf.status == 'forget': continue
                    print "getting",new_wf.name,"as replacement of",wf.name

                # clones are never output to the same ?
                #outs = session.query(Output).filter(Output.workfow_id == wf.id).all()
                #for o in outs:
                #    o.workfow_id = new_wf.id
                #    print o.datasetname,"got",new_wf.name
                #    session.commit()

                for tr in session.query(Transfer).all():
                    if wf.id in tr.workflows_id:
                        sw = copy.deepcopy(tr.workflows_id)
                        sw.remove( wf.id)
                        sw.append(new_wf.id)
                        tr.workflows_id = sw
                        print tr.phedexid,"got",new_wf.name
                        if new_wf.status != 'away':
                            new_wf.status = 'staging'
                        session.commit()
                        

        wf.status = 'forget'
        session.commit()
        
    ## for those already in, find clones or acdcs
    """
    aways = [wf.name for wf in session.query(Workflow).filter(Workflow.status == 'away').all()]
    for away in aways:
        wl = getWorkLoad(url, away)
        familly = getWorkflowById( url, wl['PrepID'] )
        if talk:
            print away,"has",len(familly)
        for member in familly:
            if member != away and member not in existing:
                print "putting",member
                new_wf = Workflow( name = member, status = 'away', wm_status = wl['RequestStatus'])
                session.add( new_wf )   
        session.commit()

    """

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    if len(sys.argv)>1:
        injector(url, sys.argv[1], sys.argv[2])
    else:
        injector(url)

    htmlor()
    
