from assignSession import *
url='cmsweb.cern.ch'

for wf in session.query(Workflow).filter(Workflow.status=='trouble').all():
    wl = getWorkLoad(url, wf.name)
    pid = wl['PrepID']
    familly = getWorkflowById( url, wl['PrepID'] )
    
    replacement = session.query(Workflow
    outs = session.query(Output).filter(Output.workflow_id == wf.id).all()
    for o in outs:
        
