from assignSession import *
import time

for wf in session.query(Workflow).filter(Workflow.status=='considered').all():
    print "workflow next in line to transfer:",wf.name
print "\n"

for wf in session.query(Workflow).filter(Workflow.status=='staging').all():
    print "workflow held for staging:",wf.name
print "\n"

for wf in session.query(Workflow).filter(Workflow.status=='staged').all():
    print "workflow next in line to assign:",wf.name
print "\n"

for wf in session.query(Workflow).filter(Workflow.status=='away').all():
    print "workflow on-going:",wf.name
print "\n"

for out in session.query(Output).all():
    if  out.workflow.status == 'away':
        print "%150s %d/%d = %3.2f%% %s %s "%(out.datasetname,
                                              out.nlumis,
                                              out.expectedlumis,
                                              out.nlumis/float(out.expectedlumis)*100.,
                                              out.workflow.name,
                                              out.workflow.status)
print "\n"

for ts in session.query(Transfer).all():
    print ts.phedexid,"serves"
    should_clean = True
    for pid in ts.workflows_id:
        w = session.query(Workflow).get(pid)
        if w.status in ['done','forget']:
            # not so easy, as you need to navigate back to prepid
            should_clean=False
        print "\t",w.name,w.status
print "\n"

for out in session.query(Output).all():
    if  out.workflow.status == 'done':
        print "%150s done on week %s"%(out.datasetname,time.strftime("%W (%x %X)",time.gmtime(out.date)))
print "\n"
