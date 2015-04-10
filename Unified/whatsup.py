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

