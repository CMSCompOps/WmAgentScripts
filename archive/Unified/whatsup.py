#!/usr/bin/env python
from assignSession import *
import time
from utils import workflowInfo

for wf in session.query(Workflow).filter(Workflow.status=='considered').all():
    print "workflow next in line to transfer:",wf.name
print "\n"

for wf in session.query(Workflow).filter(Workflow.status=='staging').all():
    wfi = workflowInfo('cmsweb.cern.ch',wf.name,spec=False)
    print "workflow held for staging:",wf.name,wfi.request['RequestPriority']
print "\n"

for wf in session.query(Workflow).filter(Workflow.status=='staged').all():
    print "workflow next in line to assign:",wf.name
print "\n"

