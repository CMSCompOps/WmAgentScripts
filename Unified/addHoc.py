#!/usr/bin/env python  
from assignSession import *
import os

print "nothing add-Hoc to be done"

for wfo in session.query(Workflow).filter(Workflow.status == 'assistance-manual').all():
    if 'EXO-RunIIWinter15wmLHE' in wfo.name:
        print "could reject it but would need the dsn"

#for wfo in session.query(Workflow).filter(Workflow.status == 'assistance-biglumi').all():
#    if 'Summer15' in wfo.name:
#        os.system('Unified/rejector.py --clone %s'% wfo.name)
    
