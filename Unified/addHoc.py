#!/usr/bin/env python  
from assignSession import *
import os


for wfo in session.query(Workflow).filter(Workflow.status == 'assistance-biglumi').all():
    if 'Summer15' in wfo.name:
        os.system('Unified/rejector.py --clone %s'% wfo.name)
    
