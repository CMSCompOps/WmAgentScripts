#!/usr/bin/env python
import sys
import os
from utils import getWorkflowById, reqmgr_url, workflowInfo
import optparse

parser = optparse.OptionParser()
parser.add_option('--go',default=False, action = 'store_true', help='Answer yes to all')
parser.add_option('--dry', default=False, action = 'store_true', help='Just show all')
parser.add_option('--workflows',help='Coma separated list of workflows to recover')
def_assignoptions = '--site acdc --xrootd'
parser.add_option('--assignoptions',default=def_assignoptions, help='The options to pass to assign.py (default is %s)'%def_assignoptions)
parser.add_option('--nocreation', default=False, action = 'store_true', help='Do not inject new request')
(options,args) = parser.parse_args()

wfs=options.workflows.split(',')

for wf in wfs:
    acdcs = []

    if options.nocreation:
        wfi = workflowInfo( reqmgr_url, wf)
        pid = wfi.request['PrepID']
        familly = filter(lambda r : r['RequestStatus'] == 'assignment-approved' and 'ACDC' in r['RequestName'],
                         getWorkflowById( reqmgr_url, pid, details=True))
        acdcs = [r['RequestName'] for r in familly]
    else:
        com = './makeACDC.py --all -w %s'% wf
        print com 
        y = (raw_input('go ?') if not options.dry else 'dry') if not options.go else 'y'
        if y.lower() in ['y','yes','go','ok']:
            makeall = os.popen('./makeACDC.py --all -w %s'% wf).read()
            for s in [l.split() for l in makeall.split('\n')]:
                if len(s)!=3: continue
                if s[1]!='for' : continue
                acdcs.append( s[0] )
    print acdcs
    #sec='--secondary_x' if 'RunIISummer16DR80Premix' in wf else ''
    sec=''
    for acdc in acdcs:
        com = './assign.py %s -w %s %s'%(options.assignoptions, acdc, sec)
        y = (raw_input('go ?') if not options.dry else 'dry') if not options.go else 'y'
        if y.lower() in ['y','yes','go','ok']:        
            os.system('./assign.py %s -w %s %s'%(options.assignoptions, acdc, sec))

