#!/usr/bin/env python
import sys
import os

import optparse

parser = optparse.OptionParser()
parser.add_option('--go',default=False, action = 'store_true', help='Answer yes to all')
parser.add_option('--workflows',help='Coma separated list of workflows to recover')
def_assignoptions = '--site acdc --xrootd'
parser.add_option('--assignoptions',default=def_assignoptions, help='The options to pass to assign.py (default is %s)'%def_assignoptions)
(options,args) = parser.parse_args()

wfs=options.workflows.split(',')

for wf in wfs:
    makeall = os.popen('./makeACDC.py --all -w %s'% wf).read()
    acdcs = []
    for s in [l.split() for l in makeall.split('\n')]:
        #print s
        if len(s)!=3: continue
        if s[1]!='for' : continue
        acdcs.append( s[0] )
    print acdcs
    #sec='--secondary_x' if 'RunIISummer16DR80Premix' in wf else ''
    sec=''
    for acdc in acdcs:
        os.system('./assign.py %s -w %s %s'%(options.assignoptions, acdc, sec))
        pass
