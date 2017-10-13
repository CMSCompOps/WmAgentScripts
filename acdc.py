#!/usr/bin/env python
import sys
import os

wfs=sys.argv[1].split(',')

for wf in wfs:
    makeall = os.popen('./makeACDC.py --all -w %s'% wf).read()
    acdcs = []
    for s in [l.split() for l in makeall.split('\n')]:
        #print s
        if len(s)!=3: continue
        if s[1]!='for' : continue
        acdcs.append( s[0] )
    print acdcs
    sec='--secondary_x' if 'RunIISummer16DR80Premix' in wf else ''
    for acdc in acdcs:
        os.system('./assign.py -s acdc -w %s %s'%(acdc, sec))
        pass
