import itertools
import httplib
import os
import json
import time
from collections import defaultdict
from utils import monitor_pub_dir, workflowInfo, getWorkflows

register=[
    #'assigned','acquired',
    'running-open','running-closed','force-complete','completed','closed-out']
wfs = []
url = 'cmsweb.cern.ch'

for r in register:
    wfs.extend( getWorkflows(url, r ,details=True) )
    print len(wfs),"after collecting",r

lfns =defaultdict(set)
for wf in wfs:
    if 'OutputModulesLFNBases' not in wf:
        print wf['RequestName']
    for base in wf['OutputModulesLFNBases']:
        lfns[base].add( wf['RequestName'] )

now = time.gmtime()
content = { "timestamp" : time.mktime(now),
            "date" : time.asctime(now),
            "protected" : sorted(lfns.keys())
            }

open('%s/listProtectedLFN.txt'%monitor_pub_dir,'w').write( json.dumps( content, indent=2))    
#open('%s/listProtectedLFN.txt.2'%monitor_pub_dir,'w').write( json.dumps( content, indent=2))    
#print '\n'.join( sorted(lfns.keys()) )

for lfn in lfns:
    print lfn
    for wf in lfns[lfn]:
        print "\t protected by ",wf
    
