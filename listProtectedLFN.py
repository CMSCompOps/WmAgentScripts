import itertools
import httplib
import os
import json
import time
from collections import defaultdict
from utils import monitor_pub_dir, workflowInfo

register=['assigned','acquired','running-open','running-closed','force-complete','completed','closed-out']
wfs = []
url = 'cmsweb.cern.ch'

def getWorkflows( status ):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    go_to = '/reqmgr2/data/request?status=%s&detail=true'%status
    r1=conn.request("GET",go_to, headers={"Accept":"application/json"})        
    r2=conn.getresponse()
    data = json.loads(r2.read())
    items = data['result']
    r=[]
    for item in items:
        r.extend( [item[k] for k in item.keys()] )
    return r

for r in register:
    wfs.extend( getWorkflows( r ) )
    print len(wfs),"after collecting",r

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
lfns =defaultdict(set)
for wf in wfs:
    wfi = workflowInfo( url , wf['RequestName'] , request=wf)

    help = WMWorkloadHelper( wfi.full_spec )

    for task in help.getAllTasks():
        for step in task.listAllStepNames():
            o = task.getOutputModulesForStep( step )
            for name,module in o.dictionary_().items():
                if hasattr(module, "lfnBase"):
                    base = getattr(module,"lfnBase")
                    if 'unmerged' in base:
                        lfns[base].add( wf['RequestName'] )
    continue                    
    #outs = wf['OutputDatasets']
    #base=wf.get('UnmergedLFNBase','/store/unmerged')
    #for out in outs:
    #    _,dsn,ps,tier = out.split('/')
    #    acq,rest = ps.split('-',1)
    #    print wf['RequestName'],wf['RequestStatus']
    #    d = '/'.join( [ base, acq, dsn, tier, rest] )
    #    print d
    #    lfns[d].add( wf['RequestName'] )
        
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
    
