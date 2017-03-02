#!/usr/bin/env python  
from utils import workflowInfo, getWorkflows, sendEmail, componentInfo, monitor_dir, reqmgr_url, siteInfo, sendLog, getWorkflowById
from assignSession import *
import reqMgrClient
import os
import sys
import json
import time



#os.system('Unified/assignor.py RunIISummer16MiniAODv2')
#os.system('Unified/assignor.py --from_status staging RunIISummer16DR80Premix')
#os.system('Unified/assignor.py --from_status staging RunIISummer16DR80-')

up = componentInfo(mcm=False, soft=['mcm'])                                 
if not up.check(): sys.exit(0)

url = reqmgr_url

may_have_one=set()
may_have_one.update([wfo.name for wfo in session.query(Workflow).filter(Workflow.status.startswith('away')).all()])
may_have_one.update([wfo.name for wfo in session.query(Workflow).filter(Workflow.status.startswith('assistance')).all()])

wfs = []
wfs.extend( getWorkflows(url, 'running-open', details=True))
wfs.extend( getWorkflows(url, 'running-closed', details=True))
wfs.extend( getWorkflows(url, 'completed', details=True))

may_have_one_too = set()
for wf in wfs:
    if wf['RequestName'] in may_have_one:
        #print wf['RequestName'],"and familly"
        may_have_one_too.update( getWorkflowById(url, wf['PrepID']) )
        
may_have_one.update( may_have_one_too )

## keep all relval reports for *ever* ...
batches = json.loads(open('batches.json').read())
for b,wfs in batches.items(): 
    #for wf in wfs: wfi = workflowInfo(url, wf)
    may_have_one.update( wfs )

for logtype in ['report','joblogs','condorlogs']:
    for d in filter(None,os.popen('ls -d %s/%s/*'%(monitor_dir,logtype)).read().split('\n')):
        if not any([m in d for m in may_have_one]):
            ## that can be removed
            print d,"report file can be removed"
            os.system('rm -rf %s'%d)
        else:
            print d,"is still in use"
    
## protected lfn list
os.system('python listProtectedLFN.py')

### dump the knonw thresholds
si = siteInfo()
m = {}
for site in sorted(si.cpu_pledges.keys()):
    print site, si.cpu_pledges[site], int(si.cpu_pledges[site]/2.)
    m[site] = {"running" : si.cpu_pledges[site],
               "pending" : int(si.cpu_pledges[site]/2.)
               }
n = time.gmtime()
m["update"] = time.asctime(n)
m["timestamp"] = time.mktime(n)
open('%s/thresholds.json'%monitor_dir,'w').write(json.dumps( m, indent=2 ))

### convert what we can from taskchain to stepchain ###
#wfs = session.query(Workflow).filter(Workflow.name.startswith('task_')).filter(Workflow.status in ['staging','staged','considered']).all()
#for wfo in wfs:
#    if not wfo.status in ['staging','staged','considered','considered-tried']: continue
#    wfi = workflowInfo(url, wfo.name)
#    if wfi.request['RequestType'] == 'TaskChain':
#        ## go fo the conversion
#        print "Converting",wfo.name,"into step chain ?"
#        #os.system('Unified/rejector.py --clone --to_step %s --comments "Transforming into StepChain for efficiency"'%( wfo.name))
#        #os.system('Unified/injector.py %s'% wfi.request['PrepID'])
#        pass

### all dqmharvest completed to announced right away ###
wfs = getWorkflows(url, 'completed', user=None, rtype='DQMHarvest')
for wf in wfs: 
    print "closing out",wf
    reqMgrClient.closeOutWorkflow(url, wf)
wfs = getWorkflows(url, 'closed-out', user=None, rtype='DQMHarvest')
for wf in wfs: 
    print "announcing",wf
    reqMgrClient.announceWorkflow(url, wf)
wfs = getWorkflows(url, 'failed', user=None, rtype='DQMHarvest')
if len(wfs):
    sendLog('addHoc','There are failed Harvesting requests\n%s'%('\n'.join(sorted( wfs))),level='critical')



### clone all DR80 that end up with issues ###
## 
#for wfo in session.query(Workflow).filter(Workflow.status == 'assistance-manual').all():
#    if not any([c in wfo.name for c in ['RunIISpring16DR80']]): continue
#    wfi = workflowInfo(url, wfo.name)
#    if wfi.getRequestNumEvents() < 500000:
#        ## small workflow that needs recovery : kill-clone
#        os.system('Unified/rejector.py --clone %s'%wfo.name)

"""
### add the value of the delay to announcing datasets
data = json.loads(open('%s/announce_delays.json'%monitor_dir).read())
for wfo in session.query(Workflow).filter(Workflow.status.startswith('done')).all()[:500]:
    if wfo.name in data: continue
    wfi = workflowInfo( url, wfo.name)
    closedout_log = filter(lambda change : change["Status"] in ["closed-out"],wfi.request['RequestTransition'])
    announced_log =  filter(lambda change : change["Status"] in ["announced"],wfi.request['RequestTransition'])
    if not closedout_log or not announced_log:
        print "cannot do shit",wfo.name
        continue
    closedout = closedout_log[-1]['UpdateTime']
    announced = announced_log[-1]['UpdateTime'] 
    delay = announced - closedout
    data[wfo.name] = {
        'closedout' : closedout,
        'announced' : announced,
        'delay' : delay
        }
    print wfo.name,"delay",delay
open('%s/announce_delays.json'%monitor_dir,'w').write( json.dumps(data, indent=2) )
"""

#for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
#    wfi = workflowInfo(url, wfo.name )
#    if wfi.request['RequestPriority'] < 100000 : continue
#    print "forcing acquiring from staging",wfo.name
#    os.system('Unified/assignor.py --go  %s'% wfo.name)

#for wfo in session.query(Workflow).filter(Workflow.status == 'staged').all():
#    wfi = workflowInfo(url, wfo.name )
#    #if wfi.request['RequestPriority'] < 100000 : continue
#    print "forcing acquiring from staged",wfo.name
#    os.system('Unified/assignor.py --go  %s'% wfo.name)



#for wfo in session.query(Workflow).filter(Workflow.status == 'assistance-manual').all():
#    if 'EXO-RunIIWinter15wmLHE' in wfo.name:
#        print "could reject it but would need the dsn"

#for wfo in session.query(Workflow).filter(Workflow.status == 'assistance-biglumi').all():
#    if 'Summer15' in wfo.name:
#        os.system('Unified/rejector.py --clone %s'% wfo.name)
    
