#!/usr/bin/env python  
from utils import workflowInfo, getWorkflows, sendEmail, componentInfo, monitor_dir, reqmgr_url, siteInfo, sendLog, getWorkflowById, agentInfo, unifiedConfiguration, monitor_eos_dir, base_eos_dir, batchInfo, reportInfo

from assignSession import *
import reqMgrClient
import os
import sys
import json
import time
import random
from JIRAClient import JIRAClient

up = componentInfo(soft=['mcm','wtc','jira'])
if not up.check(): sys.exit(0)

JC = JIRAClient() if up.status.get('jira',False) else None
if JC:
    those = JC.find({'status' : '!CLOSED'})
    for t in those:
        s= t.fields.summary
        s = s.replace('issues','')
        s = s.strip()
        if s.count(' ')!=0: continue
        print s
        wfs = getWorkflowById(reqmgr_url, s, details=True)
        statuses = set([r['RequestStatus'] for r in wfs])
        check_against = ['assignment-approved', 'running-open','running-closed','completed','acquired', 'staging', 'staged', 'assigned', 'closed-out', 'failed']
        if statuses:
            if all([s not in check_against for s in statuses]):
                print t.key,"can be closed"
                print statuses
                JC.close(t.key) ## uncomment to close JIRAs
                continue
        print t.key,statuses

UC = unifiedConfiguration()

url = reqmgr_url

wfs = getWorkflows(url, 'assigned', details=True)

now = time.mktime( time.gmtime())
for wf in wfs:
    assigned_log = filter(lambda change : change["Status"] in ["assigned"],wf['RequestTransition'])
    if assigned_log:
        then = assigned_log[-1]['UpdateTime']
        since = (now-then)/float(1*24*60*60.)
        if since>1.:
            print "workflow",wf['RequestName'],"is assigned since",then," that is",since,"days"
            sendLog('GQ','The workflow %s has been assigned for %.2f days'%(wf['RequestName'], since), level='critical')

overall_timeout = 14 #days
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
batches = batchInfo().content()
for b,pids in batches.items(): 
    for pid in pids:
        wfs = getWorkflowById(url, pid, details=True)
        for wf in wfs:
            ## check on the announce date
            announced = filter(lambda o : o['Status']in ['announced','rejected','aborted'], wf['RequestTransition']) ## check on any final state
            if announced:
                announced_time = max([a['UpdateTime'] for a in announced])
                if (now-announced_time) < (7*24*60*60):
                    ## less than 7 days announced
                    may_have_one.add( wf['RequestName'] )
            else:
                may_have_one.add( wf['RequestName'] )

print "wf that can have logs"
print '\n'.join(sorted(may_have_one))

RI = reportInfo()
RI.purge( grace = 30 ) 

for (the_dir,logtype) in [(monitor_eos_dir,'report'),
                          (monitor_dir,'report'),
                          (monitor_eos_dir,'joblogs'),
                          (monitor_eos_dir,'condorlogs')]:
    #for d in filter(None,os.popen('ls -d %s/%s/*'%(monitor_dir,logtype)).read().split('\n')):
    #for d in filter(None,os.popen('ls -d %s/%s/*'%(the_dir,logtype)).read().split('\n')):
    for d in filter(None,os.popen('find %s/%s/ -maxdepth 1 -type d -mtime +%d  '%( the_dir,logtype, overall_timeout)).read().split('\n')):
        is_locked = any([d.endswith(wf) for wf in may_have_one])
        if not is_locked:
            ## that can be removed
            print ("Removing {}".format(logtype))
            cmd = "rm -rf {}".format(d)
            print(cmd)
            os.system(cmd)
            eos_cmd = "eos rm -rf {}".format(d)
            print(eos_cmd)
            #print "with eos command"
            os.system(eos_cmd)
        else:
            #print d,"is still in use"
            pass

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
    
