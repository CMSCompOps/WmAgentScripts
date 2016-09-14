#!/usr/bin/env python  
from utils import workflowInfo, getWorkflows, sendEmail, componentInfo, monitor_dir, reqmgr_url, newLockInfo, siteInfo, sendLog
from assignSession import *
import reqMgrClient
import os
import sys
import json
import time

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
open('/afs/cern.ch/user/c/cmst2/www/unified/thresholds.json','w').write(json.dumps( m, indent=2 ))

### remove site from whitelist

banned= ['T2_US_Vanderbilt']
old=json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/equalizor.json').read())
for wfn in ['vlimant_BPH-RunIISummer15GS-Backfill-00030_00212_v0__160906_122234_1944',
            'vlimant_BPH-RunIISummer15GS-Backfill-00030_00212_v0__160907_112626_808',
            'pdmvserv_HIG-RunIISummer15wmLHEGS-00418_00157_v0__160909_001621_321',
            'pdmvserv_HIG-RunIISummer15wmLHEGS-00420_00157_v0__160909_001612_2018',
            'pdmvserv_HIG-RunIISummer15wmLHEGS-00415_00157_v0__160909_001628_2566'
            ]:
    wfi = workflowInfo(url, wfn)
    new_sites = sorted(list((set(wfi.request['SiteWhitelist']) - set(banned)) & set(si.sites_ready)))
    for task in wfi.getWorkTasks():
        if task.taskType in 'Production':
            
            bit={wfn : { task.pathName : {"ReplaceSiteWhitelist" : new_sites
                                          }}}
            print json.dumps( bit, indent=2)
            old["modifications"].update( bit )
open('/afs/cern.ch/user/c/cmst2/www/unified/equalizor.json','w').write( json.dumps( old, indent=2))

### manually lock some dataset ### not bullet proof
#nl = newLockInfo()
#nl.lock('/Neutrino_E-10_gun/RunIISpring15PrePremix-AVE_25_BX_25ns_76X_mcRun2_asymptotic_v12-v3/GEN-SIM-DIGI-RAW')
#nl.lock('/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer15GS-MCRUN2_71_V1_ext1-v2/GEN-SIM')


### convert what we can from taskchain to stepchain ###
wfs = session.query(Workflow).filter(Workflow.name.startswith('task_')).filter(Workflow.status in ['staging','staged','considered']).all()
for wfo in wfs:
    if not wfo.status in ['staging','staged','considered','considered-tried']: continue
    wfi = workflowInfo(url, wfo.name)
    if wfi.request['RequestType'] == 'TaskChain':
        ## go fo the conversion
        print "Converting",wfo.name,"into step chain ?"
        #os.system('Unified/rejector.py --clone --to_step %s --comments "Transforming into StepChain for efficiency"'%( wfo.name))
        #os.system('Unified/injector.py %s'% wfi.request['PrepID'])
        pass

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


up = componentInfo(mcm=False, soft=['mcm'])                                 
if not up.check():  
    sys.exit(1)     


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
    
