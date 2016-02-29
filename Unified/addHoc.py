#!/usr/bin/env python  
from utils import workflowInfo, getWorkflows, sendEmail, componentInfo
from assignSession import *
import reqMgrClient
import os
import sys
import json

url = 'cmsweb.cern.ch'

## all dqmharvest completed to announced right away
wfs = getWorkflows(url, 'completed', user=None, rtype='DQMHarvest')
for wf in wfs: reqMgrClient.closeOutWorkflow(url, wf)
wfs = getWorkflows(url, 'closed-out', user=None, rtype='DQMHarvest')
for wf in wfs: reqMgrClient.announceWorkflow(url, wf)

up = componentInfo(mcm=False, soft=['mcm'])                                 
if not up.check():  
    sys.exit(1)     

### catch unrunnable recoveries
not_runable_acdc=set()
wfs = getWorkflows(url, 'acquired', user=None, rtype='Resubmission',details=True)
for wf in wfs:
    wfi = workflowInfo( url , wf['RequestName'], request=wf)
    locs = wfi.getGQLocations()
    wl = set(wfi.request['SiteWhitelist'])
    for wqe,where in locs.items():
        ok = wl & set(where)
        if not ok :
            print "WQE will not run in",wf['RequestName']
            #print list(wl),"does not contain",list(where)
            print "Withlist does not contain",list(where)
            not_runable_acdc.add( wf['RequestName'] )

if not_runable_acdc:
    sendEmail('not runnable ACDCs','These %s ACDC cannot run \n%s'%( len(not_runable_acdc), '\n'.join(not_runable_acdc)), destination = ['jen_a@fnal.gov'])

### add the value of the delay to announcing datasets
data = json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/announce_delays.json').read())
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
open('/afs/cern.ch/user/c/cmst2/www/unified/announce_delays.json','w').write( json.dumps(data, indent=2) )


#os.system('Unified/assignor.py --go RunIIFall15MiniAODv2 --limit 50')

#print "nothing add-Hoc to be done"
sys.exit(0)


"""
os.system('Unified/equalizor.py -a pdmvserv_SUS-RunIISummer15GS-00049_00173_v0__151222_121539_4448')
os.system('Unified/equalizor.py -a pdmvserv_SUS-RunIISummer15GS-00058_00173_v0__151222_121515_6269')
os.system('Unified/equalizor.py -a pdmvserv_SUS-RunIIWinter15GS-00160_00183_v0__151222_121840_6707')
os.system('Unified/equalizor.py -a  pdmvserv_HIG-RunIISummer15GS-00937_00138_v0__151117_201115_6260')
os.system('Unified/equalizor.py -a  vlimant_HIG-RunIISummer15GS-00935_00138_v0__151223_004144_9025')
os.system('Unified/equalizor.py -a  pdmvserv_HIG-RunIISummer15GS-00073_00169_v0__151217_161512_9524')
os.system('Unified/equalizor.py -a  pdmvserv_EXO-RunIISummer15GS-04765_00148_v0__151204_202120_8377 ')
os.system('Unified/equalizor.py -a  pdmvserv_EXO-RunIISummer15GS-04784_00149_v0__151204_202355_4639 ')
os.system('Unified/equalizor.py -a vlimant_HIG-RunIISummer15GS-01015_00152_v0__151223_191655_2771 ')
os.system('Unified/equalizor.py -a pdmvserv_SUS-RunIISummer15GS-00003_00173_v0__151222_121430_3443')
os.system('Unified/equalizor.py -a pdmvserv_task_TSG-RunIIFall15DR76-00002__v1_T_151118_012230_2147')
os.system('Unified/equalizor.py -a jbadillo_TOP-Summer11LegDR-00039_00062_v0__151216_131228_9019')
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
    
