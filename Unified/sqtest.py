from assignSession import *
import time
from utils import getWorkLoad, checkTransferStatus, workflowInfo
import pprint
import sys

url = 'cmsweb.cern.ch'

#wfi = workflowInfo( url, "pdmvserv_HIG-2019GEMUpg14DR-00116_00086_v0__150330_112405_8526")
wfi = workflowInfo( url, "pdmvserv_task_B2G-RunIIWinter15wmLHE-00001__v1_T_150402_161327_2265")
print wfi.getLumiWhiteList()

#print wfi.acquisitionEra()

#pid = sys.argv[1]
#tr = session.query(Transfer).filter(Transfer.phedexid== pid).first()
#for wfid in tr.workflows_id:
#    wf = session.query(Workflow).get(wfid)
#    print wf.id,wf.name
#    wf.status = 'staging'
#session.commit()

#checks = checkTransferStatus( 'cmsweb.cern.ch',440053 )
#print checks

#tr = session.query(Transfer).filter(Transfer.phedexid== 440100 ).first()
#session.delete( tr )
#session.commit()

#wf = session.query(Workflow).filter(Workflow.name=="pdmvserv_TOP-Summer12DR53X-00302_00379_v0__150331_100923_4420"
#                                    ).first()

#wl = getWorkLoad( 'cmsweb.cern.ch', wf.name )
#pprint.pprint( wl )
#wf.status = 'staging'
#session.commit()

#for out in session.query(Output).all():
#    if  out.workflow.status == 'done':
#        print "%150s on week %s"%(out.datasetname,time.strftime("%W (%x %X)",time.gmtime(out.date)))

#for out in session.query(Output).all():
#    print out.status

#print "\n\n"
#for out in session.query(Output).all():
#    if  out.workflow.status == 'away':
#        print "%150s %d/%d = %3.2f%% %s %s "%(out.datasetname,
#                                              out.nlumis,
#                                              out.expectedlumis,
#                                              out.nlumis/float(out.expectedlumis)*100.,
#                                              out.workflow.name,
#                                              out.workflow.status)
    
#print "\n\n"

#for wf in session.query(Workflow).filter(Workflow.name=='pdmvserv_TAU-2019GEMUpg14DR-00038_00084_v0__150329_162031_7993').all():
#

"""
for wf in session.query(Workflow).filter(Workflow.status == 'considered').all(): 
    print "workflow:",wf.name,wf.id,wf.status,wf.wm_status   

"""

"""
existing = [wf.name for wf in session.query(Workflow).all()]
take_away=[]
for each in existing:
    if existing.count(each)!=1:
        print each
        take_away.append(each)

for each in take_away:
    for (i,wf) in enumerate(session.query(Workflow).filter(Workflow.name==each).all()):
        if i:
            session.delete( wf )
            session.commit()
"""
