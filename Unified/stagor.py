from assignSession import *
from utils import checkTransferStatus, checkTransferApproval, approveSubscription, getWorkflowByInput
import sys
import itertools
import pprint
def stagor(url,specific =None, good_enough = 99.9):
    done_by_wf_id = {}
    done_by_input = {}
    for transfer in session.query(Transfer).all():
        if specific  and str(transfer.phedexid)!=str(specific): continue

        skip=True
        for wfid in transfer.workflows_id:
            tr_wf = session.query(Workflow).get(wfid)
            if tr_wf: 
                if tr_wf.status == 'staging':
                    skip=False
                    break

        if skip: continue
        if transfer.phedexid<0: continue

        ## check the status of transfers
        checks = checkTransferApproval(url,  transfer.phedexid)
        approved = all(checks.values())
        if not approved:
            print transfer.phedexid,"is not yet approved"
            approveSubscription(url, transfer.phedexid)
            continue

        ## check on transfer completion
        checks = checkTransferStatus(url, transfer.phedexid)

        if not specific:
            for dsname in checks:
                if not dsname in done_by_input: done_by_input[dsname]={}
                done_by_input[dsname][transfer.phedexid]=all(map(lambda i:i>good_enough, checks[dsname].values()))
        if checks:
            done = all(map(lambda i:i>good_enough,list(itertools.chain.from_iterable([node.values() for node in checks.values()]))))
        else:
            ## it is empty, is that a sign that all is done and away ?
            print "ERROR with the scubscriptions API"
            done = False

        ## the thing above is NOT giving the right number
        #done = False

        for wfid in transfer.workflows_id:
            tr_wf = session.query(Workflow).get(wfid)
            if tr_wf:# and tr_wf.status == 'staging':  
                if not tr_wf.id in done_by_wf_id: done_by_wf_id[tr_wf.id]={}
                done_by_wf_id[tr_wf.id][transfer.phedexid]=done


        if done:
            ## transfer.status = 'done'
            print transfer.phedexid,"is done"
        else:
            print transfer.phedexid,"not finished"
            pprint.pprint( checks )

    #print done_by_input
    print "\n----\n"
    for dsname in done_by_input:
        if all(done_by_input[dsname].values()):
            print dsname,"is everywhere we wanted"
            ## the input dataset is fully transfered, should consider setting the corresponding wf to staged
            using_its = getWorkflowByInput(url, dsname)
            #print using_its
            for using_it in using_its:
                wf = session.query(Workflow).filter(Workflow.name == using_it).first()
                if wf and wf.status == 'staging':
                    print wf.name,"is with us. setting staged and move on"
                    wf.status = 'staged'
                    session.commit()
        else:
            print dsname,done_by_input[dsname]

    for wfid in done_by_wf_id:
        #print done_by_wf_id[wfid].values()
        ## ask that all related transfer get into a valid state
        if all(done_by_wf_id[wfid].values()):
            pass
            #tr_wf = session.query(Workflow).get(wfid)
            #print "setting",tr_wf.name,"to staged"
            #tr_wf.status = 'staged'
            #session.commit()

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec=None
    if len(sys.argv)>1:
        spec = sys.argv[1]

    stagor(url, spec)
