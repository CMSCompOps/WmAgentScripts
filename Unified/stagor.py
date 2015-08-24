#!/usr/bin/env python
from assignSession import *
from utils import checkTransferStatus, checkTransferApproval, approveSubscription, getWorkflowByInput, workflowInfo, getDatasetBlocksFraction, findLostBlocks
from utils import unifiedConfiguration, componentInfo, sendEmail, getSiteWhiteList
from utils import siteInfo, campaignInfo
import json
import sys
import itertools
import pprint
import optparse
from htmlor import htmlor

def stagor(url,specific =None, options=None):
    
    up = componentInfo()
    SI = siteInfo()
    CI = campaignInfo()

    done_by_wf_id = {}
    done_by_input = {}
    completion_by_input = {}
    good_enough = 100.0
    
    if options.fast:
        for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
            if specific and not specific in wfo.name: continue
            wfi = workflowInfo(url, wfo.name)
            sites_allowed = getSiteWhiteList( wfi.getIO() )
            if 'SiteWhitelist' in CI.parameters(wfi.request['Campaign']):
                sites_allowed = CI.parameters(wfi.request['Campaign'])['SiteWhitelist']
            if 'SiteBlacklist' in CI.parameters(wfi.request['Campaign']):
                sites_allowed = list(set(sites_allowed) - set(CI.parameters(wfi.request['Campaign'])['SiteBlacklist']))
            dataset = wfi.request['InputDataset']
            se_allowed = [SI.CE_to_SE(site) for site in sites_allowed] 
            #print se_allowed
            available = getDatasetBlocksFraction( url , dataset , sites=se_allowed )
            if available > options.goodavailability:
                print "\t\t",wfo.name,"can go staged"
                wfo.status = 'staged'
                session.commit()
        return 

    for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
        wfi = workflowInfo(url, wfo.name)
        dataset = wfi.request['InputDataset']
        done_by_input[dataset] = {}
        completion_by_input[dataset] = {}
        print wfo.name,"needs",dataset

    for transfer in session.query(Transfer).all():
        if specific  and str(transfer.phedexid)!=str(specific): continue

        skip=True
        for wfid in transfer.workflows_id:
            tr_wf = session.query(Workflow).get(wfid)
            if tr_wf: 
                if tr_wf.status == 'staging':
                    print "\t",transfer.phedexid,"is staging for",tr_wf.name
                    skip=False

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
        checks = checkTransferStatus(url, transfer.phedexid, nocollapse=True)

        if not specific:
            for dsname in checks:
                if not dsname in done_by_input: done_by_input[dsname]={}
                if not dsname in completion_by_input: completion_by_input[dsname] = {}
                done_by_input[dsname][transfer.phedexid]=all(map(lambda i:i>=good_enough, checks[dsname].values()))
                completion_by_input[dsname][transfer.phedexid]=checks[dsname].values()
        if checks:
            print "Checks for",transfer.phedexid,[node.values() for node in checks.values()]
            done = all(map(lambda i:i>=good_enough,list(itertools.chain.from_iterable([node.values() for node in checks.values()]))))
        else:
            ## it is empty, is that a sign that all is done and away ?
            print "ERROR with the scubscriptions API of ",transfer.phedexid
            print "Most likely something else is overiding the transfer request. Need to work on finding the replacement automatically, if the replacement exists"
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
        fractions = None
        if dsname in completion_by_input:
            fractions = itertools.chain.from_iterable([check.values() for check in completion_by_input.values()])
        
        ## the workflows in the waiting room for the dataset
        using_its = getWorkflowByInput(url, dsname)
        #print using_its
        using_wfos = []
        for using_it in using_its:
            wf = session.query(Workflow).filter(Workflow.name == using_it).first()
            if wf:
                using_wfos.append( wf )
        
        if not len(done_by_input[dsname]):
            print "For dataset",dsname,"there are no transfer report. That's an issue."
            for wf in using_wfos:
                if wf.status == 'staging':
                    print "sending",wf.name,"back to considered"
                    wf.status = 'considered'
                    session.commit()
                    sendEmail( "send back to considered","%s was send back and might be trouble"% wf.name)
            continue

        #need_sites = int(len(done_by_input[dsname].values())*0.7)+1
        need_sites = len(done_by_input[dsname].values())
        if need_sites > 10:
            need_sites = int(need_sites/2.)
        got = done_by_input[dsname].values().count(True)
        if all([wf.status != 'staging' for wf in using_wfos]):
            ## not a single ds-using wf is in staging => moved on already
            ## just forget about it
            print "presence of",dsname,"does not matter anymore"
            print "\t",done_by_input[dsname]
            print "\t",[wf.status for wf in using_wfos]
            print "\tneeds",need_sites
            continue #??
            
        ## should the need_sites reduces with time ?
        # with dataset choping, reducing that number might work as a block black-list.

        if len(done_by_input[dsname].values()) and all(done_by_input[dsname].values()):
            print dsname,"is everywhere we wanted"
            ## the input dataset is fully transfered, should consider setting the corresponding wf to staged
            for wf in using_wfos:
                if wf.status == 'staging':
                    print wf.name,"is with us. setting staged and move on"
                    wf.status = 'staged'
                    session.commit()
        elif fractions and len(list(fractions))>1 and set(fractions)==1:
            print dsname,"is everywhere at the same fraction"
            print "We do not want this in the end. we want the data we asked for"
            continue
            ## the input dataset is fully transfered, should consider setting the corresponding wf to staged
            for wf in using_wfos:
                if wf.status == 'staging':
                    print wf.name,"is with us everywhere the same. setting staged and move on"
                    wf.status = 'staged'
                    session.commit()
        elif got >= need_sites:
            print dsname,"is almost everywhere we wanted"
            #print "We do not want this in the end. we want the data we asked for"
            #continue
            ## the input dataset is fully transfered, should consider setting the corresponding wf to staged
            for wf in using_wfos:
                if wf.status == 'staging':
                    print wf.name,"is almost with us. setting staged and move on"
                    wf.status = 'staged'
                    session.commit()
        else:
            print dsname
            lost = findLostBlocks(url, dsname)
            known_lost = ['/ReggeGribovPartonMC_13TeV-QGSJetII/RunIIWinter15GS-castor_MCRUN2_71_V0_ext1-v1/GEN-SIM',
                          '/BprimeBprime_M-1300_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
                          'QCD_Pt-15to7000_TuneCUETP8M1_Flat_13TeV_pythia8/RunIIWinter15GS-magnetOff_MCRUN2_71_V1-v1/GEN-SIM',
                          '/ReggeGribovPartonMC_13TeV-QGSJetII/RunIIWinter15GS-castor_MCRUN2_71_V0_ext1-v1/GEN-SIM',
                          ]
            if lost and not dsname in known_lost:
                lost_names = [item['name'] for item in lost]
                print "We have lost",len(lost),"blocks",lost_names
                #print json.dumps( lost , indent=2 )
                sendEmail('we have lost a few blocks', str(len(lost))+" in total.\nDetails \n:"+json.dumps( lost , indent=2 ))
            print "\t",done_by_input[dsname]
            print "\tneeds",need_sites
            print "\tgot",got

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
    UC = unifiedConfiguration()
    parser = optparse.OptionParser()
    parser.add_option('-f','--fast', help='Make a quick check for available fraction',default=False,action='store_true')
    parser.add_option('-g','--goodavailability', help='The threshold on available fraction',default=UC.get('fast_stagor_pass_availability'),type=float)
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    stagor(url, spec, options)
    htmlor()
