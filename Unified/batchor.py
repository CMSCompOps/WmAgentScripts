#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, sendEmail, sendLog, monitor_pub_dir, unifiedConfiguration, deep_update, global_SI, getWorkflowByCampaign, base_eos_dir, monitor_dir, eosRead, eosFile, campaignInfo, batchInfo
from collections import defaultdict
import copy
import json
import os
import random

def batchor( url ):
    UC = unifiedConfiguration()
    SI = global_SI()
    CI = campaignInfo()
    BI = batchInfo()
    ## get all workflows in assignment-approved with SubRequestType = relval
    all_wfs = []
    for user in UC.get("user_relval"):
        all_wfs.extend( getWorkflows(url, 'assignment-approved', details=True, user=user, rtype='TaskChain') )

    wfs = filter( lambda r :r['SubRequestType'] == 'RelVal' if 'SubRequestType' in r else False, all_wfs)
    ## need a special treatment for those
    hi_wfs = filter( lambda r :r['SubRequestType'] == 'HIRelVal' if 'SubRequestType' in r else False, all_wfs)

    by_campaign = defaultdict(set)
    by_hi_campaign = defaultdict(set)
    for wf in wfs:
        print "Relval:",wf['RequestName'], wf['Campaign']
        by_campaign[wf['Campaign']].add( wf['PrepID'] )


    for wf in hi_wfs:
        print "HI Relval:",wf['RequestName'], wf['Campaign']
        by_hi_campaign[wf['Campaign']].add( wf['PrepID'] )
        
    default_setup = {
        "go" :True,
        "parameters" : {
            "SiteWhitelist": [ "T1_US_FNAL" ],
            "MergedLFNBase": "/store/relval",
            "Team" : "relval",
            "NonCustodialGroup" : "RelVal"
            },
        "custodial_override" : "notape",
        "phedex_group" : "RelVal",
        "lumisize" : -1,
        "fractionpass" : 0.0,
        "maxcopies" : 1
        }
    default_hi_setup = copy.deepcopy( default_setup )

    add_on = {}
    relval_routing = UC.get('relval_routing')
    def pick_one_site( p):
        ## modify the parameters on the spot to have only one site
        if "parameters" in p and "SiteWhitelist" in p["parameters"] and len(p["parameters"]["SiteWhitelist"])>1:
            choose_from = list(set(p["parameters"]["SiteWhitelist"]) & set(SI.sites_ready))
            picked = random.choice( choose_from )
            print "picked",picked,"from",choose_from
            p["parameters"]["SiteWhitelist"] = [picked]
            
    batches = BI.all()
    for campaign in by_campaign:
        if campaign in batches: continue
        ## get a bunch of information
        setup  = copy.deepcopy( default_setup )

        for key in relval_routing:
            if key in campaign:
                ## augment with the routing information
                augment_with = relval_routing[key]
                print "Modifying the batch configuration because of keyword",key
                print "with",augment_with
                setup = deep_update( setup, augment_with )

        pick_one_site( setup )
        add_on[campaign] = setup
        sendLog('batchor','Adding the relval campaigns %s with parameters \n%s'%( campaign, json.dumps( setup, indent=2)),level='critical')
        BI.update( campaign, by_campaign[campaign])

    for campaign in by_hi_campaign:
        if campaign in batches: continue
        ## get a bunch of information
        setup  = copy.deepcopy( default_hi_setup )
        possible_sites = set(["T1_DE_KIT","T1_FR_CCIN2P3"])
        hi_site = random.choice(list(possible_sites))
        setup["parameters"]["SiteWhitelist"]=[ hi_site ]

        pick_one_site( setup )
        add_on[campaign] = setup
        sendLog('batchor','Adding the HI relval campaigns %s with parameters \n%s'%( campaign, json.dumps( setup, indent=2)),level='critical')
        BI.update( campaign, by_hi_campaign[campaign])
        
    

    ## only new campaigns in announcement
    for new_campaign in list(set(add_on.keys())-set(CI.all(c_type='relval'))):
        ## this is new, and can be announced as such
        print new_campaign,"is new stuff"
        subject = "Request of RelVal samples batch %s"% new_campaign
        text="""Dear all, 
A new batch of relval workflows was requested.

Batch ID:

%s

Details of the workflows:

https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?campaign=%s

This is an automated message"""%( new_campaign, 
                                  new_campaign,
                                  )


        print subject
        print text
        to = ['hn-cms-dataopsrequests@cern.ch']
        sendEmail(subject, text, destination=to)
        sendLog('batchor',text, level='critical')

    ## go through all existing campaigns and remove the ones not in use anymore ?
    for old_campaign in CI.all(c_type='relval'):
        all_in_batch = getWorkflowByCampaign(url, old_campaign, details=True)
        if not all_in_batch: continue
        is_batch_done = all(map(lambda s : not s in ['completed','force-complete','running-open','running-closed','acquired','assigned','assignment-approved'], [wf['RequestStatus']for wf in all_in_batch]))
        ## check all statuses
        if is_batch_done:
            #print "batch",old_campaign,"can be closed or removed if necessary"
            #campaigns[old_campaign]['go'] = False ## disable
            CI.pop( old_campaign ) ## or just drop it all together ?
            BI.pop( old_campaign )
            print "batch",old_campaign," configuration was removed"

    ## merge all anyways
    CI.update( add_on , c_type = 'relval')

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'

    batchor(url)
