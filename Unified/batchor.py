#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, sendEmail, sendLog, monitor_pub_dir, unifiedConfiguration
from collections import defaultdict
import copy
import json
import os
import random

def batchor( url ):
    UC = unifiedConfiguration()
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
        by_campaign[wf['Campaign']].add( wf['RequestName'] )
    for wf in hi_wfs:
        print "HI Relval:",wf['RequestName'], wf['Campaign']
        by_hi_campaign[wf['Campaign']].add( wf['RequestName'] )
        
    default_setup = {
        "go" :True,
        "parameters" : {
            "SiteWhitelist": [ "T1_US_FNAL" ],
            "MergedLFNBase": "/store/relval",
            "Team" : "relval",
            "NonCustodialGroup" : "RelVal"
            },
        "custodial" : "T1_US_FNAL_MSS",
        "phedex_group" : "RelVal",
        "lumisize" : -1,
        "fractionpass" : 0.0,
        "maxcopies" : 1
        }
    default_hi_setup = copy.deepcopy( default_setup )

    add_on = {}
    batches = json.loads( open('batches.json').read() )
    for campaign in by_campaign:
        ## get a bunch of information
        setup  = copy.deepcopy( default_setup )
        add_on[campaign] = setup
        sendLog('batchor','Adding the relval campaigns %s with parameters \n%s'%( campaign, json.dumps( setup, indent=2)),level='critical')
        if not campaign in batches: batches[campaign] = []
        batches[campaign] = list(set(list(copy.deepcopy( by_campaign[campaign] )) + batches[campaign] ))
    for campaign in by_hi_campaign:
        ## get a bunch of information
        setup  = copy.deepcopy( default_hi_setup )
        hi_site = random.choice(["T1_DE_KIT","T1_FR_CCIN2P3"])
        setup["parameters"]["SiteWhitelist"]=[ hi_site ]

        add_on[campaign] = setup
        sendLog('batchor','Adding the HI relval campaigns %s with parameters \n%s'%( campaign, json.dumps( setup, indent=2)),level='critical')
        if not campaign in batches: batches[campaign] = []
        batches[campaign] = list(set(list(copy.deepcopy( by_hi_campaign[campaign] )) + batches[campaign] ))
        
    
    open('batches.json','w').write( json.dumps( batches , indent=2 ) )

    ## open the campaign configuration 
    campaigns = json.loads( open('campaigns.relval.json').read() )


    ## protect for overwriting ??
    for new_campaign in list(set(add_on.keys())-set(campaigns.keys())):
        ## this is new, and can be announced as such
        print new_campaign,"is new stuff"
        workflows = by_campaign[new_campaign]
        requester = list(set([wf.split('_')[0] for wf in workflows]))
        subject = "Request of RelVal samples batch %s"% new_campaign
        text="""Dear all, 
A new batch of relval workflows was requested.

Batch ID:

%s

Requestor:

%s

Details of the workflows:

https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?campaign=%s

This is an automated message"""%( new_campaign, 
                                  ', '.join(requester),
                                  new_campaign,
                                  #'\n'.join( sorted(workflows) ) 
                                  )


        print subject
        print text
        to = ['hn-cms-dataopsrequests@cern.ch']
        sendEmail(subject, text, destination=to)
        sendLog('batchor',text, level='critical')

    ## merge all anyways
    campaigns.update( add_on )

    ## write it out for posterity
    open('campaigns.json.updated','w').write(json.dumps( campaigns , indent=2))

    ## read back
    rread = json.loads(open('campaigns.json.updated').read())

    os.system('mv campaigns.json.updated campaigns.relval.json')

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'

    batchor(url)
