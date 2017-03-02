#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, sendEmail, sendLog
from collections import defaultdict
import copy
import json
import os


def batchor( url ):
    ## get all workflows in assignment-approved with SubRequestType = relval
    wfs = getWorkflows(url, 'assignment-approved', details=True, user='fabozzi')
    wfs = filter( lambda r :r['SubRequestType'] == 'RelVal' if 'SubRequestType' in r else False, wfs)


    by_campaign = defaultdict(set)
    for wf in wfs:
        print wf['RequestName'], wf['Campaign']
        by_campaign[wf['Campaign']].add( wf['RequestName'] )

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
        "fractionpass" : 0.0
        }
    add_on = {}
    batches = json.loads( open('batches.json').read() )
    for campaign in by_campaign:
        ## get a bunch of information
        setup  = copy.deepcopy( default_setup )
        ## rule HI to something special
        ## warn about the secondary setting ?
        add_on[campaign] = setup
        if not campaign in batches: batches[campaign] = []
        batches[campaign].extend(copy.deepcopy( by_campaign[campaign] ))
        
    open('batches.json','w').write( json.dumps( batches ) )

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
