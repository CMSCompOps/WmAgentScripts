#!/usr/bin/env python
from assignSession import *
import reqMgrClient
from utils import workflowInfo, campaignInfo, siteInfo, userLock
from utils import getSiteWhiteList, getWorkLoad, getDatasetPresence, getDatasets, findCustodialLocation
import optparse
import itertools
import time
from htmlor import htmlor
import os
import json

def assignor(url ,specific = None, talk=True, options=None):
    if userLock('assignor'): return

    CI = campaignInfo()
    SI = siteInfo()

    wfos=[]
    if specific:
        wfos = session.query(Workflow).filter(Workflow.name==specific).all()
    if not wfos:
        if specific:
            wfos = session.query(Workflow).filter(Workflow.status=='considered').all()
            wfos.extend( session.query(Workflow).filter(Workflow.status=='staging').all())
        wfos.extend(session.query(Workflow).filter(Workflow.status=='staged').all())

    for wfo in wfos:
        if specific:
            if not any(map(lambda sp: sp in wfo.name,specific.split(','))): continue
            #if not specific in wfo.name: continue
        print wfo.name,"to be assigned"
        wfh = workflowInfo( url, wfo.name)


        ## check if by configuration we gave it a GO
        if not CI.go( wfh.request['Campaign'] ) and not options.go:
            print "No go for",wfh.request['Campaign']
            continue

        ## check on current status for by-passed assignment
        if wfh.request['RequestStatus'] !='assignment-approved':
            print wfo.name,wfh.request['RequestStatus'],"skipping"
            if not options.test:
                continue

        ## retrieve from the schema, dbs and reqMgr what should be the next version
        version=wfh.getNextVersion()
        if not version:
            if options and options.ProcessingVersion:
                version = options.ProcessingVersion
            else:
                print "cannot decide on version number"
                continue

        (lheinput,primary,parent,secondary) = wfh.getIO()
        sites_allowed = getSiteWhiteList( (lheinput,primary,parent,secondary) )
        print "Allowed",sites_allowed
        sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in sites_allowed])]
        sites_custodial = []
        if len(sites_custodial)==0:
            print "No custodial, it's fine, it's covered in close-out"

        if len(sites_custodial)>1:
            print "more than one custodial for",wfo.name
            sys.exit(36)

        secondary_locations=None
        for sec in list(secondary):
            presence = getDatasetPresence( url, sec )
            print sec
            print json.dumps(presence, indent=2)
            #one_secondary_locations = [site for (site,(there,frac)) in presence.items() if frac>90.]
            one_secondary_locations = [site for (site,(there,frac)) in presence.items() if there]
            if secondary_locations==None:
                secondary_locations = one_secondary_locations
            else:
                secondary_locations = list(set(secondary_locations) & set(one_secondary_locations))
            ## reduce the site white list to site with secondary only
            sites_allowed = [site for site in sites_allowed if any([osite.startswith(site) for osite in one_secondary_locations])]
            

        sites_with_data = copy.deepcopy( sites_allowed )
        sites_with_any_data = copy.deepcopy( sites_allowed )
        primary_locations = None
        for prim in list(primary):
            presence = getDatasetPresence( url, prim )
            if talk:
                print prim
                print json.dumps(presence, indent=2)
            sites_with_data = [site for site in sites_with_data if any([osite.startswith(site) for osite in [psite for (psite,frac) in presence.items() if frac[1]>90.]])]
            sites_with_any_data = [site for site in sites_with_any_data if any([osite.startswith(site) for osite in presence.keys()])]
            if primary_locations==None:
                primary_locations = presence.keys()
            else:
                primary_locations = list(set(primary_locations) & set(presence.keys() ))

        sites_with_data = list(set(sites_with_data))
        sites_with_any_data = list(set(sites_with_any_data))

        opportunistic_sites=[]
        ## opportunistic running where any piece of data is available
        if secondary_locations and primary_locations:
            ## intersection of both any pieces of the primary and good IO
            #opportunistic_sites = [SI.SE_to_CE(site) for site in list((set(secondary_locations) & set(primary_locations) & set(SI.sites_with_goodIO)) - set(sites_allowed))]
            opportunistic_sites = [SI.SE_to_CE(site) for site in list((set(secondary_locations) & set(primary_locations)) - set(sites_allowed))]
            print "We could be running at",opportunistic_sites,"in addition"

        ## default back to white list to original white list with any data
        print "Allowed",sites_allowed
        sites_allowed = sites_with_any_data
        print "Selected for any data",sites_allowed

        if options.restrict:
            print "Allowed",sites_allowed
            sites_allowed = sites_with_any_data
            print "Selected",sites_allowed
        else:
            if set(sites_with_data) != set(sites_allowed):
                ## the data is not everywhere we wanted to run at : enable aaa
                print "Sites with 90% data not matching site white list (block choping!)"
                print "Resorting to AAA reading for",list(set(sites_allowed) - set(sites_with_data)),"?"
                print "Whitelist site with any data",list(set(sites_allowed) - set(sites_with_any_data))
                #options.useSiteListAsLocation = True
                #print "Not commissioned yet"
                #continue
            #print "We could be running at",opportunistic_sites,"in addition"
            ##sites_allowed = list(set(sites_allowed+ opportunistic_sites))

        if not len(sites_allowed):
            print wfo.name,"cannot be assign with no matched sites"
            continue

        parameters={
            'SiteWhitelist' : sites_allowed,
            'CustodialSites' : sites_custodial,
            'NonCustodialSites' : sites_out,
            'AutoApproveSubscriptionSites' : list(set(sites_out)),
            'AcquisitionEra' : wfh.acquisitionEra(),
            'ProcessingString' : wfh.processingString(),
            'MergedLFNBase' : '/store/mc', ## to be figured out ! from Hi shit
            'ProcessingVersion' : version,
            }

        ##parse options entered in command line if any
        if options:
            for key in reqMgrClient.assignWorkflow.keys:
                v=getattr(options,key)
                if v!=None:
                    if ',' in v: parameters[key] = filter(None,v.split(','))
                    else: parameters[key] = v

        ## pick up campaign specific assignment parameters
        parameters.update( CI.parameters(wfh.request['Campaign']) )

        if not options.test:
            parameters['execute'] = True

        if not wfh.checkWorkflowSplitting():
            ## needs to go to event based ? fail for now
            print "Falling back to event splitting ?"
            #parameters['SplittingAlgorithm'] = 'EventBased'
            continue

        ## plain assignment here
        team='production'
        if options and options.team:
            team = options.team
        result = reqMgrClient.assignWorkflow(url, wfo.name, team, parameters)

        # set status
        if not options.test:
            if result:
                wfo.status = 'away'
                session.commit()
            else:
                print "ERROR could not assign",wfo.name
        else:
            pass

    
if __name__=="__main__":
    url = 'cmsweb.cern.ch'

    parser = optparse.OptionParser()
    #parser.add_option('-e', '--execute', help='Actually assign workflows',action="store_true",dest='execute')
    parser.add_option('-t','--test', help='Only test the assignment',action='store_true',dest='test',default=False)
    parser.add_option('-r', '--restrict', help='Only assign workflows for site with input',default=False, action="store_true",dest='restrict')
    parser.add_option('--go',help="Overrides the campaign go",default=False,action='store_true')
    parser.add_option('--team',help="Specify the agent to use",default='production')

    for key in reqMgrClient.assignWorkflow.keys:
        parser.add_option('--%s'%key,help="%s Parameter of request manager assignment interface"%key, default=None)
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    assignor(url,spec, options=options)

    if not options.test:
        htmlor()
        pass
