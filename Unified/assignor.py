#!/usr/bin/env python
from assignSession import *
import reqMgrClient
from utils import workflowInfo, campaignInfo, siteInfo, userLock
from utils import getSiteWhiteList, getWorkLoad, getDatasetPresence, getDatasets, findCustodialLocation, getDatasetBlocksFraction, getDatasetEventsPerLumi
from utils import componentInfo, sendEmail
from utils import lockInfo
import optparse
import itertools
import time
from htmlor import htmlor
import os
import random
import json

def assignor(url ,specific = None, talk=True, options=None):
    if userLock('assignor'): return

    ## check that no other instances of assignor is running
    process_check = filter(None,os.popen('ps -f -e | grep assignor.py | grep -v grep  |grep python').read().split('\n'))
    if len(process_check)>1:
        ## another assignor is running on the machine : stop
        sendEmail('overlapping assignor','There are %s instances running %s'%(len(process_check), '\n'.join(process_check)))
        print "quitting because of overlapping processes"
        return 

    if not componentInfo().check(): return

    CI = campaignInfo()
    SI = siteInfo()
    LI = lockInfo()

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

        if 'SiteWhitelist' in CI.parameters(wfh.request['Campaign']):
            sites_allowed = CI.parameters(wfh.request['Campaign'])['SiteWhitelist']

        if 'SiteBlacklist' in CI.parameters(wfh.request['Campaign']):
            print "Reducing the whitelist due to black list in campaign configuration"
            print "Removing",CI.parameters(wfh.request['Campaign'])['SiteBlacklist']
            sites_allowed = list(set(sites_allowed) - set(CI.parameters(wfh.request['Campaign'])['SiteBlacklist']))


        memory_allowed = SI.sitesByMemory( wfh.request['Memory'] )
        if memory_allowed!=None:
            print "sites allowing", wfh.request['Memory'],"are",memory_allowed
            sites_allowed = list(set(sites_allowed) & set(memory_allowed))

        print "Allowed",sites_allowed
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
            

        sites_all_data = copy.deepcopy( sites_allowed )
        sites_with_data = copy.deepcopy( sites_allowed )
        sites_with_any_data = copy.deepcopy( sites_allowed )
        primary_locations = None
        available_fractions = {}
        for prim in list(primary):
            presence = getDatasetPresence( url, prim )
            if talk:
                print prim
                print json.dumps(presence, indent=2)
            available_fractions[prim] =  getDatasetBlocksFraction(url, prim, sites = [SI.CE_to_SE(site) for site in sites_allowed] )
            sites_all_data = [site for site in sites_with_data if any([osite.startswith(site) for osite in [psite for (psite,(there,frac)) in presence.items() if there]])]
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

        if available_fractions and not all([available>=1. for available in available_fractions.values()]):
            print "The input dataset is not located in full over sites"
            print json.dumps(available_fractions)
            if not options.test and not options.go:
                sendEmail( "cannot be assigned","%s is not full over sites \n %s"%(wfo.name,json.dumps(available_fractions)))
                continue ## skip skip skip

        copies_wanted = 2.
        if available_fractions and not all([available>=copies_wanted for available in available_fractions.values()]):
            print "The input dataset is not available",copies_wanted,"times, only",available_fractions.values()
            if not options.go:
                sendEmail( "cannot be assigned","%s is not sufficiently available \n %s"%(wfo.name,json.dumps(available_fractions)))
                continue

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
            sendEmail( "cannot be assigned","%s has no whitelist"%(wfo.name))
            continue

        t1_only = [ce for ce in sites_allowed if ce.startswith('T1')]
        if t1_only:
            # try to pick from T1 only first
            sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in t1_only])]
        else:
            # then pick any otherwise
            sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in sites_allowed])]


        print "Placing the output on", sites_out
        parameters={
            'SiteWhitelist' : sites_allowed,
            #'CustodialSites' : sites_custodial,
            'NonCustodialSites' : sites_out,
            'AutoApproveSubscriptionSites' : list(set(sites_out)),
            'AcquisitionEra' : wfh.acquisitionEra(),
            'ProcessingString' : wfh.processingString(),
            'MergedLFNBase' : '/store/mc', ## to be figured out
            'ProcessingVersion' : version,
            }


        ## plain assignment here
        team='production'
        if options and options.team:
            team = options.team

        if "T2_US_UCSD" in sites_with_data and random.random() < 0.9 and wfh.request['Campaign']=='RunIISpring15DR74' and int(wfh.getRequestNumEvents()) < 200000 and not any([out.endswith('RAW') for out in wfh.request['OutputDatasets']]):
            ## consider SDSC
            parameters['SiteWhitelist'] = ['T2_US_UCSD','T3_US_SDSC']
            parameters['useSiteListAsLocation'] = True
            team = 'allocation-based'
            sendEmail("sending work to SDSC","%s was assigned to SDSC/UCSD"% wfo.name, destination=['boj@fnal.gov'])
            
        if wfh.request['Campaign']=='RunIIWinter15GS' and random.random() < -1.0:
            parameters['SiteWhitelist'] = ['T3_US_SDSC']
            team = 'allocation-based'
            sendEmail("sending work to SDSC","%s was assigned to SDSC"% wfo.name, destination=['boj@fnal.gov'])
        

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

        # Handle run-dependent MC
        pstring = wfh.processingString()
        if 'PU_RD' in pstring:
            numEvents = wfh.getRequestNumEvents()
            eventsPerLumi = [getDatasetEventsPerLumi(prim) for prim in primary]
            eventsPerLumi = sum(eventsPerLumi)/float(len(eventsPerLumi))
            reqJobs = 500
            if 'PU_RD2' in pstring:
                reqJobs = 2000
                eventsPerJob = int(numEvents/(reqJobs*1.4))
                lumisPerJob = int(eventsPerJob/eventsPerLumi)
                if lumisPerJob==0:
                    print "There is no go for assigning that request without event splitting"
                    sendEmail("issue with event splitting for run-dependent MC","%s needs to be split by event with %s per job"%(wfo.name, eventsPerJob))
                    print "need to go down to",eventsPerJob,"events per job"
                    parameters['EventsPerJob'] = eventsPerJob
                else:
                    spl = wfh.getSplittings()[0]
                    eventsPerJobEstimated = spl['events_per_job'] if 'events_per_job' in spl else None
                    if eventsPerJobEstimated and eventsPerJobEstimated > eventsPerJob:
                        print "need to go down to",lumisPerJob,"in assignment"
                        sendEmail("setting lumi splitting for run-dependent MC","%s was assigned with %s lumis/job"%( wfo.name, lumisPerJob))
                        parameters['LumisPerJob'] = lumisPerJob
                    else:
                        print "the regular splitting should work for",pstring
                        sendEmail("leaving splitting untouched for PU_RD*","please check on "+wfo.name)

        result = reqMgrClient.assignWorkflow(url, wfo.name, team, parameters)


        # set status
        if not options.test:
            if result:
                wfo.status = 'away'
                session.commit()

                try:
                    ## refetch information and lock output
                    new_wfi = workflowInfo( url, wfo.name)
                    for site in [SI.CE_to_SE(site) for site in sites_allowed]:
                        for output in new_wfi.request['OutputDatasets']:
                            LI.lock( output, site, 'dataset in production')
                    if 'MCPileup' in new_wfi.request and new_wfi.request['MCPileup']:
                        LI.lock(new_wfi.request['MCPileup'], site, 'required for mixing')

                except Exception as e:
                    print "fail in locking output"
                    print str(e)
                    sendEmail("failed locking of output",str(e))


            else:
                print "ERROR could not assign",wfo.name
        else:
            pass

    
if __name__=="__main__":
    url = 'cmsweb.cern.ch'

    parser = optparse.OptionParser()
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

    if not spec:
        htmlor()
        pass
