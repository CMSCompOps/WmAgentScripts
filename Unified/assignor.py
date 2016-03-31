#!/usr/bin/env python
from assignSession import *
import reqMgrClient
from utils import workflowInfo, campaignInfo, siteInfo, userLock, global_SI, unifiedConfiguration, reqmgr_url
from utils import getSiteWhiteList, getWorkLoad, getDatasetPresence, getDatasets, findCustodialLocation, getDatasetBlocksFraction, getDatasetEventsPerLumi, newLockInfo, getLFNbase, getDatasetBlocks
from utils import componentInfo, sendEmail, sendLog
#from utils import lockInfo
from utils import duplicateLock, notRunningBefore
import optparse
import itertools
import time
from htmlor import htmlor
import os
import random
import json
import copy
import os

def assignor(url ,specific = None, talk=True, options=None):
    if userLock(): return
    if duplicateLock(): return
    #if notRunningBefore( 'stagor' ): return
    if not componentInfo().check(): return

    UC = unifiedConfiguration()
    CI = campaignInfo()
    SI = global_SI
    #LI = lockInfo()
    NLI = newLockInfo()

    n_assigned = 0
    n_stalled = 0

    wfos=[]
    if specific or options.early:
        wfos.extend( session.query(Workflow).filter(Workflow.status=='considered').all())
        wfos.extend( session.query(Workflow).filter(Workflow.status=='staging').all())
    if specific:
        wfos.extend( session.query(Workflow).filter(Workflow.status=='considered-tried').all())        
    wfos.extend(session.query(Workflow).filter(Workflow.status=='staged').all())
    #if specific:
    #    #wfos = session.query(Workflow).filter(Workflow.name==specific).all()
    #    wfos = session.query(Workflow).filter(Workflow.name.contains(specific)).all()
    #if not wfos:
    #    if specific:
    #        wfos = session.query(Workflow).filter(Workflow.status=='considered').all()
    #        wfos.extend( session.query(Workflow).filter(Workflow.status=='staging').all())
    #    wfos.extend(session.query(Workflow).filter(Workflow.status=='staged').all())

    random.shuffle( wfos )
    for wfo in wfos:
        if options.limit and (n_stalled+n_assigned)>options.limit:
            break

        if specific:
            if not any(map(lambda sp: sp in wfo.name,specific.split(','))): continue
            #if not specific in wfo.name: continue
        print "\n\n"
        wfh = workflowInfo( url, wfo.name)
        wfh.sendLog('assignor',"%s to be assigned"%wfo.name)


        ## check if by configuration we gave it a GO
        if not CI.go( wfh.request['Campaign'] ) and not options.go:
            wfh.sendLog('assignor',"No go for %s"% wfh.request['Campaign'])
            n_stalled+=1
            continue

        ## check on current status for by-passed assignment
        if wfh.request['RequestStatus'] !='assignment-approved':
            if not options.test:
                wfh.sendLog('assignor',"setting %s away and skipping"%wfo.name)
                ## the module picking up from away will do what is necessary of it
                wfo.wm_status = wfh.request['RequestStatus']
                wfo.status = 'away'
                session.commit()
                continue
            else:
                print wfo.name,wfh.request['RequestStatus']

        ## retrieve from the schema, dbs and reqMgr what should be the next version
        version=wfh.getNextVersion()
        if not version:
            if options and options.ProcessingVersion:
                version = options.ProcessingVersion
            else:
                wfh.sendLog('assignor',"cannot decide on version number")
                n_stalled+=1
                wfo.status = 'trouble'
                session.commit()
                continue

        ## the site whitelist takes into account siteInfo, campaignInfo, memory and cores
        (lheinput,primary,parent,secondary, sites_allowed) = wfh.getSiteWhiteList()

        original_sites_allowed = copy.deepcopy( sites_allowed )
        wfh.sendLog('assignor',"Site white list %s"%sorted(sites_allowed))
        override_sec_location = CI.get(wfh.request['Campaign'], 'SecondaryLocation', [])

        blocks = []
        if 'BlockWhitelist' in wfh.request:
            blocks = wfh.request['BlockWhitelist']
        if 'RunWhitelist' in wfh.request and wfh.request['RunWhitelist']:
            ## augment with run white list
            for dataset in primary:
                blocks = list(set( blocks + getDatasetBlocks( dataset, runs=wfh.request['RunWhitelist'] ) ))

        wfh.sendLog('assignor',"Allowed %s"%sorted(sites_allowed))
        secondary_locations=None
        for sec in list(secondary):
            if override_sec_location: 
                print "We don't care where the secondary is"
                print "Cannot pass for now"
                sendEmail("tempting to pass sec location check","but we cannot yet IMO")
                #pass

            presence = getDatasetPresence( url, sec )
            print sec
            print json.dumps(presence, indent=2)
            one_secondary_locations = [site for (site,(there,frac)) in presence.items() if frac>98.]
            #one_secondary_locations = [site for (site,(there,frac)) in presence.items() if there]
            if secondary_locations==None:
                secondary_locations = one_secondary_locations
            else:
                secondary_locations = list(set(secondary_locations) & set(one_secondary_locations))
            ## reduce the site white list to site with secondary only
            #sites_allowed = [site for site in sites_allowed if any([osite.startswith(site) for osite in one_secondary_locations])]
            sites_allowed = [site for site in sites_allowed if SI.CE_to_SE(site) in one_secondary_locations]
            
        wfh.sendLog('assignor',"From secondary requirement, now Allowed%s"%sorted(sites_allowed))

        initial_sites_allowed = copy.deepcopy( sites_allowed ) ## keep track of this, after secondary input location restriction : that's how you want to operate it

        sites_all_data = copy.deepcopy( sites_allowed )
        sites_with_data = copy.deepcopy( sites_allowed )
        sites_with_any_data = copy.deepcopy( sites_allowed )
        primary_locations = None
        available_fractions = {}
        set_lfn = '/store/mc' ## by default
        for prim in list(primary):
            set_lfn = getLFNbase( prim )
            presence = getDatasetPresence( url, prim , only_blocks=blocks)
            if talk:
                print prim
                print json.dumps(presence, indent=2)
            available_fractions[prim] =  getDatasetBlocksFraction(url, prim, sites = [SI.CE_to_SE(site) for site in sites_allowed] , only_blocks = blocks)
            #sites_all_data = [site for site in sites_with_data if any([osite.startswith(site) for osite in [psite for (psite,(there,frac)) in presence.items() if there]])]
            #sites_with_data = [site for site in sites_with_data if any([osite.startswith(site) for osite in [psite for (psite,frac) in presence.items() if frac[1]>90.]])]
            sites_all_data = [site for site in sites_with_data if SI.CE_to_SE(site) in [psite for (psite,(there,frac)) in presence.items() if there]]
            sites_with_data = [site for site in sites_with_data if SI.CE_to_SE(site) in [psite for (psite,frac) in presence.items() if frac[1]>90.]]
            sites_with_any_data = [site for site in sites_with_any_data if SI.CE_to_SE(site) in presence.keys()]
            wfh.sendLog('assignor',"Holding the data but not allowed %s"%sorted(list(set([se_site for se_site in presence.keys() if not SI.SE_to_CE(se_site) in sites_allowed]))))
            if primary_locations==None:
                primary_locations = presence.keys()
            else:
                primary_locations = list(set(primary_locations) & set(presence.keys() ))

        sites_with_data = list(set(sites_with_data))
        sites_with_any_data = list(set(sites_with_any_data))

        opportunistic_sites=[]
        down_time = False
        ## opportunistic running where any piece of data is available
        if secondary_locations or primary_locations:
            ## intersection of both any pieces of the primary and good IO
            #opportunistic_sites = [SI.SE_to_CE(site) for site in list((set(secondary_locations) & set(primary_locations) & set(SI.sites_with_goodIO)) - set(sites_allowed))]
            if secondary_locations and primary_locations:
                opportunistic_sites = [SI.SE_to_CE(site) for site in list((set(secondary_locations) & set(primary_locations)) - set([SI.CE_to_SE(site) for site in sites_allowed]))]
            elif primary_locations:
                opportunistic_sites = [SI.SE_to_CE(site) for site in list(set(primary_locations) - set([SI.CE_to_SE(site) for site in sites_allowed]))]
            else:
                opportunistic_sites = []
            wfh.sendLog('assignor',"We could be running in addition at %s"% sorted(opportunistic_sites))
            if any([osite in SI.sites_not_ready for osite in opportunistic_sites]):
                wfh.sendLog('assignor',"One of the usable site is in downtime %s"%([osite in SI.sites_not_ready for osite in opportunistic_sites]))
                down_time = True
                ## should this be send back to considered ?
                

        """
        if available_fractions and not all([available>=1. for available in available_fractions.values()]):
            print "The input dataset is not located in full over sites"
            print json.dumps(available_fractions)
            if not options.test and not options.go:
                known = []
                try:
                    known = json.loads(open('cannot_assign.json').read())
                except:
                    pass
                if not wfo.name in known:
                    sendEmail( "cannot be assigned","%s is not full over sites \n %s"%(wfo.name,json.dumps(available_fractions)))
                    known.append( wfo.name )
                    open('cannot_assign.json','w').write(json.dumps( known, indent=2))
                n_stalled+=1
                continue ## skip skip skip
        """

        ## should be 2 but for the time-being let's lower it to get things going
        copies_wanted,cpuh = wfh.getNCopies()
        if 'Campaign' in wfh.request and wfh.request['Campaign'] in CI.campaigns and 'maxcopies' in CI.campaigns[wfh.request['Campaign']]:
            copies_needed_from_campaign = CI.campaigns[wfh.request['Campaign']]['maxcopies']
            copies_wanted = min(copies_needed_from_campaign, copies_wanted)
        
        if not options.early:
            less_copies_than_requested = UC.get("less_copies_than_requested")
            copies_wanted = max(1,copies_wanted-less_copies_than_requested) # take one out for the efficiency


        wfh.sendLog('assignor',"needed availability fraction %s"% copies_wanted)

        ## should also check on number of sources, if large enough, we should be able to overflow most, efficiently

        if available_fractions and not all([available>=copies_wanted for available in available_fractions.values()]):
            not_even_once = not all([available>=1. for available in available_fractions.values()])
            wfh.sendLog('assignor',"The input dataset is not available %s times, only %s"%( copies_wanted, available_fractions.values()))
            if down_time and not options.go and not options.early:
                wfo.status = 'considered'
                session.commit()
                wfh.sendLog('assignor',"sending back to considered because of site downtime, instead of waiting")
                sendEmail( "cannot be assigned due to downtime","%s is not sufficiently available, due to down time of a site in the whitelist. check the assignor logs. sending back to considered."% wfo.name)
                continue
                #pass

            print json.dumps(available_fractions)
            if (options.go and not_even_once) or not options.go:
                known = []
                try:
                    known = json.loads(open('cannot_assign.json').read())
                except:
                    pass
                if not wfo.name in known and not options.limit and not options.go and not options.early:
                    wfh.sendLog('assignor',"cannot be assigned, %s is not sufficiently available.\n %s"%(wfo.name,json.dumps(available_fractions)))
                    sendEmail( "cannot be assigned","%s is not sufficiently available.\n %s"%(wfo.name,json.dumps(available_fractions)))
                    known.append( wfo.name )
                    open('cannot_assign.json','w').write(json.dumps( known, indent=2))
                n_stalled+=1
                if options.early:
                    if wfo.status == 'considered':
                        wfh.sendLog('assignor',"setting considered-tried")
                        wfo.status = 'considered-tried'
                        session.commit()
                    else:
                        print "tried but status is",wfo.status
                continue

        ## default back to white list to original white list with any data
        print "Allowed",sites_allowed
        if options.primary_aaa:
            sites_allowed = initial_sites_allowed
            #options.useSiteListAsLocation = True
            options.TrustSitelists = True
        else:
            sites_allowed = sites_with_any_data
            wfh.sendLog('assignor',"Selected for any data %s"%sorted(sites_allowed))

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
            wfh.sendLog('assignor',"cannot be assign with no matched sites")
            sendEmail( "cannot be assigned","%s has no whitelist"%(wfo.name))
            n_stalled+=1
            continue

        t1_only = [ce for ce in sites_allowed if ce.startswith('T1')]
        if t1_only:
            # try to pick from T1 only first
            sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in t1_only])]
        else:
            # then pick any otherwise
            sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in sites_allowed])]
            
        ## one last modification now that we know we can assign, and to make sure all ressource can be used by the request : set all ON sites to whitelist
        ###sites_allowed = original_sites_allowed ## not needed, afterall as secondary jobs go their own ways
            
        wfh.sendLog('assignor',"Placing the output on %s"%sites_out)
        parameters={
            'SiteWhitelist' : sites_allowed,
            #'CustodialSites' : sites_custodial,
            'NonCustodialSites' : sites_out,
            'AutoApproveSubscriptionSites' : list(set(sites_out)),
            'AcquisitionEra' : wfh.acquisitionEra(),
            'ProcessingString' : wfh.processingString(),
            'MergedLFNBase' : set_lfn,
            'ProcessingVersion' : version,
            }


        ## plain assignment here
        team='production'
        if os.getenv('UNIFIED_TEAM'): team = os.getenv('UNIFIED_TEAM')
        if options and options.team:
            team = options.team

        ## high priority team agent
        #if wfh.request['RequestPriority'] >= 100000 and (wfh.request['TimePerEvent']*int(wfh.getRequestNumEvents()))/(8*3600.) < 10000:
        #    team = 'highprio'
        #    sendEmail("sending work with highprio team","%s"% wfo.name, destination=['dmytro.kovalskyi@cern.ch'])

        ## SDSC redirection
        #if "T2_US_UCSD" in sites_with_data and random.random() < -0.5 and wfh.request['Campaign']=='RunIISpring15DR74' and int(wfh.getRequestNumEvents()) < 600000 and not any([out.endswith('RAW') for out in wfh.request['OutputDatasets']]):
        #    ## consider SDSC
        #    parameters['SiteWhitelist'] = ['T2_US_UCSD','T3_US_SDSC']
        #    parameters['useSiteListAsLocation'] = True
        #    team = 'allocation-based'
        #    sendEmail("sending work to SDSC","%s was assigned to SDSC/UCSD"% wfo.name, destination=['boj@fnal.gov'])
        
        ## SDSC redirection
        #if wfh.request['Campaign']==R'unIIWinter15GS' and random.random() < -1.0:
        #    parameters['SiteWhitelist'] = ['T3_US_SDSC']
        #    team = 'allocation-based'
        #    sendEmail("sending work to SDSC","%s was assigned to SDSC"% wfo.name, destination=['boj@fnal.gov'])
        

        if False and 'T2_CH_CERN' in parameters['SiteWhitelist']:
            ## add some check on 
            ### the amount pending to HLT
            ### the size of the request
            ### the priority of the request (maybe not if we decide to overflow during runs)
            parameters['SiteWhitelist'] = ['T2_CH_CERN_HLT']
            team = 'hlt'
            ## reduce the splitting by factor of 4, regardless of type of splitting
            sendEmail("sending work to HLT","%s was assigned to HLT"%wfo.name)
            

        ##parse options entered in command line if any
        if options:
            for key in reqMgrClient.assignWorkflow.keys:
                v=getattr(options,key)
                if v!=None:
                    if type(v)==str and ',' in v: 
                        parameters[key] = filter(None,v.split(','))
                    else: 
                        parameters[key] = v

        ## pick up campaign specific assignment parameters
        parameters.update( CI.parameters(wfh.request['Campaign']) )

        if not options.test:
            parameters['execute'] = True

        split_check = wfh.checkWorkflowSplitting()
        if split_check!=True:
            parameters.update( split_check )
            if 'EventBased' in split_check.values():
                wfh.sendLog('assignor', "Falling back to event splitting.")
                sendEmail("Fallback to EventBased","the workflow %s is too heavy to be processed as it is. Fallback to EventBased splitting"%wfo.name)
            elif 'EventsPerJob' in split_check.values():
                wfh.sendLog('assignor', "Modifying the number of job per event")
                sendEmail("Modifying the job per events","the workflow %s is too heavy in number of jobs explosion"%wfo.name)

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
                    sendEmail("issue with event splitting for run-dependent MC","%s needs to be split by event with %s per job"%(wfo.name, eventsPerJob))
                    wfh.sendLog('assignor', "%s needs to be split by event with %s per job"%(wfo.name, eventsPerJob))
                    parameters['EventsPerJob'] = eventsPerJob
                else:
                    spl = wfh.getSplittings()[0]
                    eventsPerJobEstimated = spl['events_per_job'] if 'events_per_job' in spl else None
                    eventsPerJobEstimated = spl['avg_events_per_job'] if 'avg_events_per_job' in spl else None
                    if eventsPerJobEstimated and eventsPerJobEstimated > eventsPerJob:
                        sendEmail("setting lumi splitting for run-dependent MC","%s was assigned with %s lumis/job"%( wfo.name, lumisPerJob))
                        wfh.sendLog('assignor',"%s was assigned with %s lumis/job"%( wfo.name, lumisPerJob))
                        parameters['LumisPerJob'] = lumisPerJob
                    else:
                        sendEmail("leaving splitting untouched for PU_RD*","please check on "+wfo.name)
                        wfh.sendLog('assignor',"leaving splitting untouched for PU_RD*, please check.")
        result = reqMgrClient.assignWorkflow(url, wfo.name, team, parameters)


        # set status
        if not options.test:
            if result:
                wfo.status = 'away'
                session.commit()
                n_assigned+=1
                try:
                    ## refetch information and lock output
                    new_wfi = workflowInfo( url, wfo.name)
                    (_,prim,_,sec) = new_wfi.getIO()
                    for secure in list(prim)+list(sec)+new_wfi.request['OutputDatasets']:
                        ## lock all outputs flat
                        NLI.lock( secure )
                    #for site in [SI.CE_to_SE(site) for site in sites_allowed]:
                    #    for output in new_wfi.request['OutputDatasets']:
                    #        LI.lock( output, site, 'dataset in production')
                    #    for primary in prim:
                    #        LI.lock( primary, site, 'dataset used in input')
                    #    for secondary in sec:
                    #        LI.lock( secondary, site, 'required for mixing' )

                except Exception as e:
                    print "fail in locking output"
                    
                    print str(e)
                    sendEmail("failed locking of output",str(e))


            else:
                print "ERROR could not assign",wfo.name
        else:
            pass
    print "Assignment summary:"
    sendLog('assignor',"Assigned %d Stalled %s"%(n_assigned, n_stalled))
    
if __name__=="__main__":
    url = reqmgr_url
    parser = optparse.OptionParser()
    parser.add_option('-t','--test', help='Only test the assignment',action='store_true',dest='test',default=False)
    parser.add_option('-r', '--restrict', help='Only assign workflows for site with input',default=False, action="store_true",dest='restrict')
    parser.add_option('-e', '--early', help='Fectch from early statuses',default=False, action="store_true")
    parser.add_option('--go',help="Overrides the campaign go",default=False,action='store_true')
    parser.add_option('--team',help="Specify the agent to use",default=None)
    parser.add_option('--primary_aaa',help="Force to use the secondary location restriction, if any, and use the full site whitelist initially provided to run that type of wf",default=False, action='store_true')
    parser.add_option('--limit',help="Limit the number of wf to be assigned",default=0,type='int')
    for key in reqMgrClient.assignWorkflow.keys:
        parser.add_option('--%s'%key,help="%s Parameter of request manager assignment interface"%key, default=None)
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    assignor(url,spec, options=options)

    if not spec:
        htmlor()
