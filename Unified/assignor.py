#!/usr/bin/env python
from assignSession import *
import reqMgrClient
from utils import workflowInfo, campaignInfo, siteInfo, userLock, unifiedConfiguration, reqmgr_url, monitor_pub_dir, monitor_dir, global_SI
from utils import getWorkLoad, getDatasetPresence, getDatasets, findCustodialLocation, getDatasetBlocksFraction, getDatasetEventsPerLumi, getLFNbase, getDatasetBlocks, lockInfo, getAllStuckDataset
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
    if not componentInfo().check(): return

    UC = unifiedConfiguration()
    CI = campaignInfo()
    #SI = siteInfo()
    SI = global_SI()
    #NLI = newLockInfo()
    #if not NLI.free() and not options.go: return
    LI = lockInfo()
    if not LI.free() and not options.go: return

    n_assigned = 0
    n_stalled = 0

    wfos=[]
    fetch_from = []
    if specific or options.early:
        fetch_from.extend(['considered','staging'])
    if specific:
        fetch_from.extend(['considered-tried'])
    
    fetch_from.extend(['staged'])

    if options.from_status:
        fetch_from = options.from_status.split(',')
        print "Overriding to read from",fetch_from

    for status in fetch_from:
        print "getting wf in",status
        wfos.extend(session.query(Workflow).filter(Workflow.status==status).all())
        print len(wfos)

    ## in case of partial, go for fetching a list from json ?
    #if options.partial and not specific:
    #    pass

    dataset_endpoints = json.loads(open('%s/dataset_endpoints.json'%monitor_dir).read())
    aaa_mapping = json.loads(open('%s/equalizor.json'%monitor_pub_dir).read())['mapping']

    all_stuck = set()
    all_stuck.update( json.loads( open('%s/stuck_transfers.json'%monitor_pub_dir).read() ))
    all_stuck.update( getAllStuckDataset()) 

    max_per_round = UC.get('max_per_round').get('assignor',None)
    max_cpuh_block = UC.get('max_cpuh_block')
    random.shuffle( wfos )
    for wfo in wfos:
        
        if options.limit and (n_stalled+n_assigned)>options.limit:
            break

        if max_per_round and (n_stalled+n_assigned)>max_per_round:
            break

        if specific:
            if not any(map(lambda sp: sp in wfo.name, specific.split(','))): continue
            #if not specific in wfo.name: continue
        print "\n\n"
        wfh = workflowInfo( url, wfo.name)

        if options.priority and int(wfh.request['RequestPriority']) < options.priority:
            continue

        options_text=""
        if options.early: options_text+=", early option is ON"
        if options.partial: 
            options_text+=", partial option is ON"
            options_text+=", good fraction is %.2f"%options.good_enough
        


        wfh.sendLog('assignor',"%s to be assigned%s"%(wfo.name, options_text))

        ## the site whitelist takes into account siteInfo, campaignInfo, memory and cores
        (lheinput,primary,parent,secondary, sites_allowed) = wfh.getSiteWhiteList()
        output_tiers = list(set([o.split('/')[-1] for o in wfh.request['OutputDatasets']]))

        if not output_tiers:
            n_stalled+=1
            wfi.sendLog('assignor','There is no output at all')
            sendLog('assignor','Workflow %s has no output at all'%( wfo.name), level='critical')
            continue

        is_stuck = (all_stuck & primary)
        if is_stuck:
            wfh.sendLog('assignor',"%s are stuck input"%(','.join( is_stuck)))

        ## check if by configuration we gave it a GO
        no_go = False
        if not wfh.go(log=True) and not options.go:
            no_go = True

        allowed_secondary = {}
        assign_parameters = {}
        check_secondary = (not wfh.isRelval())
        for campaign in wfh.getCampaigns():
            if campaign in CI.campaigns:
                assign_parameters.update( CI.campaigns[campaign] )

            if campaign in CI.campaigns and 'secondaries' in CI.campaigns[campaign]:
                if CI.campaigns[campaign]['secondaries']:
                    allowed_secondary.update( CI.campaigns[campaign]['secondaries'] )
                    check_secondary = True
            if campaign in CI.campaigns and 'banned_tier' in CI.campaigns[campaign]:
                banned_tier = list(set(CI.campaigns[campaign]['banned_tier']) & set(output_tiers))
                if banned_tier:
                    no_go=True
                    wfh.sendLog('assignor','These data tiers %s are not allowed'%(','.join( banned_tier)))
                    sendLog('assignor','These data tiers %s are not allowed'%(','.join( banned_tier)), level='critical')

        if secondary and check_secondary:
            if (set(secondary)&set(allowed_secondary.keys())!=set(secondary)):
                wfh.sendLog('assignor','%s is not an allowed secondary'%(', '.join(set(secondary)-set(allowed_secondary.keys()))))
                sendLog('assignor','%s is not an allowed secondary'%(', '.join(set(secondary)-set(allowed_secondary.keys()))), level='critical')
                if not options.go:
                    no_go = True
            ## then get whether there is something more to be done by secondary
            for sec in secondary:
                if sec in allowed_secondary:# and 'parameters' in allowed_secondary[sec]:
                    assign_parameters.update( allowed_secondary[sec] )

        if no_go:
            n_stalled+=1
            ## make a very loud noise if >100k priority stalled
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


        original_sites_allowed = copy.deepcopy( sites_allowed )
        wfh.sendLog('assignor',"Site white list %s"%sorted(sites_allowed))
        override_sec_location = CI.get(wfh.request['Campaign'], 'SecondaryLocation', [])

        blocks = wfh.getBlockWhiteList()
        rwl = wfh.getRunWhiteList()
        if rwl:
            ## augment with run white list
            for dataset in primary:
                blocks = list(set( blocks + getDatasetBlocks( dataset, runs=rwl ) ))
        lwl = wfh.getLumiWhiteList()
        if lwl:
            ## augment with lumi white list
            for dataset in primary:
                blocks = list(set( blocks + getDatasetBlocks( dataset, lumis=lwl)))

        wfh.sendLog('assignor',"Allowed %s"%sorted(sites_allowed))
        secondary_locations=None

        primary_aaa = options.primary_aaa
        secondary_aaa = options.secondary_aaa
        do_partial = False #options.good_enough if options.partial else 0

        if 'Campaign' in wfh.request and wfh.request['Campaign'] in CI.campaigns:
            assign_parameters.update( CI.campaigns[wfh.request['Campaign']] )

        if 'primary_AAA' in assign_parameters:
            primary_aaa = primary_aaa or assign_parameters['primary_AAA']
        if 'secondary_AAA' in assign_parameters:
            secondary_aaa = secondary_aaa or assign_parameters['secondary_AAA']
        if 'partial_copy' in assign_parameters:
            ## can this only work if there is a stuck input ? maybe not
            ## this is a number. 0 means no
            print "Could do partial disk copy assignment"
            if is_stuck or options.partial:
                do_partial = assign_parameters['partial_copy']
                wfh.sendLog('assignor',"Overiding partial copy assignment to %.2f fraction"% do_partial)
                #sendEmail('stuck input to assignment','%s is stuck for assigning %s and going fractional'%(','.join( is_stuck), wfo.name))
            
        do_partial = options.good_enough if options.partial else do_partial


        for sec in list(secondary):
            if override_sec_location: 
                print "We don't care where the secondary is"
                print "Cannot pass for now"
                #sendEmail("tempting to pass sec location check","but we cannot yet IMO")
                #pass

            presence = getDatasetPresence( url, sec )
            print sec
            print json.dumps(presence, indent=2)
            one_secondary_locations = [site for (site,(there,frac)) in presence.items() if frac>98.]

            if secondary_aaa:
                if not one_secondary_locations:
                    sec_availability = getDatasetBlocksFraction( url, sec )
                    if sec_availability >=1. and options.go:
                        ## there is at least one copy of each block on disk. We should go ahead and let it go.
                        wfh.sendLog('assignor',"The secondary %s is available %s times on disk, and usable"%( sec, sec_availability))
                    else:
                        ## not even a copy on disk anywhere !!!!
                        sites_allowed = [] ## will block the assignment
                        wfh.sendLog('assignor',"The secondary %s is nowhere on disk"% sec)
                #just continue without checking
                continue

            #one_secondary_locations = [site for (site,(there,frac)) in presence.items() if there]
            if secondary_locations==None:
                secondary_locations = one_secondary_locations
            else:
                secondary_locations = list(set(secondary_locations) & set(one_secondary_locations))
            ## reduce the site white list to site with secondary only
            #sites_allowed = [site for site in sites_allowed if any([osite.startswith(site) for osite in one_secondary_locations])]
            sites_allowed = [site for site in sites_allowed if SI.CE_to_SE(site) in one_secondary_locations]
            
        wfh.sendLog('assignor',"From/after secondary requirement, now Allowed%s"%sorted(sites_allowed))

        initial_sites_allowed = copy.deepcopy( sites_allowed ) ## keep track of this, after secondary input location restriction : that's how you want to operate it

        sites_all_data = copy.deepcopy( sites_allowed )
        sites_with_data = copy.deepcopy( sites_allowed )
        sites_with_any_data = copy.deepcopy( sites_allowed )
        primary_locations = None
        available_fractions = {}
        set_lfn = '/store/mc' ## by default

        endpoints = set()
        for prim in list(primary):
            if prim in dataset_endpoints:
                print "endpoints from stagor",dataset_endpoints[prim]
                endpoints.update( dataset_endpoints[prim] )
            set_lfn = getLFNbase( prim )
            presence = getDatasetPresence( url, prim , only_blocks=blocks)
            if talk:
                print prim
                print json.dumps(presence, indent=2)
            available_fractions[prim] =  getDatasetBlocksFraction(url, prim, sites = [SI.CE_to_SE(site) for site in sites_allowed] , only_blocks = blocks)
            if primary_aaa:
                available_fractions[prim] =  getDatasetBlocksFraction(url, prim, only_blocks = blocks)

            sites_all_data = [site for site in sites_with_data if SI.CE_to_SE(site) in [psite for (psite,(there,frac)) in presence.items() if there]]
            if primary_aaa:
                sites_all_data = list(set([SI.SE_to_CE(psite) for (psite,(there,frac)) in presence.items() if there]))
            sites_with_data = [site for site in sites_with_data if SI.CE_to_SE(site) in [psite for (psite,frac) in presence.items() if frac[1]>90.]]
            sites_with_any_data = [site for site in sites_with_any_data if SI.CE_to_SE(site) in presence.keys()]
            if primary_aaa:
                sites_with_any_data = list(set([SI.SE_to_CE(psite) for psite in presence.keys()]))

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
                wfh.sendLog('assignor',"One of the usable site is in downtime %s"%([osite for osite in opportunistic_sites if osite in SI.sites_not_ready]))
                down_time = True
                ## should this be send back to considered ?
                

        ## should be 2 but for the time-being let's lower it to get things going
        copies_wanted,cpuh = wfh.getNCopies()
        wfh.sendLog('assignor',"we need %s CPUh"%cpuh)
        if cpuh>max_cpuh_block and not options.go:
            #sendEmail('large workflow','that wf %s has a large number of CPUh %s, not assigning, please check the logs'%(wfo.name, cpuh))#,destination=['Dmytro.Kovalskyi@cern.ch'])
            sendLog('assignor','%s requires a large numbr of CPUh %s , not assigning, please check with requester'%( wfo.name, cpuh), level='critical')
            wfh.sendLog('assignor',"Requiring a large number of CPUh %s, not assigning"%cpuh)
            continue

        if 'Campaign' in wfh.request and wfh.request['Campaign'] in CI.campaigns and 'maxcopies' in CI.campaigns[wfh.request['Campaign']]:
            copies_needed_from_campaign = CI.campaigns[wfh.request['Campaign']]['maxcopies']
            copies_wanted = min(copies_needed_from_campaign, copies_wanted)
        
        if not options.early:
            less_copies_than_requested = UC.get("less_copies_than_requested")
            copies_wanted = max(1,copies_wanted-less_copies_than_requested) # take one out for the efficiency
        else:
            ## find out whether there is a site in the whitelist, that is lacking jobs and reduce to 1 copy needed to get things going
            pass

        wfh.sendLog('assignor',"needed availability fraction %s"% copies_wanted)

        ## should also check on number of sources, if large enough, we should be able to overflow most, efficiently

        ## default back to white list to original white list with any data
        wfh.sendLog('assignor',"Allowed sites :%s"% sorted(sites_allowed))

        if primary_aaa:
            ## remove the sites not reachable localy if not in having the data
            if not sites_all_data:
                wfh.sendLog('assignor',"Overiding the primary on AAA setting to Off")
                primary_aaa=False
            else:
                aaa_grid = set(sites_all_data)
                for site in list(aaa_grid):
                    aaa_grid.update( aaa_mapping.get(site,[]) )
                sites_allowed = list(set(initial_sites_allowed) & aaa_grid)
                wfh.sendLog('assignor',"Selected to read primary through xrootd %s"%sorted(sites_allowed))
                
        if not primary_aaa:
            sites_allowed = sites_with_any_data
            wfh.sendLog('assignor',"Selected for any data %s"%sorted(sites_allowed))

        ### check on endpoints for on-going transfers
        if do_partial:
            if endpoints:
                end_sites = [SI.SE_to_CE(s) for s in endpoints]
                sites_allowed = list(set(sites_allowed + end_sites))
                if down_time and not any(osite in SI.sites_not_ready for osite in end_sites):
                    print "Flip the status of downtime, since our destinations are good"
                    down_time = False
                print "with added endpoints",sorted(end_sites)
            else:
                print "Cannot do partial assignment without knowin the endpoints"
                n_stalled+=1
                continue
            
            
        #if not len(sites_allowed):
        #    if not options.early:
        #        wfh.sendLog('assignor',"cannot be assign with no matched sites")
        #        sendLog('assignor','%s has no whitelist'% wfo.name, level='critical')
        #    n_stalled+=1
        #    continue


        low_pressure = SI.sites_low_pressure(0.4)
        ## if any of the site allowed is low pressure : reduce to 1 copy so that it gets started
        allowed_and_low = sorted(set(low_pressure) & set(sites_allowed))
        if allowed_and_low:
            wfh.sendLog('assignor',"The workflow can run at %s under low pressure currently"%( ','.join( allowed_and_low )))
            copies_wanted = max(1., copies_wanted-1.)


        if available_fractions and not all([available>=copies_wanted for available in available_fractions.values()]):
            not_even_once = not all([available>=1. for available in available_fractions.values()])
            above_good = all([available >= do_partial for available in available_fractions.values()])
            wfh.sendLog('assignor',"The input dataset is not available %s times, only %s"%( copies_wanted, available_fractions.values()))
            if down_time and not options.go and not options.early:
                wfo.status = 'considered'
                session.commit()
                wfh.sendLog('assignor',"sending back to considered because of site downtime, instead of waiting")
                #sendEmail( "cannot be assigned due to downtime","%s is not sufficiently available, due to down time of a site in the whitelist. check the assignor logs. sending back to considered."% wfo.name)
                sendLog('assignor','%s is not sufficiently available, due to down time of a site in the whitelist. sending back to considered.'%( wfo.name ), level='delay')
                n_stalled+=1
                continue
                #pass

            print json.dumps(available_fractions)
            if (options.go and not_even_once) or not options.go:
                known = []
                try:
                    known = json.loads(open('cannot_assign.json').read())
                except:
                    pass
                if not wfo.name in known and not options.limit and not options.go and not options.early and not (do_partial and above_good):
                    wfh.sendLog('assignor',"cannot be assigned, %s is not sufficiently available.\n %s"%(wfo.name,json.dumps(available_fractions)))
                    #sendEmail( "cannot be assigned","%s is not sufficiently available.\n %s"%(wfo.name,json.dumps(available_fractions)))
                    known.append( wfo.name )
                    open('cannot_assign.json','w').write(json.dumps( known, indent=2))
                
                if options.early:
                    if wfo.status == 'considered':
                        wfh.sendLog('assignor',"setting considered-tried")
                        wfo.status = 'considered-tried'
                        session.commit()
                    else:
                        print "tried but status is",wfo.status
                if do_partial and above_good:
                    print "Will move on with partial locations"
                else:
                    n_stalled+=1
                    continue

        if not len(sites_allowed):
            if not options.early:
                wfh.sendLog('assignor',"cannot be assign with no matched sites")
                sendLog('assignor','%s has no whitelist'% wfo.name, level='critical')
            n_stalled+=1
            continue


        t1_only = [ce for ce in sites_allowed if ce.startswith('T1')]
        if t1_only:
            # try to pick from T1 only first
            sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in t1_only])]
        else:
            # then pick any otherwise
            sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in sites_allowed])]
            
            
        wfh.sendLog('assignor',"Placing the output on %s"%sites_out)
        parameters={
            'SiteWhitelist' : sites_allowed,
            'NonCustodialSites' : sites_out,
            'AutoApproveSubscriptionSites' : list(set(sites_out)),
            'AcquisitionEra' : wfh.acquisitionEra(),
            'ProcessingString' : wfh.processingString(),
            'MergedLFNBase' : set_lfn,
            'ProcessingVersion' : version,
            }

        if primary_aaa:
            parameters['TrustSitelists'] = True
            wfh.sendLog('assignor',"Reading primary through xrootd at %s"%sorted(sites_allowed))            

        if secondary_aaa:
            parameters['TrustPUSitelists'] = True
            wfh.sendLog('assignor',"Reading secondary through xrootd at %s"%sorted(sites_allowed))            

        ## plain assignment here
        team='production'
        if os.getenv('UNIFIED_TEAM'): team = os.getenv('UNIFIED_TEAM')
        if options and options.team:
            team = options.team
        parameters['Team'] = team


        if lheinput:
            ## throttle reading LHE article 
            wfh.sendLog('assignor', 'Setting the number of events per job to 500k max')
            parameters['EventsPerJob'] = 500000

        def pick_options(options, parameters):
            ##parse options entered in command line if any
            if options:
                for key in reqMgrClient.assignWorkflow.keys:
                    v=getattr(options,key)
                    if v!=None:
                        if type(v)==str and ',' in v: 
                            parameters[key] = filter(None,v.split(','))
                        else: 
                            parameters[key] = v

        def pick_campaign( assign_parameters, parameters):
            ## pick up campaign specific assignment parameters
            parameters.update( assign_parameters.get('parameters',{}) )

        if options.force_options:
            pick_campaign( assign_parameters, parameters)
            pick_options(options, parameters)
        else:
            ## campaign parameters update last
            pick_options(options, parameters)
            pick_campaign( assign_parameters, parameters)

        if not options.test:
            parameters['execute'] = True

        hold_split, split_check = wfh.checkSplitting()
        if hold_split and not options.go:
            if split_check:
                wfh.sendLog('assignor','Holding on to the change in splitting %s'%( '\n\n'.join([str(i) for i in split_check])))
            else:
                wfh.sendLog('assignor','Change of splitting is on hold')                
            n_stalled+=1
            continue            

        if split_check==None or split_check==False:
            n_stalled+=1
            continue
        elif split_check:
            ## operate all recommended changes
            reqMgrClient.setWorkflowSplitting(url, 
                                              wfo.name,
                                              split_check)
            wfh.sendLog('assignor','Applying the change in splitting %s'%( '\n\n'.join([str(i) for i in split_check])))

        split_check = True ## bypass completely and use the above
        """
        if split_check!=True:
            parameters.update( split_check )
            if 'NoGo' in split_check.values():
                wfh.sendLog('assignor', "Failing splitting check")
                sendLog('assignor','the workflow %s is failing the splitting check. Verify in the logs'% wfo.name, level='critical')
                n_stalled+=1
                continue

            if 'EventBased' in split_check.values():
                wfh.sendLog('assignor', "Falling back to event splitting.")
                #sendEmail("Fallback to EventBased","the workflow %s is too heavy to be processed as it is. Fallback to EventBased splitting"%wfo.name)
                sendLog('assignor','the workflow %s is too heavy to be processed as it is. Fallback to EventBased splitting ?'%wfo.name, level='critical')
                ## we have a problem here, that EventBased should never be used as a backup
                if not options.go:  
                    n_stalled+=1
                    continue
                continue ## skip all together
            elif 'EventsPerJob' in split_check.values():
                wfh.sendLog('assignor', "Modifying the number of events per job")
                #sendEmail("Modifying the job per events","the workflow %s is too heavy in number of jobs explosion"%wfo.name)
                sendLog('assignor',"the workflow %s is too heavy in number of jobs explosion"%wfo.name, level='critical')
            elif 'EventsPerLumi' in split_check.values():
                wfh.sendLog('assignor', "Modifying the number of events per lumi to be able to process this")
        """

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
                    #sendEmail("issue with event splitting for run-dependent MC","%s needs to be split by event with %s per job"%(wfo.name, eventsPerJob))
                    sendLog('assignor', "%s needs to be split by event with %s per job"%(wfo.name, eventsPerJob), level='critical')
                    wfh.sendLog('assignor', "%s needs to be split by event with %s per job"%(wfo.name, eventsPerJob))
                    parameters['EventsPerJob'] = eventsPerJob
                else:
                    spl = wfh.getSplittings()[0]
                    eventsPerJobEstimated = spl['events_per_job'] if 'events_per_job' in spl else None
                    eventsPerJobEstimated = spl['avg_events_per_job'] if 'avg_events_per_job' in spl else None
                    if eventsPerJobEstimated and eventsPerJobEstimated > eventsPerJob:
                        #sendEmail("setting lumi splitting for run-dependent MC","%s was assigned with %s lumis/job"%( wfo.name, lumisPerJob))
                        sendLog('assignor',"%s was assigned with %s lumis/job"%( wfo.name, lumisPerJob), level='critical')
                        wfh.sendLog('assignor',"%s was assigned with %s lumis/job"%( wfo.name, lumisPerJob))
                        parameters['LumisPerJob'] = lumisPerJob
                    else:
                        #sendEmail("leaving splitting untouched for PU_RD*","please check on "+wfo.name)
                        sendLog('assignor',"leaving splitting untouched for %s, please check on %s"%( pstring, wfo.name), level='critical')
                        wfh.sendLog('assignor',"leaving splitting untouched for PU_RD*, please check.")


        
        
        result = reqMgrClient.assignWorkflow(url, wfo.name, None, parameters) ## team is not relevant anymore here


        # set status
        if not options.test:
            if result:
                wfo.status = 'away'
                session.commit()
                n_assigned+=1
                wfh.sendLog('assignor',"Properly assigned\n%s"%(json.dumps( parameters, indent=2)))
                try:
                    ## refetch information and lock output
                    new_wfi = workflowInfo( url, wfo.name)
                    (_,prim,_,sec) = new_wfi.getIO()
                    for secure in list(prim)+list(sec)+new_wfi.request['OutputDatasets']:
                        ## lock all outputs flat
                        #NLI.lock( secure )
                        LI.lock( secure, reason = 'assigning')
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
                wfh.sendLog('assignor',"Failed to assign %s.\n%s \n Please check the logs"%(wfo.name, reqMgrClient.assignWorkflow.errorMessage))
                sendLog('assignor',"Failed to assign %s.\n%s \n Please check the logs"%(wfo.name, reqMgrClient.assignWorkflow.errorMessage), level='critical')
                print "ERROR could not assign",wfo.name
        else:
            pass
    print "Assignment summary:"
    sendLog('assignor',"Assigned %d Stalled %s"%(n_assigned, n_stalled))
    if n_stalled and not options.go and not options.early:
        sendLog('assignor',"%s workflows cannot be assigned. Please take a look"%(n_stalled), level='critical')
    
if __name__=="__main__":
    url = reqmgr_url
    parser = optparse.OptionParser()
    parser.add_option('-t','--test', help='Only test the assignment',action='store_true',dest='test',default=False)
    parser.add_option('-e', '--early', help='Fectch from early statuses',default=False, action="store_true")
    parser.add_option('-p', '--partial', help='Let the workflow assign to place with any part of the data, existent of being made',default=False, action="store_true")
    parser.add_option('--good_enough', help='Only useful with --partial option, determines whether to get the workflow started', default=0.5, type=float)
    parser.add_option('--go',help="Overrides the campaign go",default=False,action='store_true')
    parser.add_option('--team',help="Specify the agent to use",default=None)
    parser.add_option('--primary_aaa',help="Force to use the secondary location restriction, if any, and use the full site whitelist initially provided to run that type of wf",default=False, action='store_true')
    parser.add_option('--secondary_aaa',help="Force to use the primary location restriction",default=False, action='store_true')
    parser.add_option('--limit',help="Limit the number of wf to be assigned",default=0,type='int')
    parser.add_option('--priority',help="Lower limit on priority of wf to be assigned", default=0, type='int')
    parser.add_option('--from_status',help="The unified status we should try to assign from", default=None)
    parser.add_option('--force_options', help="Use the command line options as last modifiers", default=False, action='store_true')

    for key in reqMgrClient.assignWorkflow.keys:
        parser.add_option('--%s'%key,help="%s Parameter of request manager assignment interface"%key, default=None)
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    assignor(url,spec, options=options)

    if not spec:
        htmlor()
