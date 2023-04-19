#!/usr/bin/env python
from assignSession import *
import reqMgrClient
from utils import workflowInfo, campaignInfo, siteInfo, userLock, unifiedConfiguration, reqmgr_url, monitor_pub_dir, \
    monitor_dir, global_SI
from utils import getDatasetEventsPerLumi, getLFNbase, lockInfo, do_html_in_each_module
from utils import componentInfo, sendEmail, sendLog, getWorkflows, eosRead
# from utils import lockInfo
from utils import moduleLock
import optparse
import random
import json
import copy
import os
import sys
import traceback

sys.path.append('..')
from MSPileupClient import MSPileupClient


def findKeys(key, dictionary):
    values = set()
    for k, v in dictionary.items():
        if type(v) is dict:
            for k2, v2 in v.items():
                if k2 == key:
                    values.add(v2)
        elif k == key:
            values.add(v)
    return list(values)


def uploadRelValPileup(workflow):

    try:
        mspileupClient = MSPileupClient(url="cmsweb.cern.ch")
        pileups = findKeys("MCPileup", workflow.request)

        for pileup in pileups:

            pileupDocument = {
                "pileupName": pileup,
                "pileupType": "premix",  # TODO: should be pileupDetails[pileupType]
                "expectedRSEs": ["T2_CH_CERN"],  # TODO: make it generic
                "campaigns": [],
                "active": True
            }

            responseToGET = MSPileupClient.getByPileupName(pileup)["result"]
            if not responseToGET:
                print ("This pileup doesn't exist in MSPileup, starting the creation")
                responseToPOST = mspileupClient.createPileupDocument(pileupDocument)
                if responseToPOST:
                    print ("Response for the create POST call:")
                    print (responseToPOST)
                else:
                    print ("Pileup creation failed")
            else:
                print ("Pileup document exists, updating")
                responseToPUT = mspileupClient.updatePileupDocument(pileupDocument)
                print ("Response for the update PUT call:")
                print(responseToPUT)
        return True

    except Exception as e:
        print ("Exception while PUT request")
        print (str(e))
        print(traceback.format_exc())
        return False


def get_priority_block(priority):
    if priority >= 110000:
        return "block1"
    elif priority >= 90000:
        return "block2"
    elif priority >= 85000:
        return "block3"
    elif priority >= 80000:
        return "block4"
    elif priority >= 70000:
        return "block5"
    elif priority >= 63000:
        return "block6"
    else:
        return "block7"


def get_workflow_count_by_status(status):
    workflows = getWorkflows(reqmgr_url, status, details=False)
    return len(workflows)


def get_pending_workflow_count():
    statuses = ['assigned', 'staging', 'staged', 'acquired']
    count = 0
    for status in statuses:
        count += get_workflow_count_by_status(status)
    return count


def assignor(url, specific=None, talk=True, options=None):
    if userLock() and not options.manual: return
    mlock = moduleLock()
    if mlock() and not options.manual: return
    if not componentInfo().check() and not options.manual: return

    UC = unifiedConfiguration()
    CI = campaignInfo()
    SI = siteInfo()
    SI = global_SI()
    ###NLI = newLockInfo()
    ###if not NLI.free() and not options.go: return
    LI = lockInfo()
    # if not LI.free() and not options.go and not options.manual: return

    n_assigned = 0
    n_stalled = 0

    wfos = []
    fetch_from = []
    if specific or options.early:
        fetch_from.extend(['considered', 'staging'])
    if specific:
        fetch_from.extend(['considered-tried'])

    if options.early:
        print("Option Early is on")

    fetch_from.extend(['staged'])

    if options.from_status:
        fetch_from = options.from_status.split(',')
        print("Overriding to read from", fetch_from)

    for status in fetch_from:
        print("getting wf in", status)
        wfos.extend(session.query(Workflow).filter(Workflow.status == status).all())
        print(len(wfos))

    ## in case of partial, go for fetching a list from json ?
    # if options.partial and not specific:
    #    pass

    #aaa_mapping = json.loads(eosRead('%s/equalizor.json' % monitor_pub_dir))['mapping']
    #all_stuck = set()
    #all_stuck.update(json.loads(eosRead('%s/stuck_transfers.json' % monitor_pub_dir)))

    max_per_round = UC.get('max_per_round').get('assignor', None)
    max_cpuh_block = UC.get('max_cpuh_block')

    ##order by priority instead of random
    cache = sorted(getWorkflows(url, 'assignment-approved', details=True), key=lambda r: r['RequestPriority'])
    cache = [r['RequestName'] for r in cache]

    def rank(wfn):
        return cache.index(wfn) if wfn in cache else 0

    wfos = sorted(wfos, key=lambda wfo: rank(wfo.name), reverse=True)
    print("10 first", [wfo.name for wfo in wfos[:10]])
    print("10 last", [wfo.name for wfo in wfos[-10:]])

    pending_workflow_count = get_pending_workflow_count()
    # In how many workflows are we going to update the pending_workflow_count?
    count_update_frequency = 100
    workflow_count = 0

    max_per_round = UC.get('max_per_round').get('assignor', None)
    wfos = wfos[:max_per_round]

    for wfo in wfos:

        if options.limit and (n_stalled + n_assigned) > options.limit:
            break

        if specific:
            if not any([sp in wfo.name for sp in specific.split(',')]): continue
            # if not specific in wfo.name: continue

        if not options.manual and 'rucio' in (wfo.name).lower(): continue
        print("\n\n")
        wfh = workflowInfo(url, wfo.name)

        try:
            if wfh.request["PrepID"] in UC.get('prepIDs_to_skip'):
                continue
        except:
            pass

        if wfh.request['RequestStatus'] in ['rejected', 'aborted', 'aborted-completed', 'aborted-archived',
                                            'rejected-archived'] and wfh.isRelval():
            wfo.status = 'forget'
            session.commit()
            n_stalled += 1
            continue

        if options.priority and int(wfh.request['RequestPriority']) < int(options.priority):
            wfh.sendLog('assignor', "Priority mode is ON: Stalling this request since its priority is below %s" % (
                str(options.priority)))
            print("Priority mode is ON: Stalling " + str(
                wfh.request['RequestName']) + " since its priority is below " + str(options.priority))
            continue

        workflow_count += 1
        if not workflow_count % count_update_frequency:
            print ('Pending workflow count is updated')
            pending_workflow_count = get_pending_workflow_count()

        # Control to keep the number of workflows in acquired under a limit
        priority_block = get_priority_block(int(wfh.request['RequestPriority']))
        acquired_threshold = UC.get('acquired_threshold_per_priority_block').get(priority_block, None)
        if pending_workflow_count > acquired_threshold:
            # Stalling the assignment
            wfh.sendLog('assignor',
                        "Pending workflow check in ON: Stalling the assignment for " + str(wfh.request['RequestName']))
            wfh.sendLog('assignor',
                        "The number of pending workflows ([assigned,acquired]): " + str(pending_workflow_count))
            wfh.sendLog('assignor', "Threshold for the priority block of this request: " + str(acquired_threshold))
            continue
        else:
            # Okay to assign
            print(("Pending workflow check in ON: The following request has passed the check - okay to assign: " + str(
                wfh.request['RequestName'])))
            wfh.sendLog('assignor',
                        "Pending workflow check in ON: The following request has passed the check - okay to assign: " + str(
                            wfh.request['RequestName']))

        options_text = ""
        if options.early: options_text += ", early option is ON"

        wfh.sendLog('assignor', "%s to be assigned %s" % (wfo.name, options_text))

        ## the site whitelist takes into account siteInfo, campaignInfo, memory and cores
        (lheinput, primary, parent, secondary, sites_allowed, sites_not_allowed) = wfh.getSiteWhiteList()

        output_tiers = list(set([o.split('/')[-1] for o in wfh.request['OutputDatasets']]))

        if not output_tiers:
            n_stalled += 1
            wfh.sendLog('assignor', 'There is no output at all')
            sendLog('assignor', 'Workflow %s has no output at all' % (wfo.name), level='critical')
            continue

        #is_stuck = (all_stuck & primary)
        #if is_stuck:
        #    wfh.sendLog('assignor', "%s are stuck input" % (','.join(is_stuck)))

        ## check if by configuration we gave it a GO
        no_go = False
        if not wfh.go(log=True) and not options.go:
            no_go = True

        allowed_secondary = {}
        assign_parameters = {}
        check_secondary = (not wfh.isRelval())
        for campaign in wfh.getCampaigns():
            if campaign in CI.campaigns:
                assign_parameters.update(CI.campaigns[campaign])

            if campaign in CI.campaigns and 'secondaries' in CI.campaigns[campaign]:
                if CI.campaigns[campaign]['secondaries']:
                    allowed_secondary.update(CI.campaigns[campaign]['secondaries'])
                    check_secondary = True
            if campaign in CI.campaigns and 'banned_tier' in CI.campaigns[campaign]:
                banned_tier = list(set(CI.campaigns[campaign]['banned_tier']) & set(output_tiers))
                if banned_tier:
                    no_go = True
                    wfh.sendLog('assignor', 'These data tiers %s are not allowed' % (','.join(banned_tier)))
                    sendLog('assignor', 'These data tiers %s are not allowed' % (','.join(banned_tier)),
                            level='critical')

        if secondary and check_secondary:
            if (set(secondary) & set(allowed_secondary.keys()) != set(secondary)):
                msg = '%s is not an allowed secondary' % (', '.join(set(secondary) - set(allowed_secondary.keys())))
                wfh.sendLog('assignor', msg)
                critical_msg = msg + '\nWorkflow URL: https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=task_{}'.format(
                    wfh.getPrepIDs()[0])
                sendLog('assignor', critical_msg, level='critical')
                if not options.go:
                    no_go = True
            ## then get whether there is something more to be done by secondary
            for sec in secondary:
                if sec in allowed_secondary:  # and 'parameters' in allowed_secondary[sec]:
                    assign_parameters.update(allowed_secondary[sec])

        if no_go:
            n_stalled += 1
            ## make a very loud noise if >100k priority stalled
            continue

        ## check on current status for by-passed assignment
        if wfh.request['RequestStatus'] != 'assignment-approved':
            if not options.test:
                wfh.sendLog('assignor', "setting %s away and skipping" % wfo.name)
                ## the module picking up from away will do what is necessary of it
                wfo.wm_status = wfh.request['RequestStatus']
                wfo.status = 'away'
                session.commit()
                continue
            else:
                print(wfo.name, wfh.request['RequestStatus'])

        ## retrieve from the schema, dbs and reqMgr what should be the next version
        version = wfh.getNextVersion()
        if not version:
            if options and options.ProcessingVersion:
                version = options.ProcessingVersion
            else:
                wfh.sendLog('assignor', "cannot decide on version number")
                n_stalled += 1
                wfo.status = 'trouble'
                session.commit()
                continue

        wfh.sendLog('assignor', "Site white list %s" % sorted(sites_allowed))

        blocks = wfh.getBlocks()
        if blocks:
            wfh.sendLog('assignor', "Needs {} blocks in input {}".format(len(blocks), '\n'.join(blocks)))
        wfh.sendLog('assignor', "Allowed %s" % sorted(sites_allowed))

        primary_aaa = options.primary_aaa
        secondary_aaa = options.secondary_aaa

        if 'Campaign' in wfh.request and wfh.request['Campaign'] in CI.campaigns:
            assign_parameters.update(CI.campaigns[wfh.request['Campaign']])

        if 'primary_AAA' in assign_parameters and primary:
            primary_aaa = primary_aaa or assign_parameters['primary_AAA']
        if 'secondary_AAA' in assign_parameters:
            secondary_aaa = secondary_aaa or assign_parameters['secondary_AAA']

        wfh.sendLog('assignor', "Initial values for primary_AAA=%s and secondary_AAA=%s" % (primary_aaa, secondary_aaa))

        if primary_aaa:
            if "T2_CH_CERN_HLT" in sites_allowed:
                sites_allowed.remove("T2_CH_CERN_HLT")
            if "T2_CH_CERN_HLT" not in sites_not_allowed:
                sites_not_allowed.append("T2_CH_CERN_HLT")
            if "T2_CH_CERN_P5" in sites_allowed:
                sites_allowed.remove("T2_CH_CERN_P5")
            if "T2_CH_CERN_P5" not in sites_not_allowed:
                sites_not_allowed.append("T2_CH_CERN_P5")

        ## keep track of this, after secondary input location restriction : that's how you want to operate it
        initial_sites_allowed = copy.deepcopy(sites_allowed)

        set_lfn = '/store/mc'  ## by default

        for prim in list(primary):
            set_lfn = getLFNbase(prim)
            ## if they are requested for processing, they should bbe all closed already
            # FIXME: remove this closeAllBlocks
            # closeAllBlocks(url, prim, blocks)

        ## should be 2 but for the time-being let's lower it to get things going
        _copies_wanted, cpuh = wfh.getNCopies()
        wfh.sendLog('assignor', "we need %s CPUh" % cpuh)
        if cpuh > max_cpuh_block and not options.go:
            # sendEmail('large workflow','that wf %s has a large number of CPUh %s, not assigning, please check the logs'%(wfo.name, cpuh))#,destination=['Dmytro.Kovalskyi@cern.ch'])
            sendLog('assignor', '%s requires a large numbr of CPUh %s , not assigning, please check with requester' % (
            wfo.name, cpuh), level='critical')
            wfh.sendLog('assignor', "Requiring a large number of CPUh %s, not assigning" % cpuh)
            continue

        ## should also check on number of sources, if large enough, we should be able to overflow most, efficiently

        ## default back to white list to original white list with any data
        wfh.sendLog('assignor', "Allowed sites :%s" % sorted(sites_allowed))

        # TODO Alan on 1/april/2020: keep the AAA functionality
        #if primary_aaa:
            ## remove the sites not reachable localy if not in having the data
        #    if not sites_allowed:
        #        wfh.sendLog('assignor', "Overiding the primary on AAA setting to Off")
        #        primary_aaa = False
        #    else:
        #        aaa_grid = set(sites_allowed)
        #        for site in list(aaa_grid):
        #            aaa_grid.update(aaa_mapping.get(site, []))
        #        sites_allowed = list(set(initial_sites_allowed) & aaa_grid)
        #        wfh.sendLog('assignor', "Selected to read primary through xrootd %s" % sorted(sites_allowed))

        isStoreResults = ('StoreResults' == wfh.request.setdefault('RequestType', None))

        if isStoreResults:
            if 'MergedLFNBase' in wfh.request:
                set_lfn = wfh.request['MergedLFNBase']
            else:
                n_stalled += 1
                wfh.sendLog('assignor', "Cannot assign StoreResults request because MergedLFN is missing")
                sendLog('assignor', 'Cannot assign StoreResults request because MergedLFN is missing', level='critical')
                continue

        if not primary_aaa:
            if isStoreResults:
                ## if we are dealing with a StoreResults request, we don't need to check dataset availability and
                ## should use the SiteWhiteList set in the original request
                if 'SiteWhitelist' in wfh.request:
                    sites_allowed = wfh.request['SiteWhitelist']
                else:
                    wfh.sendLog('assignor', "Cannot assign StoreResults request because SiteWhitelist is missing")
                    sendLog('assignor', 'Cannot assign StoreResults request because SiteWhitelist is missing',
                            level='critical')
                    n_stalled += 1
                    continue
            wfh.sendLog('assignor', "Selected for any data %s" % sorted(sites_allowed))

        # if not len(sites_allowed):
        #    if not options.early:
        #        wfh.sendLog('assignor',"cannot be assign with no matched sites")
        #        sendLog('assignor','%s has no whitelist'% wfo.name, level='critical')
        #    n_stalled+=1
        #    continue

        if not len(sites_allowed) and not options.SiteWhitelist:
            if not options.early:
                wfh.sendLog('assignor', "cannot be assign with no matched sites")
                sendLog('assignor', '%s has no whitelist' % wfo.name, level='critical')
            n_stalled += 1
            continue

        t1t2_only = [ce for ce in sites_allowed if [ce.startswith('T1') or ce.startswith('T2')]]
        if t1t2_only:
            # try to pick from T1T2 only first
            sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in t1t2_only])]
            # then pick any otherwise
        else:
            sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in sites_allowed])]

        print("available=", SI.disk[sites_out[0]])
        wfh.sendLog('assignor', "Placing the output on %s" % sites_out)
        parameters = {
            'SiteWhitelist': sites_allowed,
            'SiteBlacklist': sites_not_allowed,
            'NonCustodialSites': sites_out,
            'AutoApproveSubscriptionSites': list(set(sites_out)),
            'AcquisitionEra': wfh.acquisitionEra(),
            'ProcessingString': wfh.processingString(),
            'MergedLFNBase': set_lfn,
            'ProcessingVersion': version,
        }

        if primary_aaa:
            parameters['TrustSitelists'] = True
            wfh.sendLog('assignor', "Reading primary through xrootd at %s" % sorted(sites_allowed))

        if secondary_aaa:
            # Do not set TrustPUSitelist to True if there is no secondary
            if secondary:
                parameters['TrustPUSitelists'] = True
                wfh.sendLog('assignor', "Reading secondary through xrootd at %s" % sorted(sites_allowed))

        if wfh.isRelval():
            result = uploadRelValPileup(wfh)
            if not result:
                print("Couldn't upload pileup document to MSPileup for " + str(wfh.request['RequestName']))
                wfh.sendLog('assignor',"Couldn't upload pileup document to MSPileup for " + str(wfh.request['RequestName']))
                wfh.sendLog('assignor',"Stalling the assignment")
                continue

        ## plain assignment here
        team = 'production'
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
                    v = getattr(options, key)
                    if v != None:
                        if type(v) == str and ',' in v:
                            parameters[key] = [_f for _f in v.split(',') if _f]
                        else:
                            parameters[key] = v

        def pick_campaign(assign_parameters, parameters):
            ## pick up campaign specific assignment parameters
            parameters.update(assign_parameters.get('parameters', {}))

        if options.force_options:
            pick_campaign(assign_parameters, parameters)
            pick_options(options, parameters)
        else:
            ## campaign parameters update last
            pick_options(options, parameters)
            pick_campaign(assign_parameters, parameters)

        if not options.test:
            parameters['execute'] = True

        hold_split, split_check = wfh.checkSplitting()
        if hold_split and not options.go:
            if split_check:
                wfh.sendLog('assignor',
                            'Holding on to the change in splitting %s' % ('\n\n'.join([str(i) for i in split_check])))
            else:
                wfh.sendLog('assignor', 'Change of splitting is on hold')
            n_stalled += 1
            continue

        if split_check == None or split_check == False:
            n_stalled += 1
            continue
        elif split_check:
            ## operate all recommended changes
            reqMgrClient.setWorkflowSplitting(url,
                                              wfo.name,
                                              split_check)
            wfh.sendLog('assignor',
                        'Applying the change in splitting %s' % ('\n\n'.join([str(i) for i in split_check])))

        split_check = True  ## bypass completely and use the above

        # Handle run-dependent MC
        pstring = wfh.processingString()
        if 'PU_RD' in pstring:
            numEvents = wfh.getRequestNumEvents()
            eventsPerLumi = [getDatasetEventsPerLumi(prim) for prim in primary]
            eventsPerLumi = sum(eventsPerLumi) / float(len(eventsPerLumi))
            reqJobs = 500
            if 'PU_RD2' in pstring:
                reqJobs = 2000
                eventsPerJob = int(numEvents / (reqJobs * 1.4))
                lumisPerJob = int(eventsPerJob / eventsPerLumi)
                if lumisPerJob == 0:
                    # sendEmail("issue with event splitting for run-dependent MC","%s needs to be split by event with %s per job"%(wfo.name, eventsPerJob))
                    sendLog('assignor', "%s needs to be split by event with %s per job" % (wfo.name, eventsPerJob),
                            level='critical')
                    wfh.sendLog('assignor', "%s needs to be split by event with %s per job" % (wfo.name, eventsPerJob))
                    parameters['EventsPerJob'] = eventsPerJob
                else:
                    spl = wfh.getSplittings()[0]
                    # FIXME: decide which of the lines below needs to remain...
                    eventsPerJobEstimated = spl['events_per_job'] if 'events_per_job' in spl else None
                    eventsPerJobEstimated = spl['avg_events_per_job'] if 'avg_events_per_job' in spl else None
                    if eventsPerJobEstimated and eventsPerJobEstimated > eventsPerJob:
                        # sendEmail("setting lumi splitting for run-dependent MC","%s was assigned with %s lumis/job"%( wfo.name, lumisPerJob))
                        sendLog('assignor', "%s was assigned with %s lumis/job" % (wfo.name, lumisPerJob),
                                level='critical')
                        wfh.sendLog('assignor', "%s was assigned with %s lumis/job" % (wfo.name, lumisPerJob))
                        parameters['LumisPerJob'] = lumisPerJob
                    else:
                        # sendEmail("leaving splitting untouched for PU_RD*","please check on "+wfo.name)
                        sendLog('assignor',
                                "leaving splitting untouched for %s, please check on %s" % (pstring, wfo.name),
                                level='critical')
                        wfh.sendLog('assignor', "leaving splitting untouched for PU_RD*, please check.")

        ## make sure to autoapprove all NonCustodialSites
        parameters['AutoApproveSubscriptionSites'] = list(
            set(parameters['NonCustodialSites'] + parameters.get('AutoApproveSubscriptionSites', [])))
        result = reqMgrClient.assignWorkflow(url, wfo.name, None, parameters)  ## team is not relevant anymore here

        # set status
        if not options.test:
            if result:
                wfo.status = 'away'
                session.commit()
                n_assigned += 1
                wfh.sendLog('assignor', "Properly assigned\n%s" % (json.dumps(parameters, indent=2)))
                if wfh.producePremix() and (not wfh.isRelval()):
                    title = "Heavy workflow assigned to {}".format(parameters['SiteWhitelist'])
                    body = "Workflow name: {}".format(wfh.request['RequestName'])
                    body += "\nOutput dataset(s): {}".format(wfh.request['OutputDatasets'])
                    body += "\nAssigned to: {}".format(parameters['SiteWhitelist'])
                    sendEmail(title, body, destination=['cms-production-heavy-workflow-notifications@cern.ch'])

                try:
                    ## refetch information and lock output
                    new_wfi = workflowInfo(url, wfo.name)
                    (_, prim, _, sec) = new_wfi.getIO()
                    for secure in list(prim) + list(sec) + new_wfi.request['OutputDatasets']:
                        ## lock all outputs
                        LI.lock(secure, reason='assigning')

                except Exception as e:
                    print("fail in locking output")

                    print(str(e))
                    sendEmail("failed locking of output", str(e))


            else:
                wfh.sendLog('assignor', "Failed to assign %s.\n%s \n Please check the logs" % (
                wfo.name, reqMgrClient.assignWorkflow.errorMessage))
                sendLog('assignor', "Failed to assign %s.\n%s \n Please check the logs" % (
                wfo.name, reqMgrClient.assignWorkflow.errorMessage), level='critical')
                print("ERROR could not assign", wfo.name)
        else:
            pass
    print("Assignment summary:")
    sendLog('assignor', "Assigned %d Stalled %s" % (n_assigned, n_stalled))
    if n_stalled and not options.go and not options.early:
        sendLog('assignor', "%s workflows cannot be assigned. Please take a look" % (n_stalled), level='critical')


if __name__ == "__main__":
    url = reqmgr_url
    parser = optparse.OptionParser()
    parser.add_option('-t', '--test', help='Only test the assignment', action='store_true', dest='test', default=False)
    parser.add_option('-m', '--manual', help='Manual assignment, bypassing lock check', action='store_true',
                      dest='manual', default=False)
    parser.add_option('-e', '--early', help='Fectch from early statuses', default=False, action="store_true")
    parser.add_option('--go', help="Overrides the campaign go", default=False, action='store_true')
    parser.add_option('--team', help="Specify the agent to use", default=None)
    parser.add_option('--primary_aaa',
                      help="Force to use the secondary location restriction, if any, and use the full site whitelist initially provided to run that type of wf",
                      default=False, action='store_true')
    parser.add_option('--secondary_aaa', help="Force to use the primary location restriction", default=False,
                      action='store_true')
    parser.add_option('--limit', help="Limit the number of wf to be assigned", default=0, type='int')
    parser.add_option('--priority', help="Lower limit on priority of wf to be assigned", default=0, type='int')
    parser.add_option('--from_status', help="The unified status we should try to assign from", default=None)
    parser.add_option('--force_options', help="Use the command line options as last modifiers", default=False,
                      action='store_true')

    for key in reqMgrClient.assignWorkflow.keys:
        parser.add_option('--%s' % key, help="%s Parameter of request manager assignment interface" % key, default=None)
    (options, args) = parser.parse_args()

    spec = None
    if len(args) != 0:
        spec = args[0]

    assignor(url, spec, options=options)
