#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, workflowInfo, getDatasetEventsAndLumis, findCustodialLocation, getDatasetEventsPerLumi, siteInfo, getDatasetPresence, campaignInfo, getWorkflowById, makeReplicaRequest, global_SI, getDatasetSize, getDatasetFiles, sendLog, reqmgr_url, dbs_url, dbs_url_writer
from utils import componentInfo, unifiedConfiguration, userLock, duplicateLock
import phedexClient
import dbs3Client
dbs3Client.dbs3_url = dbs_url
dbs3Client.dbs3_url_writer = dbs_url_writer
import reqMgrClient
import json
from collections import defaultdict
import optparse
import os
import copy
import time
import random
from McMClient import McMClient
from htmlor import htmlor
from utils import sendEmail 
from utils import closeoutInfo

def checkor(url, spec=None, options=None):
    fDB = closeoutInfo()
    if userLock():   return
    if duplicateLock():  return

    UC = unifiedConfiguration()
    use_mcm = True
    up = componentInfo(mcm=use_mcm, soft=['mcm'])
    if not up.check(): return
    use_mcm = up.status['mcm']

    wfs=[]
    if options.new:
        ## get all in running and check

        ## you want to intersect with what is completed !
        if options.strict:
            completed_wfi = getWorkflows(url, status='completed')
            for wfo in session.query(Workflow).filter(Workflow.status == 'away').all():
                if wfo.name in completed_wfi:
                    wfs.append( wfo )
                else:
                    print wfo.name,"is not completed"
                    sendLog('checkor','%s is not completed'%( wfo.name))
        else:
            wfs.extend( session.query(Workflow).filter(Workflow.status == 'away').all() )

    if options.current:
        ## recheck those already there, probably to just pass them along
        wfs.extend( session.query(Workflow).filter(Workflow.status== 'assistance').all() )

    if options.old:
        ## than get all in need for assistance
        wfs.extend( session.query(Workflow).filter(Workflow.status.startswith('assistance-')).all() )


    custodials = defaultdict(list) #sites : dataset list
    transfers = defaultdict(list) #sites : dataset list
    invalidations = [] #a list of files
    SI = global_SI
    CI = campaignInfo()
    mcm = McMClient(dev=False)

    def get_campaign(output, wfi):
        campaign = None
        try:
            campaign = output.split('/')[2].split('-')[0]
        except:
            if 'Campaign' in wfi.request:
                campaign = wfi.request['Campaign']
        return campaign

    ## retrieve bypass and onhold configuration
    bypasses = []
    holdings = []
    try:
        already_notified = json.loads(open('already_notifified.json').read())
    except:
        print "no record of already notified workflow. starting fresh"
        already_notified = []

    for bypassor,email in [('vlimant','vlimant@cern.ch'),('jen_a','jen_a@fnal.gov')]:
        bypass_file = '/afs/cern.ch/user/%s/%s/public/ops/bypass.json'%(bypassor[0],bypassor)
        if not os.path.isfile(bypass_file):
            #sendLog('checkor','no file %s',bypass_file)
            continue
        try:
            bypasses.extend( json.loads(open(bypass_file).read()))
        except:
            sendLog('checkor',"cannot get by-passes from %s for %s"%(bypass_file ,bypassor))
            sendEmail("malformated by-pass information","%s is not json readable"%(bypass_file), destination=[email])
        
        holding_file = '/afs/cern.ch/user/%s/%s/public/ops/onhold.json'%(bypassor[0],bypassor)
        if not os.path.isfile(holding_file):
            #sendLog('checkor',"no file %s"%holding_file)
            continue
        try:
            holdings.extend( json.loads(open(holding_file).read()))
        except:
            sendLog('checkor',"cannot get holdings from %s for %s"%(holding_file, bypassor))
            sendEmail("malformated by-pass information","%s is not json readable"%(holding_file), destination=[email])

    ## once this was force-completed, you want to bypass
    for rider,email in [('vlimant','vlimant@cern.ch'),('jen_a','jen_a@fnal.gov'),('srimanob','srimanob@mail.cern.ch')]:
        rider_file = '/afs/cern.ch/user/%s/%s/public/ops/forcecomplete.json'%(rider[0],rider)
        if not os.path.isfile(rider_file):
            print "no file",rider_file
            #sendLog('checkor',"no file %s"%rider_file)
            continue
        try:
            bypasses.extend( json.loads(open( rider_file ).read() ) )
        except:
            sendLog('checkor',"cannot get force complete list from %s"%rider)
            sendEmail("malformated force complet file","%s is not json readable"%rider_file, destination=[email])

    pattern_fraction_pass = UC.get('pattern_fraction_pass')

    total_running_time = 5.*60. 
    sleep_time = 1
    if len(wfs):
        sleep_time = min(max(0.5, total_running_time / len(wfs)), 10)

    random.shuffle( wfs )

    print len(wfs),"to consider, pausing for",sleep_time

    for wfo in wfs:
        if spec and not (spec in wfo.name): continue
        time.sleep( sleep_time )
        
        ## get info
        wfi = workflowInfo(url, wfo.name)
        wfi.sendLog('checkor',"checking on %s %s"%( wfo.name,wfo.status))
        ## make sure the wm status is up to date.
        # and send things back/forward if necessary.
        wfo.wm_status = wfi.request['RequestStatus']
        if wfo.wm_status == 'closed-out':
            ## manually closed-out
            wfi.sendLog('checkor',"%s is already %s, setting close"%( wfo.name , wfo.wm_status))
            wfo.status = 'close'
            session.commit()
            continue

        elif wfo.wm_status in ['failed','aborted','aborted-archived','rejected','rejected-archived','aborted-completed']:
            ## went into trouble
            wfo.status = 'trouble'
            wfi.sendLog('checkor',"%s is in trouble %s"%(wfo.name, wfo.wm_status))
            session.commit()
            continue
        elif wfo.wm_status in ['assigned','acquired']:
            ## not worth checking yet
            wfi.sendLog('checkor',"%s is not running yet"%wfo.name)
            session.commit()
            continue
        
        if '-onhold' in wfo.status:
            if wfo.name in holdings and wfo.name not in bypasses:
                wfi.sendLog('checkor',"%s is on hold"%wfo.name)
                continue

        if wfo.wm_status != 'completed': #and not wfo.name in bypasses:
            ## for sure move on with closeout check if in completed
            wfi.sendLog('checkor',"no need to check on %s in status %s"%(wfo.name, wfo.wm_status))
            session.commit()
            continue

        if wfo.name in holdings and wfo.name not in bypasses:
            wfo.status = 'assistance-onhold'
            wfi.sendLog('checkor',"setting %s on hold"%wfo.name)
            session.commit()
            continue

        session.commit()        
        #sub_assistance="" # if that string is filled, there will be need for manual assistance
        existing_assistance_tags = set(wfo.status.split('-')[1:]) #[0] should be assistance
        assistance_tags = set()

        is_closing = True

        ## get it from somewhere
        bypass_checks = False
        for bypass in bypasses:
            if bypass in wfo.name:
                wfi.sendLog('checkor',"we can bypass checks on %s because of keyword%s "%( wfo.name, bypass))
                bypass_checks = True
                break
        
        if not CI.go( wfi.request['Campaign'] ) and not bypass_checks:
            print "No go for",wfo.name
            wfi.sendLog('checkor',"No go for %s"%wfi.request['Campaign'])
            continue


        tiers_with_no_check = copy.deepcopy(UC.get('tiers_with_no_check')) # dqm*
        vetoed_custodial_tier = copy.deepcopy(UC.get('tiers_with_no_custodial')) #dqm*, reco
        campaigns = {}
        for out in wfi.request['OutputDatasets']:
            c = get_campaign(out, wfi)
            campaigns[out] = c
            if c in CI.campaigns and 'custodial_override' in CI.campaigns[c]:
                vetoed_custodial_tier = list(set(vetoed_custodial_tier) - set(CI.campaigns[c]['custodial_override']))
                ## add those that we need to check for custodial copy
                tiers_with_no_check = list(set(tiers_with_no_check) - set(CI.campaigns[c]['custodial_override'])) ## would remove DQM from the vetoed check

        check_output_text = "Initial outputs:"+",".join(sorted(wfi.request['OutputDatasets'] ))
        wfi.request['OutputDatasets'] = [ out for out in wfi.request['OutputDatasets'] if not any([out.split('/')[-1] == veto_tier for veto_tier in tiers_with_no_check])]
        check_output_text += "\nWill check on:"+",".join(sorted(wfi.request['OutputDatasets'] ))
        check_output_text += "\ntiers out:"+",".join( sorted(tiers_with_no_check ))
        check_output_text += "\ntiers no custodial:"+",".join( sorted(vetoed_custodial_tier) )

        wfi.sendLog('checkor', check_output_text )

        ## anything running on acdc : getting the real prepid is not worth it
        familly = getWorkflowById(url, wfi.request['PrepID'], details=True)
        acdc = []
        acdc_inactive = []
        for member in familly:
            if member['RequestType'] != 'Resubmission': continue
            if member['RequestName'] == wfo.name: continue
            if member['RequestDate'] < wfi.request['RequestDate']: continue
            if member['RequestStatus'] in ['running-open','running-closed','assigned','acquired']:
                print wfo.name,"still has an ACDC running",member['RequestName']
                acdc.append( member['RequestName'] )
                ## cannot be bypassed!
                is_closing = False
                assistance_tags.add('recovering')
            elif member['RequestStatus']==None:
                print member['RequestName'],"is not real"
                pass
            else:
                acdc_inactive.append( member['RequestName'] )
                assistance_tags.add('recovered')

        ## completion check
        percent_completions = {}
        if not 'TotalInputEvents' in wfi.request:
            event_expected,lumi_expected = 0,0
            if not 'recovery' in wfo.status:
                sendEmail("missing member of the request","TotalInputEvents is missing from the workload of %s"% wfo.name, destination=['jen_a@fnal.gov'])
        else:
            event_expected,lumi_expected =  wfi.request['TotalInputEvents'],wfi.request['TotalInputLumis']

        if 'RequestNumEvents' in wfi.request and int(wfi.request['RequestNumEvents']):
            event_expected = int(wfi.request['RequestNumEvents'])
        elif 'Task1' in wfi.request and 'RequestNumEvents' in wfi.request['Task1']:
            event_expected = int(wfi.request['Task1']['RequestNumEvents'])

        fractions_pass = {}
        over_100_pass = False
        (lhe,prim,_,_) = wfi.getIO()
        if lhe or prim: over_100_pass = False

        for output in wfi.request['OutputDatasets']:
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=output)
            percent_completions[output] = 0.

            if lumi_expected:
                percent_completions[output] = lumi_count / float( lumi_expected )
            if event_expected:
                wfi.sendLog('checkor', "event completion real %s expected %s"%(event_count, event_expected ))
                percent_completions[output] = max(percent_completions[output], float(event_count) / float( event_expected ) )

            fractions_pass[output] = 0.95
            c = campaigns[output]
            if c in CI.campaigns and 'fractionpass' in CI.campaigns[c]:
                fractions_pass[output] = CI.campaigns[c]['fractionpass']
                wfi.sendLog('checkor', "overriding fraction to %s for %s by campaign requirement"%( fractions_pass[output], output))

            if options.fractionpass:
                fractions_pass[output] = options.fractionpass
                print "overriding fraction to",fractions_pass[output],"by command line for",output

            for key in pattern_fraction_pass:
                if key in output:
                    fractions_pass[output] = pattern_fraction_pass[key]
                    print "overriding fraction to",fractions_pass[output],"by dataset key",key
                    

        if not all([percent_completions[out] >= fractions_pass[out] for out in fractions_pass]):
            print wfo.name,"is not completed"
            print json.dumps(percent_completions, indent=2)
            print json.dumps(fractions_pass, indent=2)
            ## hook for creating automatically ACDC ?
            if not bypass_checks:
                assistance_tags.add('recovery')
                is_closing = False

        if over_100_pass and any([percent_completions[out] >100 for out in fractions_pass]):
            print wfo.name,"is over completed"
            print json.dumps(percent_completions, indent=2)
            if not bypass_checks:
                assistance_tags.add('over100')
                is_closing = False

        ## correct lumi < 300 event per lumi
        events_per_lumi = {}
        for output in wfi.request['OutputDatasets']:
            events_per_lumi[output] = getDatasetEventsPerLumi( output )


        lumi_upper_limit = {}
        for output in wfi.request['OutputDatasets']:
            upper_limit = 301.
            campaign = campaigns[output]
            #if 'EventsPerLumi' in wfi.request and 'FilterEfficiency' in wfi.request:
            #    upper_limit = 1.5*wfi.request['EventsPerLumi']*wfi.request['FilterEfficiency']
            #    print "setting the upper limit of lumisize to",upper_limit,"by request configuration"

            if campaign in CI.campaigns and 'lumisize' in CI.campaigns[campaign]:
                upper_limit = CI.campaigns[campaign]['lumisize']
                print "overriding the upper lumi size to",upper_limit,"for",campaign

            if options.lumisize:
                upper_limit = options.lumisize
                print "overriding the upper lumi size to",upper_limit,"by command line"
                
            lumi_upper_limit[output] = upper_limit
            if wfi.request['RequestType'] in ['ReDigi']: lumi_upper_limit[output] = -1
        
        if any([ (lumi_upper_limit[out]>0 and events_per_lumi[out] >= lumi_upper_limit[out]) for out in events_per_lumi]):
            print wfo.name,"has big lumisections"
            print json.dumps(events_per_lumi, indent=2)
            ## hook for rejecting the request ?
            if not bypass_checks:
                assistance_tags.add('biglumi')
                is_closing = False 


        any_presence = {}
        for output in wfi.request['OutputDatasets']:
            any_presence[output] = getDatasetPresence(url, output, vetoes=[])

        ## custodial copy
        custodial_locations = {}
        custodial_presences = {}
        for output in wfi.request['OutputDatasets']:
            custodial_presences[output] = [s for s in any_presence[output] if 'MSS' in s]
            custodial_locations[output] = phedexClient.getCustodialSubscriptionRequestSite(output)

            if not custodial_locations[output]:
                custodial_locations[output] = []

        ## presence in phedex
        phedex_presence ={}
        for output in wfi.request['OutputDatasets']:
            phedex_presence[output] = phedexClient.getFileCountDataset(url, output )


            
        out_worth_checking = [out for out in custodial_locations.keys() if out.split('/')[-1] not in vetoed_custodial_tier]
        size_worth_checking = sum([getDatasetSize(out)/1023. for out in out_worth_checking ]) ## size in TBs of all outputs
        if not all(map( lambda sites : len(sites)!=0, [custodial_locations[out] for out in out_worth_checking])):
            print wfo.name,"has not all custodial location"
            print json.dumps(custodial_locations, indent=2)

            ##########
            ## hook for making a custodial replica ?
            custodial = None
            ## get from other outputs
            for output in out_worth_checking:
                if len(custodial_locations[output]): 
                    custodial = custodial_locations[output][0]
            if custodial and float(SI.storage[custodial]) < size_worth_checking:
                print "cannot use the other output custodial:",custodial,"because of limited space"
                custodial = None

            ## try to get it from campaign configuration
            if not custodial:
                for output in out_worth_checking:
                    campaign = campaigns[output]
                    if campaign in CI.campaigns and 'custodial' in CI.campaigns[campaign]:
                        custodial = CI.campaigns[campaign]['custodial']
                        print "Setting custodial to",custodial,"from campaign configuration"

            if custodial and float(SI.storage[custodial]) < size_worth_checking:
                print "cannot use the campaign configuration custodial:",custodial,"because of limited space"
                custodial = None

            ## get from the parent
            pick_custodial = True
            use_parent_custodial = UC.get('use_parent_custodial')
            _,prim,_,_ = wfi.getIO()
            if not custodial and prim and use_parent_custodial:
                parent_dataset = prim.pop()
                ## this is terribly dangerous to assume only 
                parents_custodial = phedexClient.getCustodialSubscriptionRequestSite( parent_dataset )
                ###parents_custodial = findCustodialLocation(url, parent_dataset)
                if not parents_custodial:
                    parents_custodial = []

                if len(parents_custodial):
                    custodial = parents_custodial[0]
                else:
                    print "the input dataset",parent_dataset,"does not have custodial in the first place. abort"
                    sendEmail( "dataset has no custodial location", "Please take a look at %s in the logs of checkor"%parent_dataset)
                    ## cannot be bypassed, this is an issue to fix
                    is_closing = False
                    pick_custodial = False
                    assistance_tags.add('parentcustodial')
                                
            if custodial and float(SI.storage[custodial]) < size_worth_checking:
                print "cannot use the parent custodial:",custodial,"because of limited space"
                custodial = None

            if not custodial and pick_custodial:
                ## pick one at random
                custodial = SI.pick_SE(size=size_worth_checking)

            if not custodial:
                print "cannot find a custodial for",wfo.name
                sendEmail( "cannot find a custodial","cannot find a custodial for %s probably because of the total output size %d"%( wfo.name, size_worth_checking))

                
            if custodial and (is_closing or bypass_checks):
                print "picked",custodial,"for tape copy"
                ## remember how much you added this round already ; this stays locally
                SI.storage[custodial] -= size_worth_checking
                ## register the custodial request, if there are no other big issues
                for output in out_worth_checking:
                    if not len(custodial_locations[output]):
                        if phedex_presence[output]>=1:
                            custodials[custodial].append( output )
                            ## let's wait and see if that's needed 
                            assistance_tags.add('custodial')
                        else:
                            print "no file in phedex for",output," not good to add to custodial requests"
            #cannot be bypassed


            is_closing = False

        ## disk copy 
        disk_copies = {}
        for output in wfi.request['OutputDatasets']:
            disk_copies[output] = [s for s in any_presence[output] if (not 'MSS' in s) and (not 'Buffer' in s)]

        if not all(map( lambda sites : len(sites)!=0, disk_copies.values())):
            print wfo.name,"has not all output on disk"
            print json.dumps(disk_copies, indent=2)


        ## presence in dbs
        dbs_presence = {}
        dbs_invalid = {}
        for output in wfi.request['OutputDatasets']:
            dbs_presence[output] = dbs3Client.getFileCountDataset( output )
            dbs_invalid[output] = dbs3Client.getFileCountDataset( output, onlyInvalid=True)

        fraction_invalid = 0.01
        if not all([dbs_presence[out] == (dbs_invalid[out]+phedex_presence[out]) for out in wfi.request['OutputDatasets']]) and not options.ignorefiles:
            print wfo.name,"has a dbs,phedex mismatch"
            print json.dumps(dbs_presence, indent=2)
            print json.dumps(dbs_invalid, indent=2)
            print json.dumps(phedex_presence, indent=2)
            if not 'recovering' in assistance_tags:
                assistance_tags.add('filemismatch')
                #print this for show and tell if no recovery on-going
                for out in dbs_presence:
                    _,_,missing_phedex,missing_dbs  = getDatasetFiles(url, out)
                    if missing_phedex:
                        print "These %d files are missing in phedex"%(len(missing_phedex))
                        print "\n".join( missing_phedex )
                    if missing_dbs:
                        print "These %d files are missing in dbs"%(len(missing_dbs))
                        print "\n".join( missing_dbs )

            #if not bypass_checks:
            ## I don't think we can by pass this
            is_closing = False

        if not all([(dbs_invalid[out] <= int(fraction_invalid*dbs_presence[out])) for out in wfi.request['OutputDatasets']]) and not options.ignorefiles:
            print wfo.name,"has a dbs invalid file level too high"
            print json.dumps(dbs_presence, indent=2)
            print json.dumps(dbs_invalid, indent=2)
            print json.dumps(phedex_presence, indent=2)
            ## need to be going and taking an eye
            assistance_tags.add('invalidfiles')
            if not bypass_checks:
                #sub_assistance+="-invalidfiles"
                is_closing = False

        ## put that heavy part at the end
        ## duplication check
        duplications = {}
        if is_closing or bypass_checks:
            print "starting duplicate checker for",wfo.name
            for output in wfi.request['OutputDatasets']:
                print "\tchecking",output
                duplications[output] = True
                try:
                    duplications[output] = dbs3Client.duplicateRunLumi( output , skipInvalid=True, verbose=True)
                except:
                    try:
                        duplications[output] = dbs3Client.duplicateRunLumi( output , skipInvalid=True, verbose=True)
                    except:
                        print "was not possible to get the duplicate count for",output
                        is_closing=False

            if any(duplications.values()) and not options.ignoreduplicates:
                print wfo.name,"has duplicates"
                print json.dumps(duplications,indent=2)
                ## hook for making file invalidation ?
                ## it shouldn't be allowed to bypass it
                assistance_tags.add('duplicates')
                is_closing = False 



        ## for visualization later on
        if not wfo.name in fDB.record: 
            #print "adding",wfo.name,"to close out record"
            fDB.record[wfo.name] = {
            'datasets' :{},
            'name' : wfo.name,
            'closeOutWorkflow' : None,
            }
        fDB.record[wfo.name]['closeOutWorkflow'] = is_closing
        fDB.record[wfo.name]['priority'] = wfi.request['RequestPriority']
        fDB.record[wfo.name]['prepid'] = wfi.request['PrepID']

        for output in wfi.request['OutputDatasets']:
            if not output in fDB.record[wfo.name]['datasets']: fDB.record[wfo.name]['datasets'][output] = {}
            rec = fDB.record[wfo.name]['datasets'][output]
            rec['percentage'] = float('%.2f'%(percent_completions[output]*100))
            rec['duplicate'] = duplications[output] if output in duplications else 'N/A'
            rec['phedexReqs'] = float('%.2f'%any_presence[output][custodial_presences[output][0]][1]) if len(custodial_presences[output])!=0 else 'N/A'
            rec['closeOutDataset'] = is_closing
            rec['transPerc'] = float('%.2f'%any_presence[output][ disk_copies[output][0]][1]) if len(disk_copies[output])!=0 else 'N/A'
            rec['correctLumis'] = int(events_per_lumi[output]) if (events_per_lumi[output] > lumi_upper_limit[output]) else True
            rec['missingSubs'] = False if len(custodial_locations[output])==0 else ','.join(list(set(custodial_locations[output])))
            rec['dbsFiles'] = dbs_presence[output]
            rec['dbsInvFiles'] = dbs_invalid[output]
            rec['phedexFiles'] = phedex_presence[output]
            rec['acdc'] = "%d / %d"%(len(acdc),len(acdc+acdc_inactive))

        ## and move on
        if is_closing:
            ## toggle status to closed-out in request manager
            print "setting",wfo.name,"closed-out"
            if not options.test:
                if wfo.wm_status in ['closed-out','announced','normal-archived']:
                    print wfo.name,"is already",wfo.wm_status,"not trying to closed-out and assuming it does"
                    res = None
                else:
                    res = reqMgrClient.closeOutWorkflowCascade(url, wfo.name)
                    print "close out answer",res

                if not res in ["None",None]:
                    print "try to get the current status again"
                    wfi_bis = workflowInfo(url, wfo.name)
                    if wfi_bis.request['RequestStatus'] == 'closed-out':
                        print "the request did toggle to closed-out"
                        res = None
                    
                if not res in ["None",None]:
                    print "retrying to closing out"
                    print res
                    res = reqMgrClient.closeOutWorkflowCascade(url, wfo.name)
                    
                
                if res in [None,"None"]:
                    wfo.status = 'close'
                    session.commit()
                else:
                    print "could not close out",wfo.name,"will try again next time"
        else:
            ## full known list
            #recovering # has active ACDC
            ##OUT #recovered #had inactive ACDC
            #recovery #not over the pass bar
            #over100 # over 100%
            #biglumi # has a big lumiblock
            #parentcustodial # the parent does not have a valid subscription yet
            #custodial # has had the transfer made, is waiting for a valid custodial subscription to appear
            #filemismatch # there is a dbs/phedex mismatch
            #duplicates #a lumi section is there twice

            ## manual is not added yet, and should be so by recoveror
            print wfo.name,"was tagged with :",list(assistance_tags)
            if 'recovering' in assistance_tags:
                ## if active ACDC, being under threshold, filemismatch do not matter
                assistance_tags = assistance_tags - set(['recovery','filemismatch'])
            if 'recovery' in assistance_tags and 'recovered' in assistance_tags:
                ## should not set -recovery to anything that add ACDC already
                assistance_tags = assistance_tags - set(['recovery','recovered']) 
                ## straight to manual
                assistance_tags.add('manual')


            ## that means there is something that needs to be done acdc, lumi invalidation, custodial, name it
            print wfo.name,"needs assistance with",",".join( assistance_tags )
            print wfo.name,"existing conditions",",".join( existing_assistance_tags )
            
            #########################################
            ##### notification to requester #########
            go_notify=False
            if assistance_tags and not 'manual' in existing_assistance_tags and existing_assistance_tags != assistance_tags:
                go_notify=True
            
            if go_notify and wfo.name in already_notified:
                print "double notification"
                sendEmail('double notification','please take a look at %s'%(wfo.name))
            elif go_notify:
                    
                already_notified.append( wfo.name )
                pids = wfi.getPrepIDs() ## could be multiple requests

                detailslink = 'https://cmsweb.cern.ch/reqmgr/view/details/%s'
                perflink = 'https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s'%(wfo.name)
                splitlink = 'https://cmsweb.cern.ch/reqmgr/view/splitting/%s'%(wfo.name)
                ## notify templates
                messages= {
                    'recovery': 'Samples completed with missing statistics:\n%s\n%s '%( '\n'.join(['%.2f %% complete for %s'%(percent_completions[output]*100, output) for output in wfi.request['OutputDatasets'] ] ), perflink ),
                    'biglumi': 'Samples completed with large luminosity blocks:\n%s\n%s '%('\n'.join(['%d > %d for %s'%(events_per_lumi[output], lumi_upper_limit[output], output) for output in wfi.request['OutputDatasets'] if (events_per_lumi[output] > lumi_upper_limit[output])]), splitlink),
                    'duplicates': 'Samples completed with duplicated luminosity blocks:\n%s\n'%( '\n'.join(['%s'%output for output in wfi.request['OutputDatasets'] if output in duplications and duplications[output] ] ) ),
                    #'filemismatch': 'Samples completed with inconsistency in DBS/Phedex',
                    #'manual' :                     'Workflow completed and requires manual checks by Ops',
                    }
                
                content = "The request PREPID (WORKFLOW) is facing issue in production.\n"
                motive = False
                for case in messages:
                    if case in assistance_tags:
                        content+= "\n"+messages[case]+"\n"
                        motive = True
                content += "You are invited to check, while this is being taken care of by Comp-Ops.\n"
                content += "This is an automated message from Comp-Ops.\n"

                items_notified = set()
                if use_mcm and motive:
                    wfi.notifyRequestor( content , mcm = mcm)

            #########################################


            ## logic to set the status further
            if assistance_tags:
                new_status = 'assistance-'+'-'.join(sorted(assistance_tags) )
            else:
                new_status = 'assistance'

            ## case where the workflow was in manual from recoveror
            if not 'manual' in wfo.status or new_status!='assistance-recovery':
                wfo.status = new_status
                if not options.test:
                    print "setting",wfo.name,"to",wfo.status
                    session.commit()
            else:
                print "current status is",wfo.status,"not changing to anything"

    open('already_notifified.json','w').write( json.dumps( already_notified , indent=2))

    fDB.html()
    if not spec:
        #sendEmail("fresh assistance status available","Fresh status are available at https://cmst2.web.cern.ch/cmst2/unified/assistance.html",destination=['jen_a@fnal.gov'])
        #it's a bit annoying
        pass

    ## custodial requests
    print "Custodials"
    print json.dumps(custodials, indent=2)
    for site in custodials:
        print ','.join(custodials[site]),'=>',site
        if not options.test:
            result = makeReplicaRequest(url, site, list(set(custodials[site])),"custodial copy at production close-out",custodial='y',priority='low', approve = (site in SI.sites_auto_approve) )
            print result

    print "Transfers"
    print json.dumps(transfers, indent=2)
    ## replicas requests
    for site in transfers:
        print ','.join(transfers[site]),'=>',site
        if not options.test:
            result = None
            #result = makeReplicaRequest(url, site, list(set(transfers[site])),"copy to disk at production close-out")
            print result

    print "File Invalidation"
    print invalidations

if __name__ == "__main__":
    url = reqmgr_url

    parser = optparse.OptionParser()
    parser.add_option('-t','--test', help='Only test the checkor', action='store_true', default=False)
    parser.add_option('-n','--new', help='fetch from running workflows', action='store_true', default=False)
    parser.add_option('-s','--strict', help='only checkor matter', action='store_true', default=False)
    parser.add_option('-c','--current', help='update those in assistance', action='store_true', default=False)    
    parser.add_option('-o','--old',help='update those in complex assistance',action='store_true', default=False)
    parser.add_option('--fractionpass',help='The completion fraction that is permitted', default=0.0,type='float')
    parser.add_option('--ignorefiles', help='Force ignoring dbs/phedex differences', action='store_true', default=False)
    parser.add_option('--lumisize', help='Force the upper limit on lumisection', default=0, type='float')
    parser.add_option('--ignoreduplicates', help='Force ignoring lumi duplicates', default=False, action='store_true')
    parser.add_option('--html',help='make the monitor page',action='store_true', default=False)
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    options.new = True
    options.current = True
    options.old = True

    checkor(url, spec, options=options)
    
    #if options.html:
    htmlor()


