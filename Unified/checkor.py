#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, workflowInfo, getDatasetEventsAndLumis, findCustodialLocation, getDatasetEventsPerLumi, siteInfo, getDatasetPresence, campaignInfo, getWorkflowById, makeReplicaRequest
from utils import componentInfo
import phedexClient
import dbs3Client
import reqMgrClient
import json
from collections import defaultdict
import optparse
import os
import time
from McMClient import McMClient
from htmlor import htmlor
from utils import sendEmail 
from utils import closeoutInfo

def checkor(url, spec=None, options=None):
    fDB = closeoutInfo()

    use_mcm = True
    up = componentInfo(mcm=use_mcm, soft=['mcm'])
    if not up.check(): return
    use_mcm = up.status['mcm']

    wfs=[]
    if options.fetch:
        ## get all in running and check
        wfs.extend( session.query(Workflow).filter(Workflow.status == 'away').all() )
        wfs.extend( session.query(Workflow).filter(Workflow.status== 'assistance').all() )
    if options.nofetch:
        ## than get all in need for assistance
        wfs.extend( session.query(Workflow).filter(Workflow.status.startswith('assistance-')).all() )


    custodials = defaultdict(list) #sites : dataset list
    transfers = defaultdict(list) #sites : dataset list
    invalidations = [] #a list of files
    SI = siteInfo()
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

    by_passes = []
    for bypassor,email in [('jbadillo','julian.badillo.rojas@cern.ch'),('vlimant','vlimant@cern.ch'),('jen_a','jen_a@fnal.gov')]:
        bypass_file = '/afs/cern.ch/user/%s/%s/public/ops/bypass.json'%(bypassor[0],bypassor)
        if not os.path.isfile(bypass_file):
            print "no file",bypass_file
            continue
        try:
            by_passes.extend( json.loads(open(bypass_file).read()))
        except:
            print "cannot get by-passes from",bypass_file,"for",bypassor
            sendEmail("malformated by-pass information","%s is not json readable"%(bypass_file), destination=[email])

    for wfo in wfs:
        if spec and not (spec in wfo.name): continue

        print "checking on",wfo.name

        ## get info
        wfi = workflowInfo(url, wfo.name)

        ## make sure the wm status is up to date.
        # and send things back/forward if necessary.
        wfo.wm_status = wfi.request['RequestStatus']
        if wfo.wm_status == 'closed-out':
            ## manually closed-out
            print wfo.name,"is already",wfo.wm_status
            wfo.status = 'close'
            session.commit()
            continue
        elif wfo.wm_status in ['failed','aborted','aborted-archived','rejected','rejected-archived','aborted-completed']:
            ## went into trouble
            wfo.status = 'trouble'
            print wfo.name,"is in trouble",wfo.wm_status
            session.commit()
            continue
        elif wfo.wm_status in ['assigned','acquired']:
            ## not worth checking yet
            print wfo.name,"not running yet"
            session.commit()
            continue
        
        
        if wfo.wm_status != 'completed':
            ## for sure move on with closeout check if in completed
            print "no need to check on",wfo.name,"in status",wfo.wm_status
            session.commit()
            continue

        session.commit()        
        sub_assistance="" # if that string is filled, there will be need for manual assistance

        is_closing = True
        ## do the closed-out checks one by one

        ## get it from somewhere
        by_pass_checks = False
        if wfo.name in by_passes:
            print "we can bypass checks on",wfo.name
            by_pass_checks = True
        for bypass in by_passes:
            if bypass in wfo.name:
                print "we can bypass",wfo.name,"because of keyword",bypass
                by_pass_checks = True
                break

        # tuck out DQMIO/DQM
        wfi.request['OutputDatasets'] = [ out for out in wfi.request['OutputDatasets'] if not '/DQM' in out]

        ## anything running on acdc
        familly = getWorkflowById(url, wfi.request['PrepID'], details=True)
        acdc = []
        acdc_inactive = []
        has_recovery_going=False
        had_any_recovery = False
        for member in familly:
            if member['RequestType'] != 'Resubmission': continue
            if member['RequestName'] == wfo.name: continue
            if member['RequestDate'] < wfi.request['RequestDate']: continue
            if member['RequestStatus'] in ['running-open','running-closed','assignment-approved','assigned','acquired']:
                print wfo.name,"still has an ACDC running",member['RequestName']
                acdc.append( member['RequestName'] )
                #print json.dumps(member,indent=2)
                ## hook for just waiting ...
                is_closing = False
                has_recovery_going=True
            elif member['RequestStatus']==None:
                print member['RequestName'],"is not real"
                pass
            else:
                acdc_inactive.append( member['RequestName'] )
                had_any_recovery = True
        ## completion check
        percent_completions = {}
#        print "let's see who is crashing", wfo.name
#        print wfi.request['TotalInputEvents'],wfi.request['TotalInputLumis']
        if not 'TotalInputEvents' in wfi.request:
            event_expected,lumi_expected = 0,0
            if not 'recovery' in wfo.status:
                sendEmail("missing member of the request","TotalInputEvents is missing from the workload of %s"% wfo.name, destination=['julian.badillo.rojas@cern.ch'])
        else:
            event_expected,lumi_expected =  wfi.request['TotalInputEvents'],wfi.request['TotalInputLumis']

        fractions_pass = {}
        for output in wfi.request['OutputDatasets']:
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=output)
            percent_completions[output] = 0.
            if lumi_expected:
                percent_completions[output] = lumi_count / float( lumi_expected )

            fractions_pass[output] = 0.95
            c = get_campaign(output, wfi)
            if c in CI.campaigns and 'fractionpass' in CI.campaigns[c]:
                fractions_pass[output] = CI.campaigns[c]['fractionpass']
                print "overriding fraction to",fractions_pass[output],"for",output
            if options.fractionpass:
                fractions_pass[output] = options.fractionpass
                print "overriding fraction to",fractions_pass[output],"by command line for",output

        if not all([percent_completions[out] >= fractions_pass[out] for out in fractions_pass]):
            print wfo.name,"is not completed"
            print json.dumps(percent_completions, indent=2)
            print json.dumps(fractions_pass, indent=2)
            ## hook for creating automatically ACDC ?
            if has_recovery_going:
                sub_assistance+='-recovering'
            elif had_any_recovery:
                ## we want to have this looked at
                sub_assistance+='-manual'
            else:
                sub_assistance+='-recovery'
            is_closing = False

        ## correct lumi < 300 event per lumi
        events_per_lumi = {}
        for output in wfi.request['OutputDatasets']:
            events_per_lumi[output] = getDatasetEventsPerLumi( output )


        lumi_upper_limit = {}
        for output in wfi.request['OutputDatasets']:
            upper_limit = 301.
            campaign = get_campaign(output, wfi)
            if 'EventsPerLumi' in wfi.request and 'FilterEfficiency' in wfi.request:
                upper_limit = 1.5*wfi.request['EventsPerLumi']*wfi.request['FilterEfficiency']
                print "setting the upper limit of lumisize to",upper_limit,"by request configuration"

            if campaign in CI.campaigns and 'lumisize' in CI.campaigns[campaign]:
                upper_limit = CI.campaigns[campaign]['lumisize']
                print "overriding the upper lumi size to",upper_limit,"for",campaign

            if options.lumisize:
                upper_limit = options.lumisize
                print "overriding the upper lumi size to",upper_limit,"by command line"
                
            lumi_upper_limit[output] = upper_limit
        
        if any([ events_per_lumi[out] >= lumi_upper_limit[out] for out in events_per_lumi]):
            print wfo.name,"has big lumisections"
            print json.dumps(events_per_lumi, indent=2)
            ## hook for rejecting the request ?
            sub_assistance+='-biglumi'
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

        vetoed_custodial_tier = ['MINIAODSIM']
        out_worth_checking = [out for out in custodial_locations.keys() if out.split('/')[-1] not in vetoed_custodial_tier]
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
            ## try to get it from campaign configuration
            if not custodial:
                for output in out_worth_checking:
                    campaign = get_campaign(output, wfi)
                    if campaign in CI.campaigns and 'custodial' in CI.campaigns[campaign]:
                        custodial = CI.campaigns[campaign]['custodial']
                        print "Setting custodial to",custodial,"from campaign configuration"
                        break
            ## get from the parent
            pick_custodial = True
            if not custodial and 'InputDataset' in wfi.request:
                ## this is terribly dangerous to assume only 
                parents_custodial = phedexClient.getCustodialSubscriptionRequestSite( wfi.request['InputDataset'])
                ###parents_custodial = findCustodialLocation(url, wfi.request['InputDataset'])
                if not parents_custodial:
                    parents_custodial = []

                if len(parents_custodial):
                    custodial = parents_custodial[0]
                else:
                    print "the input dataset",wfi.request['InputDataset'],"does not have custodial in the first place. abort"
                    sendEmail( "dataset has no custodial location", "Please take a look at %s in the logs of checkor"%wfi.request['InputDataset'])
                    is_closing = False
                    pick_custodial = False

            if not custodial and pick_custodial:
                ## pick one at random
                custodial = SI.pick_SE()

            if custodial and ((not sub_assistance and not acdc) or by_pass_checks):
                ## register the custodial request, if there are no other big issues
                for output in out_worth_checking:
                    if not len(custodial_locations[output]):
                        custodials[custodial].append( output )
            else:
                print "cannot find a custodial for",wfo.name
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

        ## presence in phedex
        phedex_presence ={}
        for output in wfi.request['OutputDatasets']:
            phedex_presence[output] = phedexClient.getFileCountDataset(url, output )

        fraction_invalid = 0.01
        if not all([dbs_presence[out] == (dbs_invalid[out]+phedex_presence[out]) for out in wfi.request['OutputDatasets']]) and not options.ignorefiles:
            print wfo.name,"has a dbs,phedex mismatch"
            print json.dumps(dbs_presence, indent=2)
            print json.dumps(dbs_invalid, indent=2)
            print json.dumps(phedex_presence, indent=2)
            ## hook for just waiting ...
            is_closing = False

        if not all([(dbs_invalid[out] <= int(fraction_invalid*dbs_presence[out])) for out in wfi.request['OutputDatasets']]) and not options.ignorefiles:
            print wfo.name,"has a dbs invalid file level too high"
            print json.dumps(dbs_presence, indent=2)
            print json.dumps(dbs_invalid, indent=2)
            print json.dumps(phedex_presence, indent=2)
            ## need to be going and taking an eye
            sub_assistance+="-invalidfiles"
            is_closing = False

        ## put that heavy part at the end
        ## duplication check
        duplications = {}
        if is_closing:
            print "starting duplicate checker for",wfo.name
            for output in wfi.request['OutputDatasets']:
                print "\tchecking",output
                duplications[output] = True
                try:
                    duplications[output] = dbs3Client.duplicateRunLumi( output )
                except:
                    try:
                        duplications[output] = dbs3Client.duplicateRunLumi( output )
                    except:
                        print "was not possible to get the duplicate count for",output
                        is_closing=False

            if any(duplications.values()) and not options.ignoreduplicates:
                print wfo.name,"has duplicates"
                print json.dumps(duplications,indent=2)
                ## hook for making file invalidation ?
                sub_assistance+='-duplicates'
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

        if by_pass_checks:
            ## force closing
            is_closing = True

        ## and move on
        if is_closing:
            ## toggle status to closed-out in request manager
            print "setting",wfo.name,"closed-out"
            if not options.test:
                print reqMgrClient.closeOutWorkflowCascade(url, wfo.name)
                # set it from away/assistance* to close
                wfo.status = 'close'
                session.commit()
        else:
            ## that means there is something that needs to be done acdc, lumi invalidation, custodial, name it
            new_status = 'assistance'+sub_assistance
            print wfo.name,"needs assistance with",new_status
            
            if sub_assistance and wfo.status != new_status and 'PrepID' in wfi.request and not 'manual' in wfo.status:
                pid = wfi.getPrepIDs()[0].replace('task_','')
                #pid = wfi.request['PrepID'].replace('task_','')
                ## notify
                messages= {
                    'recovery' : 'Samples completed with missing lumi count:\n%s '%( '\n'.join(['%.2f %% complete for %s'%(percent_completions[output]*100, output) for output in wfi.request['OutputDatasets'] ] ) ),
                    'biglumi' : 'Samples completed with large luminosity blocks:\n%s '%('\n'.join(['%d > %d for %s'%(events_per_lumi[output], lumi_upper_limit[output], output) for output in wfi.request['OutputDatasets'] if (events_per_lumi[output] > lumi_upper_limit[output])])),
                    'duplicate' : 'Samples completed with duplicated luminosity blocks:\n%s'%( '\n'.join(['%s'%output for output in wfi.request['OutputDatasets'] if output in duplications and duplications[output] ] ) ),
                    }
                text ="The request %s (%s) is facing issue in production.\n" %( pid, wfo.name )
                content = ""
                for case in messages:
                    if case in new_status:
                        content+= "\n"+messages[case]+"\n"
                text += content
                text += "You are invited to check, while this is being taken care of by Ops.\n"
                text += "This is an automated message."
                if use_mcm and content:
                    print "Sending notification back to requestor"
                    print text
                    batches = mcm.getA('batches',query='contains=%s&status=announced'%pid)
                    if len(batches):
                        ## go notify the batch
                        bid = batches[-1]['prepid']
                        print "batch nofication to",bid
                        mcm.put('/restapi/batches/notify', { "notes" : text, "prepid" : bid})


                    ## go notify the request
                    print "request notification to",pid
                    mcm.put('/restapi/requests/notify',{ "message" : text, "prepids" : [pid] })

            ## case where the workflow was in manual from recoveror
            if not 'manual' in wfo.status or new_status!='assistance-recovery':
                wfo.status = new_status
                if not options.test:
                    print "setting",wfo.name,"to",wfo.status
                    session.commit()
            else:
                print "current status is",wfo.status,"not changing to anything"


    fDB.html()

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
    url='cmsweb.cern.ch'

    parser = optparse.OptionParser()
    parser.add_option('-t','--test', help='Only test the checkor', action='store_true', default=False)
    parser.add_option('-f','--fetch', help='fetch new stuff not already in assistance', action='store_true', default=False)
    parser.add_option('-n','--nofetch',help='update those in assistance',action='store_true', default=False)
    parser.add_option('--fractionpass',help='The completion fraction that is permitted', default=0.0,type='float')
    parser.add_option('--ignorefiles', help='Force ignoring dbs/phedex differences', action='store_true', default=False)
    parser.add_option('--lumisize', help='Force the upper limit on lumisection', default=0, type='float')
    parser.add_option('--ignoreduplicates', help='Force ignoring lumi duplicates', default=False, action='store_true')
    parser.add_option('--html',help='make the monitor page',action='store_true', default=False)
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    if options.fetch and options.nofetch:
        print "cannot fetch and not fetch at the same time"
        sys.exit(1)

    if not options.fetch and not options.nofetch:
        ## no argugments : default usage
        options.fetch = True
        options.nofetch = True
        
    checkor(url, spec, options=options)
    
    if options.html:
        htmlor()


