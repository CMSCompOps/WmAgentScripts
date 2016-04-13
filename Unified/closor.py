#!/usr/bin/env python
from assignSession import *
from utils import componentInfo, sendEmail, setDatasetStatus, unifiedConfiguration, workflowInfo, siteInfo, sendLog, reqmgr_url, monitor_dir
import reqMgrClient
import json
import time
import sys
import os
from utils import getDatasetEventsAndLumis, campaignInfo, getDatasetPresence, findLateFiles
from htmlor import htmlor
from collections import defaultdict
import reqMgrClient
import re
import copy


def spawn_harvesting(url, wfi , in_full):
    SI = siteInfo()
    
    all_OK = {}
    requests = []
    outputs = wfi.request['OutputDatasets'] 
    if ('EnableHarvesting' in wfi.request and wfi.request['EnableHarvesting']) or ('DQMConfigCacheID' in wfi.request and wfi.request['DQMConfigCacheID']):
        if not 'MergedLFNBase' in wfi.request:
            print "fucked up"
            sendEmail('screwed up wl cache','%s wl cache is bad'%(wfi.request['RequestName']))
            all_OK['fake'] = False
            return all_OK,requests

        wfi = workflowInfo(url, wfi.request['RequestName'])
        dqms = [out for out in outputs if '/DQM' in out]
        if not all([in_full[dqm_input] for dqm_input in dqms]):
            wfi.sendLog('closor',"will not be able to assign the harvesting: holding up")
            for dqm_input in dqms:
                all_OK[dqm_input] = False
                return all_OK,requests

        for dqm_input in dqms:
            ## handle it properly
            harvesting_schema = {
                'Requestor': os.getenv('USER'),
                'RequestType' : 'DQMHarvest',
                'Group' : 'DATAOPS'
                }
            copy_over = ['ProcessingString',
                         'DQMUploadUrl',
                         'CMSSWVersion',
                         'CouchDBName',
                         'CouchWorkloadDBName',
                         'CouchURL',
                         'DbsUrl',
                         'inputMode',
                         'DQMConfigCacheID',
                         'OpenRunningTimeout',
                         'ScramArch',
                         'CMSSWVersion',
                         'Campaign',
                         'Memory', #dummy
                         'SizePerEvent', #dummy
                         'GlobalTag', #dummy
                         ]
            for item in copy_over:
                harvesting_schema[item] = copy.deepcopy(wfi.request[item])
            harvesting_schema['InputDataset'] = dqm_input
            harvesting_schema['TimePerEvent'] = 1
            harvesting_schema['PrepID'] = 'Harvest-'+wfi.request['PrepID']
            harvesting_schema['RequestString'] = 'HARVEST-'+wfi.request['RequestString']
            harvesting_schema['DQMHarvestUnit'] = 'byRun'
            harvesting_schema['ConfigCacheUrl'] = harvesting_schema['CouchURL'] ## uhm, how stupid is that ?
            harvesting_schema['RequestPriority'] = wfi.request['RequestPriority']*10

            harvest_request = reqMgrClient.submitWorkflow(url, harvesting_schema)
            if not harvest_request:
                print "Error in making harvesting for",wfo.name
                print "schema"
                print json.dumps( harvesting_schema, indent = 2)
                harvest_request = reqMgrClient.submitWorkflow(url, harvesting_schema)
                if not harvest_request:
                    print "Error twice in harvesting for",wfo.name
                    print "schema"
                    print json.dumps( harvesting_schema, indent = 2)

            if harvest_request:
                requests.append( harvest_request )
                ## should we protect for setting approved ? no, it's notified below, assignment will fail, likely
                data = reqMgrClient.setWorkflowApproved(url, harvest_request)
                print "created",harvest_request,"for harvesting of",dqm_input
                wfi.sendLog('closor',"created %s for harvesting of %s"%( harvest_request, dqm_input))
                ## assign it directly
                team = wfi.request['Teams'][0]
                parameters={
                    'SiteWhitelist' : [SI.SE_to_CE(se) for se in wfi.request['NonCustodialSites']],
                    'AcquisitionEra' : wfi.acquisitionEra(),
                    'ProcessingString' : wfi.processingString(),
                    'MergedLFNBase' : wfi.request['MergedLFNBase'], 
                    'ProcessingVersion' : wfi.request['ProcessingVersion'],
                    'execute' : True
                    }
                if in_full[dqm_input]:
                    print "using full copy at",in_full[dqm_input]
                    parameters['SiteWhitelist'] = [SI.SE_to_CE(se) for se in in_full[dqm_input]]
                else:
                    print "cannot do anything if not having a full copy somewhere"
                    all_OK[dqm_input]=False
                    continue

                result = reqMgrClient.assignWorkflow(url, harvest_request, team, parameters)
                if not result:
                    sendEmail('harvesting request created','%s was created at announcement of %s in %s, failed to assign'%(harvest_request, dqm_input, wfi.request['RequestName']), destination=[wfi.request['Requestor']+'@cern.ch'])
                else:
                    sendEmail('harvesting request assigned','%s was created at announcement of %s in %s, and assigned'%(harvest_request, dqm_input, wfi.request['RequestName']), destination=[wfi.request['Requestor']+'@cern.ch']) 
                    wfi.sendLog('closor','%s was created at announcement of %s in %s, and assigned'%(harvest_request, dqm_input, wfi.request['RequestName']))

            else:
                print "could not make the harvesting for",wfo.name,"not announcing"
                wfi.sendLog('closor',"could not make the harvesting request")
                all_OK[dqm_input]=False                    
    return (all_OK, requests)

def closor(url, specific=None):
    if not componentInfo().check(): return

    UC = unifiedConfiguration()
    CI = campaignInfo()
    #LI = lockInfo()

    all_late_files = []
    check_fullcopy_to_announce = UC.get('check_fullcopy_to_announce')
    ## manually closed-out workflows should get to close with checkor
    if specific:
        wfs = session.query(Workflow).filter(Workflow.name.contains(specific)).all()
    else:
        wfs = session.query(Workflow).filter(Workflow.status=='close').all()

    held = set()

    for wfo in wfs:

        if specific and not specific in wfo.name: continue

        ## what is the expected #lumis 
        wfi = workflowInfo(url, wfo.name )
        wfo.wm_status = wfi.request['RequestStatus']

        if wfi.request['RequestStatus'] in  ['announced','normal-archived']:
            ## manually announced ??
            wfo.status = 'done'
            wfo.wm_status = wfi.request['RequestStatus']
            wfi.sendLog('closor','%s is announced already : %s'%( wfo.name,wfo.wm_status))
        session.commit()


        expected_lumis = 1
        if not 'TotalInputLumis' in wfi.request:
            print wfo.name,"has not been assigned yet, or the database is corrupted"
        else:
            expected_lumis = wfi.request['TotalInputLumis']

        ## what are the outputs
        outputs = wfi.request['OutputDatasets']
        ## check whether the number of lumis is as expected for each
        all_OK = defaultdict(lambda : False)
        #print outputs
        if len(outputs): 
            print wfo.name,wfi.request['RequestStatus']
        for out in outputs:
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=out)
            odb = session.query(Output).filter(Output.datasetname==out).first()
            if not odb:
                print "adding an output object",out
                odb = Output( datasetname = out )
                odb.workflow = wfo
                session.add( odb )
            odb.nlumis = lumi_count
            odb.nevents = event_count
            odb.workfow_id = wfo.id
            if odb.expectedlumis < expected_lumis:
                odb.expectedlumis = expected_lumis
            else:
                expected_lumis = odb.expectedlumis
            odb.date = time.mktime(time.gmtime())
            session.commit()

            wfi.sendLog('closor',"\t%60s %d/%d = %3.2f%%"%(out,lumi_count,expected_lumis,lumi_count/float(expected_lumis)*100.))
            #print wfo.fraction_for_closing, lumi_count, expected_lumis
            #fraction = wfo.fraction_for_closing
            #fraction = 0.0
            #all_OK.append((float(lumi_count) > float(expected_lumis*fraction)))
            all_OK[out] = True 


        ## check for at least one full copy prior to moving on
        in_full = {}
        for out in outputs:
            in_full[out] = []
            presence = getDatasetPresence( url, out )
            where = [site for site,info in presence.items() if info[0]]
            if where:
                all_OK[out] = True
                print out,"is in full at",",".join(where)
                in_full[out] = copy.deepcopy(where)
            else:

                going_to = wfi.request['NonCustodialSites']+wfi.request['CustodialSites']
                wfi.sendLog('closor',"%s is not in full anywhere. send to %s"%(out, ",".join(sorted(going_to))))
                at_destination = dict([(k,v) for (k,v) in presence.items() if k in going_to])
                else_where = dict([(k,v) for (k,v) in presence.items() if not k in going_to])
                print json.dumps( at_destination )
                print json.dumps( else_where, indent=2 )
                ## do the full stuck transfer study, missing files and shit !
                for there in going_to:
                    late_info = findLateFiles(url, out, going_to = there )
                    for l in late_info:
                        l.update({"workflow":wfo.name,"dataset":out})
                    all_late_files.extend( late_info )
                if check_fullcopy_to_announce:
                    ## only set this false if the check is relevant
                    all_OK[out] = False

    
        ## verify if we have to do harvesting

        (OK, requests) = spawn_harvesting(url, wfi, in_full)
        all_OK.update( OK )

        ## only that status can let me go into announced
        if all(all_OK.values()) and wfi.request['RequestStatus'] in ['closed-out']:
            print wfo.name,"to be announced"
            results=[]#'dummy']
            if not results:
                for out in outputs:
                    if all_OK[out]:
                        results.append(setDatasetStatus(out, 'VALID'))
                        tier = out.split('/')[-1]
                        campaign = None
                        try:
                            campaign = out.split('/')[2].split('-')[0]
                        except:
                            if 'Campaign' in wfi.request and wfi.request['Campaign']:
                                campaign = wfi.request['Campaign']
                        to_DDM = False
                        ## campaign override
                        if campaign and campaign in CI.campaigns and 'toDDM' in CI.campaigns[campaign] and tier in CI.campaigns[campaign]['toDDM']:
                            to_DDM = True
                        ## by typical enabling
                        if tier in UC.get("tiers_to_DDM"):
                            to_DDM = True
                        ## check for unitarity
                        if not tier in UC.get("tiers_no_DDM")+UC.get("tiers_to_DDM"):
                            print "tier",tier,"neither TO or NO DDM for",out
                            results.append('Not recognitized tier %s'%tier)
                            sendEmail("failed DDM injection","could not recognize %s for injecting in DDM"% out)
                            continue

                        n_copies = 2
                        destinations=[]
                        if to_DDM and campaign and campaign in CI.campaigns and 'DDMcopies' in CI.campaigns[campaign]:
                            ddm_instructions = CI.campaigns[campaign]['DDMcopies']
                            if type(ddm_instructions) == int:
                                n_copies = CI.campaigns[campaign]['DDMcopies']
                            elif type(ddm_instructions) == dict:
                                ## a more fancy configuration
                                for ddmtier,indication in ddm_instructions.items():
                                    if ddmtier==tier or ddmtier in ['*','all']:
                                        ## this is for us
                                        if 'N' in indication:
                                            n_copies = indication['N']
                                        if 'host' in indication:
                                            destinations = indication['host']
                                            
                        destination_spec = ""
                        if destinations:
                            destination_spec = "--destination="+",".join( destinations )
                        ## inject to DDM when necessary
                        if to_DDM:
                            #print "Sending",out," to DDM"
                            p = os.popen('python assignDatasetToSite.py --nCopies=%d --dataset=%s %s --exec'%(n_copies, out,destination_spec))
                            print p.read()
                            status = p.close()
                            if status!=None:
                                print "Failed DDM, retrying a second time"
                                p = os.popen('python assignDatasetToSite.py --nCopies=%d --dataset=%s %s --exec'%(n_copies, out,destination_spec))
                                print p.read()
                                status = p.close()    
                                if status!=None:
                                    sendEmail("failed DDM injection","could not add "+out+" to DDM pool. check closor logs.")
                            results.append( status )
                            if status == None:
                                wfi.sendLog('closor','%s is send to AnalysisOps DDM pool in %s copies %s'%( n_copies, out,destination_spec))
                                                            
                    else:
                        print wfo.name,"no stats for announcing",out
                        results.append('No Stats')

                if all(map(lambda result : result in ['None',None,True],results)):
                    ## only announce if all previous are fine
                    res = reqMgrClient.announceWorkflowCascade(url, wfo.name)
                    if not res in ['None',None]:
                        ## check the status again, it might well have toggled
                        wl_bis = workflowInfo(url, wfo.name)
                        wfo.wm_status = wl_bis.request['RequestStatus']
                        session.commit()
                        if wl_bis.request['RequestStatus'] in  ['announced','normal-archived']:
                            res = None
                        else:
                            ## retry ?
                            res = reqMgrClient.announceWorkflowCascade(url, wfo.name) 
                            
                    results.append( res )
                                
            #print results
            if all(map(lambda result : result in ['None',None,True],results)):
                wfo.status = 'done'
                session.commit()
                wfi.sendLog('closor',"workflow is announced")
            else:
                print "ERROR with ",wfo.name,"to be announced",json.dumps( results )
                
        else:
            print wfo.name,"not good for announcing:",wfi.request['RequestStatus']
            wfi.sendLog('closor',"cannot be announced")
            held.add( wfo.name )

    days_late = 0.
    retries_late = 10

    really_late_files = [info for info in all_late_files if info['retries']>=retries_late]
    really_late_files = [info for info in really_late_files if info['delay']/(60*60*24.)>=days_late]

    if really_late_files:
        subject = 'These %d files are lagging for %d days and %d retries announcing dataset \n%s'%(len(really_late_files), days_late, retries_late, json.dumps( really_late_files , indent=2) )
        sendEmail('waiting for files to announce', subject)
        sendLog('closor',subject)
        print subject
        open('%s/stuck_files.json'%monitor_dir,'w').write( json.dumps( really_late_files , indent=2))

    if held:
        sendEmail("held from announcing","the workflows below are held up, please check the logs https://cmst2.web.cern.ch/cmst2/unified/logs/closor/last.log \n%s"%("\n".join( held )))

        
if __name__ == "__main__":
    url = reqmgr_url
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
    closor(url,spec)

    if not spec:
        htmlor()
