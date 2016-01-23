#!/usr/bin/env python
from assignSession import *
from utils import componentInfo, sendEmail, setDatasetStatus, unifiedConfiguration, workflowInfo
import reqMgrClient
import json
import time
import sys
from utils import getDatasetEventsAndLumis, campaignInfo, getDatasetPresence, findLateFiles
from htmlor import htmlor
from collections import defaultdict

def closor(url, specific=None):
    if not componentInfo().check(): return

    UC = unifiedConfiguration()
    CI = campaignInfo()
    #LI = lockInfo()

    all_late_files = []
    ## manually closed-out workflows should get to close with checkor
    for wfo in session.query(Workflow).filter(Workflow.status=='close').all():

        if specific and not specific in wfo.name: continue

        ## what is the expected #lumis 
        wfi = workflowInfo(url, wfo.name )
        wfo.wm_status = wfi.request['RequestStatus']

        if wfi.request['RequestStatus'] in  ['announced','normal-archived']:
            ## manually announced ??
            wfo.status = 'done'
            wfo.wm_status = wfi.request['RequestStatus']
            print wfo.name,"is announced already",wfo.wm_status

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

            print "\t%60s %d/%d = %3.2f%%"%(out,lumi_count,expected_lumis,lumi_count/float(expected_lumis)*100.)
            #print wfo.fraction_for_closing, lumi_count, expected_lumis
            #fraction = wfo.fraction_for_closing
            #fraction = 0.0
            #all_OK.append((float(lumi_count) > float(expected_lumis*fraction)))
            all_OK[out] = True 


        ## check for at least one full copy prior to moving on
        for out in outputs:
            presence = getDatasetPresence( url, out )
            where = [site for site,info in presence.items() if info[0]]
            if where:
                all_OK[out] = True
                print out,"is in full at",",".join(where)
            else:
                print out,"is not in full anywhere"
                going_to = wfi.request['NonCustodialSites']+wfi.request['CustodialSites']
                print 'send to',','.join(going_to)
                at_destination = dict([(k,v) for (k,v) in presence.items() if k in going_to])
                else_where = dict([(k,v) for (k,v) in presence.items() if not k in going_to])
                print json.dumps( at_destination )
                print json.dumps( else_where, indent=2 )
                ## do the full stuck transfer study, missing files and shit !
                for there in going_to:
                    late_info = findLateFiles(url, out, going_to = there )
                    all_late_files.extend( late_info )
                all_OK[out] = False

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
                print wfo.name,"is announced"
            else:
                print "ERROR with ",wfo.name,"to be announced",json.dumps( results )
        else:
            print wfo.name,"not good for announcing:",wfi.request['RequestStatus']

    days_late = 0.
    retries_late = 10

    really_late_files = [info for info in all_late_files if info['retries']>=retries_late]
    really_late_files = [info for info in really_late_files if info['delay']/(60*60*24.)>=days_late]

    if really_late_files:
        sendEmail('waiting for files to announce','These files are lagging for %d days and %d retries announcing dataset \n%s'%(days_late, retries_late, json.dumps( really_late_files , indent=2)))
        open('/afs/cern.ch/user/c/cmst2/www/unified/stuck_files.json','w').write( json.dumps( really_late_files , indent=2))


        
if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
    closor(url,spec)

    htmlor()
