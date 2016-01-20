#!/usr/bin/env python
from assignSession import *
from utils import getWorkLoad
from utils import componentInfo, sendEmail, setDatasetStatus, unifiedConfiguration
import reqMgrClient
import json
import time
import sys
import subprocess
from utils import getDatasetEventsAndLumis, campaignInfo, getDatasetPresence
#from utils import lockInfo
from htmlor import htmlor

def closor(url, specific=None):
    if not componentInfo().check(): return

    UC = unifiedConfiguration()
    CI = campaignInfo()
    #LI = lockInfo()

    ## manually closed-out workflows should get to close with checkor
    for wfo in session.query(Workflow).filter(Workflow.status=='close').all():

        if specific and not specific in wfo.name: continue

        ## what is the expected #lumis 
        wl = getWorkLoad(url, wfo.name)
        wfo.wm_status = wl['RequestStatus']

        if wl['RequestStatus'] in  ['announced','normal-archived']:
            ## manually announced ??
            wfo.status = 'done'
            wfo.wm_status = wl['RequestStatus']
            print wfo.name,"is announced already",wfo.wm_status

        session.commit()


        expected_lumis = 1
        if not 'TotalInputLumis' in wl:
            print wfo.name,"has not been assigned yet, or the database is corrupted"
        else:
            expected_lumis = wl['TotalInputLumis']

        ## what are the outputs
        outputs = wl['OutputDatasets']
        ## check whether the number of lumis is as expected for each
        all_OK = defaultdict(lambda : False)
        #print outputs
        if len(outputs): 
            print wfo.name,wl['RequestStatus']
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
                print json.dumps( presence, indent=2 )
                all_OK[out] = False

        ## only that status can let me go into announced
        if all(all_OK.values()) and wl['RequestStatus'] in ['closed-out']:
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
                            if 'Campaign' in wl and wl['Campaign']:
                                campaign = wl['Campaign']
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
                            status = subprocess.call(['python','assignDatasetToSite.py','--nCopies=%d'%n_copies,'--dataset='+out,'--exec', destination_spec])
                            if status!=0:
                                print "Failed DDM, retrying a second time"
                                status = subprocess.call(['python','assignDatasetToSite.py','--nCopies=%d'%n_copies,'--dataset='+out,'--exec', destination_spec])
                                if status!=0:
                                    results.append("Failed DDM for %s"% out)
                                    sendEmail("failed DDM injection","could not add "+out+" to DDM pool. check closor logs.")
                    else:
                        print wfo.name,"no stats for announcing",out
                        results.append('No Stats')

                if all(map(lambda result : result in ['None',None,True],results)):
                    ## only announce if all previous are fine
                    res = reqMgrClient.announceWorkflowCascade(url, wfo.name)
                    if not res in ['None',None]:
                        ## check the status again, it might well have toggled
                        wl_bis = getWorkLoad(url, wfo.name)
                        wfo.wm_status = wl_bis['RequestStatus']

                        if wl_bis['RequestStatus'] in  ['announced','normal-archived']:
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
            print wfo.name,"not good for announcing:",wl['RequestStatus']
                

if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    spec=None
    if len(sys.argv)>1:
        spec=sys.argv[1]
    closor(url,spec)

    htmlor()
