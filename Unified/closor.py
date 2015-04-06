from assignSession import *
from utils import getWorkLoad
from dbs.apis.dbsClient import DbsApi
import reqMgrClient
import setDatasetStatusDBS3
import json
import time
import sys
import subprocess

def closor(url, specific=None):
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    for wfo in session.query(Workflow).filter(Workflow.status=='away').all():
        if specific and not specific in wfo.name: continue
        ## what is the expected #lumis 
        wl = getWorkLoad(url, wfo.name)

        if wfo.wm_status != wl['RequestStatus']:
            wfo.wm_status = wl['RequestStatus']
            session.commit()

        take_out = ['failed','aborted','rejected','rejected-archived']
        if wl['RequestStatus'] in take_out:
            wfo.status = 'trouble'
            wfo.wm_status = wl['RequestStatus']
            session.commit()
            continue

        if wl['RequestStatus'] in  ['announced','normal-archived']:
            wfo.status = 'done'
            wfo.wm_status = wl['RequestStatus']
            session.commit()
            continue

        if wl['RequestType'] == 'Resubmission':
            #session.delete( wl)
            #session.commit()
            print wfo.name,"can be taken out"
            wfo.status = 'forget'
            wfo.wm_status = wl['RequestStatus']
            session.commit()
            continue

        if wl['RequestStatus'] in ['assigned','acquired']:
            print wfo.name,"not running yet"
            continue

        if not 'TotalInputLumis' in wl:
            print wfo.name,"has not been assigned yet"
            continue

        expected_lumis = wl['TotalInputLumis']

        ## what are the outputs
        outputs = wl['OutputDatasets']
        ## check whether the number of lumis is as expected for each
        all_OK = []
        #print outputs
        if len(outputs): 
            print wfo.name,wl['RequestStatus']
        for out in outputs:
            reply = dbsapi.listFileSummaries(dataset=out)
            lumi_count = 0
            event_count = 0
            for f in reply:
                lumi_count +=f['num_lumi']
                event_count +=f['num_event']

            odb = session.query(Output).filter(Output.datasetname==out).first()
            if not odb:
                print "adding an output object",out
                odb = Output( datasetname = out )
                odb.workflow = wfo
                session.add( odb )
            odb.nlumis = lumi_count
            odb.nevents = event_count
            if odb.expectedlumis < expected_lumis:
                odb.expectedlumis = expected_lumis
            else:
                expected_lumis = odb.expectedlumis
            odb.date = time.mktime(time.gmtime())
            session.commit()

            print "\t%60s %d/%d = %3.2f%%"%(out,lumi_count,expected_lumis,lumi_count/float(expected_lumis)*100.)
            #print wfo.fraction_for_closing, lumi_count, expected_lumis
            fraction = wfo.fraction_for_closing
            fraction = 0.05
            all_OK.append((float(lumi_count) > float(expected_lumis*fraction)))


        ## only that status can let me go into announced
        if wl['RequestStatus'] in ['closed-out']: ## add force-completed ??
            print wfo.name,"to be announced"
            results=[]#'dummy']
            if not results:
                results.append(reqMgrClient.announceWorkflowCascade(url, wfo.name))
                for (io,out) in enumerate(outputs):
                    if all_OK[io]:
                        results.append(setDatasetStatusDBS3.setStatusDBS3('https://cmsweb.cern.ch/dbs/prod/global/DBSWriter', out, 'VALID' ,''))
                        ## inject to DDM everything from ReDigi
                        if wl['RequestType'] == 'ReDigi' and not ('/DQM' in out):
                            print "Sending",out," to DDM"
                            subprocess.call(['python','assignDatasetToSite.py','--dataset='+out,'--exec'])
                    else:
                        print wfo.name,"no stats for announcing",out
                        results.append(None)
            
            #print results
            if all(map(lambda result : result in ['None',None],results)):
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
