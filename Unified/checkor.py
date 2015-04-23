#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, workflowInfo, getDatasetEventsAndLumis, findCustodialLocation, getDatasetEventsPerLumi, siteInfo
import phedexClient
import dbs3Client
import json
from collections import defaultdict
import optparse
from htmlor import htmlor

class falseDB:
    def __init__(self):
        self.record = json.loads(open('closedout.json').read())

    def __del__(self):
        open('closedout.json','w').write( json.dumps( self.record , indent=2 ) )
        html = open('/afs/cern.ch/user/c/cmst2/www/unified/closeout.html','w')
        html.write('<html><table border=1>')
        html.write('<tr><th>workflow</th><th>OutputDataSet</th><th>%Compl</th><th>Dupl</th><th>CorrectLumis</th><th>Scubscr</th><th>Tran</th><th>dbsF</th><th>phdF</th><th>ClosOut</th></tr>')

        for wf in sorted(self.record.keys()):
            order = ['percentage','duplicate','correctLumis','missingSubs','phedexReqs','dbsFiles','phedexFiles']
            wf_and_anchor = '<a id="%s">%s</a>'%(wf,wf)
            for out in self.record[wf]['datasets']:
                html.write('<tr>')
                html.write('<td>%s</td>'% wf_and_anchor)
                html.write('<td>%s</td>'% out)
                for f in order:
                    value = self.record[wf]['datasets'][out][f]
                    html.write('<td>%s</td>'% value)
                html.write('<td>%s</td>'%self.record[wf]['closeOutWorkflow'])
                html.write('</tr>')
                wf_and_anchor = wf

def checkor(url, spec=None, options=None):
    fDB = falseDB()

    wfs=[]
    if options.fetch:
        workflows = getWorkflows(url, status='completed')
        for wf in workflows:
            wfo = session.query(Workflow).filter(Workflow.name == wf ).first()
            if wfo:
                wfs.append(wfo )
    else:
        ## than get all in need for assistance
        wfs = session.query(Workflow).filter(Workflow.status.startswith('assistance')).all()


    custodials = defaultdict(list) #sites : dataset list
    transfers = defaultdict(list) #sites : dataset list
    invalidations = [] #a list of files
    SI = siteInfo()

    for wfo in wfs:
        if spec and not (spec in wfo.name): continue

        if wfo.status != 'away': continue

        ## get info
        wfi = workflowInfo(url, wfo.name)

        is_closing = True
        ## do the closed-out checks one by one

        # tuck out DQMIO/DQM
        wfi.request['OutputDatasets'] = [ out for out in wfi.request['OutputDatasets'] if not '/DQM' in out]

        ## completion check
        percent_completions = {}
        for output in wfi.request['OutputDatasets']:
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=output)
            event_expected,lumi_expected =  wfi.request['TotalInputEvents'],wfi.request['TotalInputLumis']
            percent_completions[output] = 0
            if lumi_expected:
                percent_completions[output] = lumi_count / float( lumi_expected )
        if not all([fract > 0.95 for fract in percent_completions.values()]):
            print wfo.name,"is not completed"
            print json.dumps(percent_completions, indent=2)
            ## hook for creating automatically ACDC ?
            is_closing = False

        ## duplication check
        duplications = {}
        for output in wfi.request['OutputDatasets']:
            duplications[output] = dbs3Client.duplicateRunLumi( output )
            
        if any(duplications.values()):
            print wfo.name,"has duplicates",
            print json.dumps(duplications,indent=2)
            ## hook for making file invalidation ?
            is_closing = False 

        ## correct lumi ??< 300 event per lumi
        events_per_lumi = {}
        for output in wfi.request['OutputDatasets']:
            events_per_lumi[output] = getDatasetEventsPerLumi( output )

        if any([ epl> 300. for epl in events_per_lumi.values()]):
            print wfo.name,"has big lumisections"
            print json.dumps(events_per_lumi, indent=2)
            ## hook for rejecting the request ?
            is_closing = False 


        ## custodial copy
        custodial_locations = {}
        for output in wfi.request['OutputDatasets']:
            custodial_locations[output] = findCustodialLocation(url, output)

        if not all(map( lambda sites : len(sites)!=0, custodial_locations.values())):
            print wfo.name,"has not all custodial location"
            print json.dumps(custodial_locations, indent=2)

            ##########
            ## hook for making a custodial replica ?
            custodial = None
            ## get from other outputs
            for (output,sites) in custodial_locations.items():
                if len(sites): custodial = sites[0]
            ## get from the parent
            if not custodial and 'InputDataset' in wfi.request:
                parents_custodial = findCustodialLocation(url, wfi.request['InputDataset'])
                if len(parents_custodial):
                    custodial = parents_custodial[0]
                else:
                    print "the input dataset",wfi.request['InputDataset'],"does not have custodial in the first place. abort"
            else:
                ## pick one at random
                custodial = SI.pick_SE()

            if custodial:
                ## register the custodial request
                for (output,sites) in custodial_locations.items():
                    if not len(sites):
                        custodials[custodial].append( output )
            is_closing = False

        ## disk copy 
        

        ## presence in dbs
        dbs_presence = {}
        for output in wfi.request['OutputDatasets']:
            dbs_presence[output] = dbs3Client.getFileCountDataset( output )
        
        ## presence in phedex
        phedex_presence ={}
        for output in wfi.request['OutputDatasets']:
            phedex_presence[output] = phedexClient.getFileCountDataset(url, output )

        if not all([dbs_presence[out] == phedex_presence[out] for out in wfi.request['OutputDatasets']]):
            print wfo.name,"has a dbs,phedex mismatch"
            print json.dumps(dbs_presence, indent=2)
            print json.dumps(phedex_presence, indent=2)
            ## hook for just waiting ...
            is_closing = False

        ## for visualization later on
        if not wfo.name in fDB.record: fDB.record[wfo.name] = {
            'datasets' :{},
            'name' : wfo.name,
            'closeOutWorkflow' : None,
            }
        fDB.record[wfo.name]['closeOutWorkflow'] = is_closing
        for output in wfi.request['OutputDatasets']:
            if not output in fDB.record[wfo.name]['datasets']: fDB.record[wfo.name]['datasets'][output] = {}
            rec = fDB.record[wfo.name]['datasets'][output]
            rec['percentage'] = float('%.2f'%percent_completions[output])
            rec['duplicate'] = duplications[output]
            rec['phedexReqs'] = None
            rec['closeOutDataset'] = None
            rec['transPerc'] = None
            rec['correctLumis'] = (events_per_lumi[output] <= 300)
            rec['missingSubs'] = False if len(custodial_locations[output])==0 else ','.join(custodial_locations[output])
            rec['dbsFiles'] = dbs_presence[output]
            rec['phedexFiles'] = phedex_presence[output]
            

        ## and move on
        if is_closing:
            ## toggle status to closed-out in request manager
            print "setting",wfo.name,"closed-out"
            #reqMgrClient.closeOutWorkflowCascade(url, wfo.name)
            wfo.status = 'away'
            #session.commit()
        else:
            print wfo.name,"needs assistance"
            ## that means there is something that needs to be done acdc, lumi invalidation, custodial, name it
            wfo.status = 'assistance'
            #session.commit()


    ## custodial requests
    print "Custodials"
    print json.dumps(custodials, indent=2)
    for site in custodials:
        print ','.join(custodials[site]),'=>',site
        result = None
        #result = makeReplicaRequest(url, site, list(set(custodials[site])),"custodial copy at close-out",custodial='y')
        print result

    print "Transfers"
    print json.dumps(transfers, indent=2)
    ## replicas requests
    for site in transfers:
        print ','.join(transfers[site]),'=>',site
        result = None
        #result = makeReplicaRequest(url, site, list(set(transfers[site])),"copy to disk at close-out")
        print result

    print "File Invalidation"
    print invalidation

if __name__ == "__main__":
    url='cmsweb.cern.ch'

    parser = optparse.OptionParser()
    parser.add_option('-t','--test', help='Only test the checkor', action='store_true', default=False)
    parser.add_option('-f','--fetch', help='fetch new stuff not already in assistance', action='store_true', default=False)
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    checkor(url, spec, options=options)
    
    htmlor()
        
