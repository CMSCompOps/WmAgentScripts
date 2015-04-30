#!/usr/bin/env python
from assignSession import *
from utils import getWorkflows, workflowInfo, getDatasetEventsAndLumis, findCustodialLocation, getDatasetEventsPerLumi, siteInfo, getDatasetPresence, campaignInfo, getWorkflowById, makeReplicaRequest
import phedexClient
import dbs3Client
import reqMgrClient
import json
from collections import defaultdict
import optparse
import os
import time
from htmlor import htmlor

class falseDB:
    def __init__(self):
        self.record = json.loads(open('closedout.json').read())

    def table_header(self):
        text = '<table border=1><thead><tr><th>workflow</th><th>OutputDataSet</th><th>%Compl</th><th>acdc</th><th>Dupl</th><th>CorrectLumis</th><th>Scubscr</th><th>Tran</th><th>dbsF</th><th>dbsIF</th><th>\
phdF</th><th>ClosOut</th></tr></thead>'
        return text

    def one_line(self, wf, wfo, count):
        if count%2:            color='lightblue'
        else:            color='white'
        text=""
        try:
            pid = filter(lambda b :b.count('-')==2, wf.split('_'))[0]
        except:
            pid ='None'
        ## return the corresponding html
        order = ['percentage','acdc','duplicate','correctLumis','missingSubs','phedexReqs','dbsFiles','dbsInvFiles','phedexFiles']
        wf_and_anchor = '<a id="%s">%s</a>'%(wf,wf)
        for out in self.record[wf]['datasets']:
            text+='<tr bgcolor=%s>'%color
            text+='<td>%s<br><a href=https://cmsweb.cern.ch/reqmgr/view/details/%s>dts</a>, <a href=https://cmsweb.cern.ch/reqmgr/view/splitting/%s>splt</a>, <a href=https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s>perf</a>, <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s>ac</a>, <a href=assistance.html#%s>%s</a></td>'% (wf_and_anchor,
                                                                                                                                                                                                                                                                                                                            wf, wf, wf,wf,pid,
                                                                                                                                                                                                                                                                                                                            wfo.status)

            text+='<td>%s</td>'% out
            for f in order:
                if f in self.record[wf]['datasets'][out]:
                    value = self.record[wf]['datasets'][out][f]
                else:
                    value = "-NA-"
                if f =='acdc':
                    text+='<td><a href=https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byprepid?key="%s">%s</a></td>'%(pid , value)
                else:
                    text+='<td>%s</td>'% value
            text+='<td>%s</td>'%self.record[wf]['closeOutWorkflow']
            text+='</tr>'
            wf_and_anchor = wf

        return text
    def summary(self):
        os.system('cp closedout.json closedout.json.last')
        
        html = open('/afs/cern.ch/user/c/cmst2/www/unified/closeout.html','w')
        html.write('<html>')
        html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/ target=_blank> logs</a> <br><br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))

        html.write( self.table_header() )

        for (count,wf) in enumerate(sorted(self.record.keys())):
            wfo = session.query(Workflow).filter(Workflow.name == wf).first()
            if not wfo: continue
            if not (wfo.status == 'away' or wfo.status.startswith('assistance')):
                print "Taking",wf,"out of the close-out record"
                self.record.pop(wf)
                continue
            html.write( self.one_line( wf, wfo , count) )

        html.write('</table>')
        html.write('<br>'*100) ## so that the anchor works ok
        html.write('bottom of page</html>')

        open('closedout.json','w').write( json.dumps( self.record , indent=2 ) )

def checkor(url, spec=None, options=None):
    fDB = falseDB()

    wfs=[]
    if options.fetch:
        #workflows = getWorkflows(url, status='completed')
        #for wf in workflows:
        #    wfo = session.query(Workflow).filter(Workflow.name == wf ).first()
        #    if wfo:
        #        if not wfo.status in ['away','assistance']: continue
        #        wfs.append(wfo )
        wfs.extend( session.query(Workflow).filter(Workflow.status == 'away').all() )
        wfs.extend( session.query(Workflow).filter(Workflow.status== 'assistance').all() )
    else:
        ## than get all in need for assistance
        wfs.extend( session.query(Workflow).filter(Workflow.status.startswith('assistance-')).all() )


    custodials = defaultdict(list) #sites : dataset list
    transfers = defaultdict(list) #sites : dataset list
    invalidations = [] #a list of files
    SI = siteInfo()
    CI = campaignInfo()

    for wfo in wfs:
        if spec and not (spec in wfo.name): continue

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
        elif wfo.wm_status in ['failed','aborted','aborted-archived','rejected','rejected-archived']:
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
        pf = 0.95
        if 'Campaign' in wfi.request and wfi.request['Campaign'] in CI.campaigns and 'fractionpass' in CI.campaigns[wfi.request['Campaign']]:
            pf = CI.campaigns[wfi.request['Campaign']]['fractionpass']
            print "overriding fraction to",pf,"for",wfi.request['Campaign']
        if options.fractionpass:
            pf = options.fractionpass
            print "overriding fraction to",pf,"by command line"

        if not all([fract > pf for fract in percent_completions.values()]):
            print wfo.name,"is not completed"
            print json.dumps(percent_completions, indent=2)
            ## hook for creating automatically ACDC ?
            sub_assistance+='-recovery'
            is_closing = False

        ## correct lumi ??< 300 event per lumi
        events_per_lumi = {}
        for output in wfi.request['OutputDatasets']:
            events_per_lumi[output] = getDatasetEventsPerLumi( output )
            if output.endswith('/LHE'):
                events_per_lumi[output] = 0.
                

        if any([ epl> 300. for epl in events_per_lumi.values()]):
            print wfo.name,"has big lumisections"
            print json.dumps(events_per_lumi, indent=2)
            ## hook for rejecting the request ?
            sub_assistance+='-biglumi'
            is_closing = False 

        ## anything running on acdc
        familly = getWorkflowById(url, wfi.request['PrepID'], details=True)
        acdc = []
        for member in familly:
            if member['RequestName'] == wfo.name: continue
            if member['RequestDate'] < wfi.request['RequestDate']: continue
            if member['RequestStatus'] in ['running-opened','running-closed','assignment-approved','assigned','acquired']:
                print wfo.name,"still has a member running",member['RequestName']
                acdc.append( member['RequestName'] )
                #print json.dumps(member,indent=2)
                ## hook for just waiting ...
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
                if len(custodial_locations[output]): custodial = custodial_locations[output][0]
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

            if custodial and not sub_assistance and not acdc:
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

        if not all([dbs_presence[out] == (dbs_invalid[out]+phedex_presence[out]) for out in wfi.request['OutputDatasets']]):
            print wfo.name,"has a dbs,phedex mismatch"
            print json.dumps(dbs_presence, indent=2)
            print json.dumps(phedex_presence, indent=2)
            ## hook for just waiting ...
            is_closing = False

        ## put that heavy part at the end
        ## duplication check
        duplications = {}
        if is_closing:
            for output in wfi.request['OutputDatasets']:
                try:
                    duplications[output] = dbs3Client.duplicateRunLumi( output )
                except:
                    try:
                        duplications[output] = dbs3Client.duplicateRunLumi( output )
                    except:
                        print "was not possible to get the duplicate count for",output
                        is_closing=False

            if any(duplications.values()) and not options.ignoreduplicates:
                print wfo.name,"has duplicates",
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
            rec['correctLumis'] = (events_per_lumi[output] <= 300)
            rec['missingSubs'] = False if len(custodial_locations[output])==0 else ','.join(list(set(custodial_locations[output])))
            rec['dbsFiles'] = dbs_presence[output]
            rec['dbsInvFiles'] = dbs_invalid[output]
            rec['phedexFiles'] = phedex_presence[output]
            rec['acdc'] = len(acdc)

        ## and move on
        if is_closing:
            ## toggle status to closed-out in request manager
            print "setting",wfo.name,"closed-out"
            if not options.test:
                reqMgrClient.closeOutWorkflowCascade(url, wfo.name)
                # set it from away/assistance* to close
                wfo.status = 'close'
                session.commit()
        else:
            print wfo.name,"needs assistance"
            ## that means there is something that needs to be done acdc, lumi invalidation, custodial, name it
            wfo.status = 'assistance'+sub_assistance
            if not options.test:
                print "setting",wfo.name,"to",wfo.status
                session.commit()

    fDB.summary()
    ## custodial requests
    print "Custodials"
    print json.dumps(custodials, indent=2)
    for site in custodials:
        print ','.join(custodials[site]),'=>',site
        if not options.test:
            result = makeReplicaRequest(url, site, list(set(custodials[site])),"custodial copy at production close-out",custodial='y',priority='low')
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
    parser.add_option('--fractionpass',help='The completion fraction that is permitted', default=0.0,type='float')
    parser.add_option('--ignoreduplicates', help='Force ignoring lumi duplicates', default=False, action='store_true')
    parser.add_option('--html',help='make the monitor page',action='store_true', default=False)
    (options,args) = parser.parse_args()
    spec=None
    if len(args)!=0:
        spec = args[0]

    checkor(url, spec, options=options)
    
    if options.html:
        htmlor()

    fdb = falseDB()
        
    short_html = open('/afs/cern.ch/user/c/cmst2/www/unified/assistance_summary.html','w')
    html = open('/afs/cern.ch/user/c/cmst2/www/unified/assistance.html','w')
    html.write("""
<html>
""")
    short_html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/last.log target=_blank> log</a> <br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))
    html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/last.log target=_blank> log</a> <br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))

    html.write('<a href=assistance_summary.html> Summary </a> <br>')    
    short_html.write('<a href=assistance.html> Details </a> <br>')

    assist = defaultdict(list)
    for wfo in session.query(Workflow).filter(Workflow.status.startswith('assistance')).all():
        assist[wfo.status].append( wfo )
    
    for status in sorted(assist.keys()):
        html.write("Workflow in status <b> %s </b>"% status)
        html.write( fdb.table_header())
        short_html.write("""
Workflow in status <b> %s </b>
<table border=1>
<thead>
<tr>
<th> workflow </th> <th> output dataset </th><th> completion </th>
</tr>
</thead>
"""% status )
        
        for (count,wfo) in enumerate(assist[status]):
            if count%2:            color='lightblue'
            else:            color='white'
            if not wfo.name in fdb.record: 
                print "wtf with",wfo.name
                continue
            html.write( fdb.one_line( wfo.name, wfo, count))
            for out in fdb.record[wfo.name]['datasets']:
                short_html.write("""
<tr bgcolor=%s>
<td> <a id=%s>%s</a> </td><td> %s </td><td> <a href=closeout.html#%s>%s</a> </td>
</tr>
"""%( color, 
      wfo.name,wfo.name,
      out, 
      wfo.name,
      fdb.record[wfo.name]['datasets'][out]['percentage'],
      
      ))
        html.write("</table><br><br>")
        short_html.write("</table><br><br>")
    short_html.write("<br>"*100)
    short_html.write("bottom of page</html>")    
    html.write("<br>"*100)
    html.write("bottom of page</html>")    
                           
