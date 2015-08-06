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
            tpid = 'task_'+pid if 'task' in wf else pid
        except:
            pid ='None'
            tpid= 'None'
            
        ## return the corresponding html
        order = ['percentage','acdc','duplicate','correctLumis','missingSubs','phedexReqs','dbsFiles','dbsInvFiles','phedexFiles']
        wf_and_anchor = '<a id="%s">%s</a>'%(wf,wf)
        for out in self.record[wf]['datasets']:
            text+='<tr bgcolor=%s>'%color
            text+='<td>%s<br><a href=https://cmsweb.cern.ch/reqmgr/view/details/%s>dts</a>, <a href=https://cmsweb.cern.ch/reqmgr/view/splitting/%s>splt</a>, <a href=https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s>perf</a>, <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s>ac</a>, <a href=assistance.html#%s>%s</a></td>'% (wf_and_anchor,
                                                                                                                                                                                                                                                                                                                            wf, wf, wf,tpid,wf,
                                                                                                                                                                                                                                                                                                                            wfo.status)

            text+='<td>%s</td>'% out
            for f in order:
                if f in self.record[wf]['datasets'][out]:
                    value = self.record[wf]['datasets'][out][f]
                else:
                    value = "-NA-"
                if f =='acdc':
                    text+='<td><a href=https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byprepid?key="%s">%s</a></td>'%(tpid , value)
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

    use_mcm = True
    up = componentInfo(mcm=use_mcm, soft=['mcm'])
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

        # tuck out DQMIO/DQM
        wfi.request['OutputDatasets'] = [ out for out in wfi.request['OutputDatasets'] if not '/DQM' in out]

        ## anything running on acdc
        familly = getWorkflowById(url, wfi.request['PrepID'], details=True)
        acdc = []
        acdc_inactive = []
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
            else:
                acdc_inactive.append( member['RequestName'] )
        ## completion check
        percent_completions = {}
#        print "let's see who is crashing", wfo.name
#        print wfi.request['TotalInputEvents'],wfi.request['TotalInputLumis']
        if not 'TotalInputEvents' in wfi.request:
            event_expected,lumi_expected = 0,0
            if not 'recovery' in wfo.status:
                sendEmail("missing member of the request","TotalInputEvents is missing from the workload of %s"% wfo.name,'vlimant@cern.ch', ['vlimant@cern.ch','matteoc@fnal.gov','julian.badillo.rojas@cern.ch'])
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

        if not all([percent_completions[out] > fractions_pass[out] for out in fractions_pass]):
            print wfo.name,"is not completed"
            print json.dumps(percent_completions, indent=2)
            print json.dumps(fractions_pass, indent=2)
            ## hook for creating automatically ACDC ?
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
            if campaign in CI.campaigns and 'lumisize' in CI.campaigns[campaign]:
                upper_limit = CI.campaigns[campaign]['lumisize']
                print "overriding the upper lumi size to",upper_limit,"for",campaign
            if options.lumisize:
                upper_limit = options.lumisize
                print "overriding the upper lumi size to",upper_limit,"by command line"
            lumi_upper_limit[output] = upper_limit
        
        if any([ events_per_lumi[out] > lumi_upper_limit[out] for out in events_per_lumi]):
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
                if len(parents_custodial):
                    custodial = parents_custodial[0]
                else:
                    print "the input dataset",wfi.request['InputDataset'],"does not have custodial in the first place. abort"
                    sendEmail( "dataset has no custodial location", "Please take a look at %s in the logs of checkor"%wfi.request['InputDataset'],
                               'vlimant@cern.ch', ['vlimant@cern.ch','matteoc@fnal.gov'])
                    is_closing = False
                    pick_custodial = False

            if not custodial and pick_custodial:
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
            print wfo.name,"needs assistance"
            ## that means there is something that needs to be done acdc, lumi invalidation, custodial, name it
            new_status = 'assistance'+sub_assistance
            
            if sub_assistance and wfo.status != new_status and 'PrepID' in wfi.request:
                pid = wfi.request['PrepID'].replace('task_','')
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
                print "Sending notification back to requestor"
                if use_mcm and content:
                    batches = mcm.getA('batches',query='contains=%s&status=announced'%pid)
                    if len(batches):
                        ## go notify the batch
                        bid = batches[-1]['prepid']
                        print "batch nofication to",bid
                        mcm.put('/restapi/batches/notify', { "notes" : text, "prepid" : bid})


                    ## go notify the request
                    print "request notification to",pid
                    mcm.put('/restapi/requests/notify',{ "message" : text, "prepids" : [pid] })

                
            wfo.status = new_status
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
        html.write("Workflow in status <b> %s </b> (%d)"% (status, len(assist[status])))
        html.write( fdb.table_header())
        short_html.write("""
Workflow in status <b> %s </b>
<table border=1>
<thead>
<tr>
<th> workflow </th> <th> output dataset </th><th> completion </th>
</tr>
</thead>
"""% (status))
        
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
                           
