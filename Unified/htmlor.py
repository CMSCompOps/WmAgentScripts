#!/usr/bin/env python
from assignSession import *
import time
from utils import getWorkLoad, campaignInfo, siteInfo, getWorkflows, unifiedConfiguration, getPrepIDs, componentInfo, getAllAgents, sendLog, moduleLock, dataCache, agentInfo, display_time, eosFile, eosRead, StartStopInfo, remainingDatasetInfo
import os
import json
from collections import defaultdict
import sys
from utils import monitor_dir, base_dir, phedex_url, reqmgr_url, monitor_pub_dir, unified_url_eos, monitor_eos_dir, monitor_pub_eos_dir, base_eos_dir, closeoutInfo, agent_speed_draining, statusHistory
import random
from JIRAClient import JIRAClient

def htmlor( caller = ""):
    mlock = moduleLock(silent=True)
    if mlock(): return

    up = componentInfo(soft=['mcm','wtc'])
    if not up.check(): return 

    for backup in ['statuses.json','siteInfo.json','listProtectedLFN.txt','equalizor.json']:
        print "copying",backup,"to old location"
        os.system('cp %s/%s /afs/cern.ch/user/c/cmst2/www/unified/.'%(monitor_pub_dir, backup))
        #os.system('cp %s/%s %s/.'%(monitor_dir, backup, monitor_pub_dir))

    try:
        boost = json.loads(eosRead('%s/equalizor.json'%monitor_pub_dir))['modifications']
    except:
        boost = {}
    cache = getWorkflows(reqmgr_url,'assignment-approved', details=True)
    cache.extend( getWorkflows(reqmgr_url,'acquired', details=True) )
    cache.extend( getWorkflows(reqmgr_url,'running-open', details=True) )
    cache.extend( getWorkflows(reqmgr_url,'running-closed', details=True) )
    def getWL( wfn ):
        cached = filter(lambda d : d['RequestName']==wfn, cache)
        if cached:
            wl = cached[0]
        else:
            wl = getWorkLoad(reqmgr_url,wfn)
        return wl

    def wfl(wf,view=False,p=False,ms=False,within=False,ongoing=False,status=False,update=False):
        wfn = wf.name
        wfs = wf.wm_status
        wl = None
        pid = None
        wl_pid = None
        pids=filter(lambda seg: seg.count('-')==2, wf.name.split('_'))
        if len(pids):
            pids = pids[:1]
            pid=pids[0]
            
        if not pids:
            wl = getWL( wf.name )
            pids = getPrepIDs( wl )
            pid = pids[0]

        wl_pid = pid
        if 'task' in wf.name:
            wl_pid = 'task_'+pid

        
        text = "<div>%s</div> "%wfn
        #text=', '.join([
                #wfn,
                #'<a href="https://cmsweb.cern.ch/reqmgr/view/details/%s" target="_blank">%s</a> '%(wfn,wfn),
                #'<table><tr><td>%s</td></tr></table>'%(wfn),
                #'<span>%s</span>'%(wfn),
                #"<div>%s</div> "%wfn,
                #'(%s)'%wfs])
        text+=', '.join([
                '(%s)'%wfs,
                '<a href="https://%s/reqmgr2/fetch?rid=%s" target="_blank">dts</a>'%(reqmgr_url,wfn),
                ##'<a href="https://cmsweb.cern.ch/reqmgr/view/details/%s" target="_blank">dts-req1</a>'%wfn,
                #TOFIX '<a href=https://cmsweb.cern.ch/reqmgr/view/showWorkload?requestName=%s target="_blank">wkl</a>'%wfn,
                #'<a href="https://%s/couchdb/reqmgr_workload_cache/%s" target="_blank">wfc</a>'%(reqmgr_url,wfn),
                '<a href="https://%s/reqmgr2/data/request?name=%s" target="_blank">req</a>'%(reqmgr_url,wfn),
                #'<a href="https://cmsweb.cern.ch/reqmgr/reqMgr/request?requestName=%s" target="_blank">dwkc</a>'%wfn,
                #TOFIX '<a href="https://cmsweb.cern.ch/reqmgr/view/splitting/%s" target="_blank">spl</a>'%wfn,
                '<a href="https://cms-pdmv.cern.ch/stats/?RN=%s" target="_blank">vw</a>'%wfn,
                '<a href="https://cms-pdmv.cern.ch/stats/restapi/get_one/%s" target="_blank">vwo</a>'%wfn,
                '<a href="https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=full&reverse=0&reverse=1&npp=20&subtext=%s&sall=q" target="_blank">elog</a>'%pid,
                '<a href="https://cms-gwmsmon.cern.ch/prodview/%s" target="_blank">pv</a>'%wfn,
                #deprecated '<a href="https://cmsweb.cern.ch/reqmgr/reqMgr/outputDatasetsByRequestName/%s" target="_blank">out</a>'%wfn,
                '<a href="closeout.html#%s" target="_blank">clo</a>'%wfn,
                '<a href="statuses.html#%s" target="_blank">st</a>'%wfn,
                '<a href="https://%s/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s" target="_blank">perf</a>'%(reqmgr_url,wfn),
                #'<a href="http://dabercro.web.cern.ch/dabercro/unified/showlog/?search=%s" target="_blank">history</a>'%(pid),
                '<a href="https://cms-unified.web.cern.ch/cms-unified/showlog/?search=%s" target="_blank">history</a>'%(pid),
                ])
        if within and (not view or wfs=='completed'):
            wl = getWL( wfn )
            dataset =None
            if 'InputDataset' in wl:
                dataset = wl['InputDataset']                
            if 'Task1' in wl and 'InputDataset' in wl['Task1']:
                dataset = wl['Task1']['InputDataset']

            if dataset:
                text+=', '.join(['',
                                 '<a href=https://cmsweb.cern.ch/das/request?input=%s target=_blank>input</a>'%dataset,
                                 '<a href=https://cmsweb.cern.ch/phedex/prod/Data::Subscriptions#state=create_since=0;filter=%s target=_blank>sub</a>'%dataset,
                                 '<a href=https://cmsweb.cern.ch/phedex/datasvc/xml/prod/subscriptions?dataset=%s&collapse=n target=_blank>ds</a>'%dataset,
                                 '<a href=https://cmsweb.cern.ch/phedex/datasvc/xml/prod/blockreplicas?dataset=%s target=_blank>rep</a>'%dataset,
                                 ])

        if p:
            cached = filter(lambda d : d['RequestName']==wfn, cache)
            if cached:
                wl = cached[0]
            else:
                wl = getWorkLoad('cmsweb.cern.ch',wfn)
            text+=', (%s)'%(wl['RequestPriority'])
            pass

        if pid:
            if ms:
                mcm_s = json.loads(os.popen('curl https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_status/%s --insecure'%pid).read())[pid]
                text+=', <a href="https://cms-pdmv.cern.ch/mcm/requests?prepid=%s" target="_blank">mcm (%s)</a>'%(pid,mcm_s)
            else:
                text+=', <a href="https://cms-pdmv.cern.ch/mcm/requests?prepid=%s" target="_blank">mcm</a>'%(pid)
                text+=', <a href="https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s" target="_blank">ac</a>'%(wl_pid)
        text += ', <a href="https://%s/couchdb/workqueue/_design/WorkQueue/_rewrite/elementsInfo?request=%s" target="_blank">gq</a>'%(reqmgr_url,wfn)
        text += ', <a href="https://its.cern.ch/jira/issues/?jql=(text~%s OR text~task_%s) AND project = CMSCOMPPR" target="_blank">jira</a>'% (pid, pid)
        if status:
            if wf.status.startswith('assistance'):
                text+=', <a href="assistance.html#%s" target="_blank">assist</a>'%wfn
            text+=' : %s '%(wf.status)

        #if view and not wfs in ['acquired','assigned','assignment-approved']:
        #    text+='<a href="https://cms-pdmv.web.cern.ch/cms-pdmv/stats/growth/%s.gif" target="_blank"><img src="https://cms-pdmv.web.cern.ch/cms-pdmv/stats/growth/%s.gif" style="height:50px"></a>'%(wfn.replace('_','/'),wfn.replace('_','/'))

        if ongoing:
            #wl = getWL( wfn )            
            #if 'running' in wl['RequestStatus']:
            if wfs!='acquired':
                text+='<a href="https://cms-gwmsmon.cern.ch/prodview/%s" target="_blank"><img src="https://cms-gwmsmon.cern.ch/prodview/graphs/%s/daily" style="height:50px"></a>'%(wfn,wfn)

        if ongoing:
            if not os.path.isfile('%s/report/%s'%(monitor_dir,wfn)):
                if (random.random() < 0.005):
                    #print wfn,"report absent, doing it"
                    print wfn,"report absent, NOT doing i. Too expensive"
                    pass
                    #os.system('python Unified/showError.py -w %s'%(wfn))
                    #text += '<a href=report/%s target=_blank>report</a>'%wfn
                else:
                    #print wfn,"report absent, could be doing it"
                    pass
            else:
                text += '<a href=report/%s target=_blank>report</a>'%wfn
                #text += ' <a href=%/report/%s target=_blank>e_report</a>'%(unified_url_eos,wfn)

            date2 = time.strftime('%Y-%m-%d+%H:%M', time.gmtime())

            date1 = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(30*24*60*60)) )
            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#table=Jobs&date1=%s&date2=%s&sortby=site&task=wmagent_%s"> 1m</a>'%( date1, date2, wfn )
            date1 = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(7*24*60*60)) )
            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#table=Jobs&date1=%s&date2=%s&sortby=site&task=wmagent_%s"> 1w</a>'%( date1, date2, wfn )
            date1 = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(1*24*60*60)) )
            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#table=Jobs&date1=%s&date2=%s&sortby=site&task=wmagent_%s">1d</a>'%( date1, date2, wfn )
            date1 = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(5*60*60)) )
            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#table=Jobs&date1=%s&date2=%s&sortby=site&task=wmagent_%s"> 5h</a>'%( date1, date2, wfn )
            date1 = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(1*60*60)) )
            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#table=Jobs&date1=%s&date2=%s&sortby=site&task=wmagent_%s"> 1h</a>'%( date1, date2, wfn )

        if ongoing and wfn in boost:
            for task in boost[wfn]:
                overflow = boost[wfn][task].get('ReplaceSiteWhitelist',None)
                if not overflow:
                    overflow = boost[wfn][task].get('AddWhitelist',None)
                if overflow:
                    text+=',boost (<a href=public/equalizor.json>%d</a>)'%len(overflow)

        #text+="<hr>"
        return text


    def phl(phid):
        text=', '.join([
                str(phid),
                '<a href="https://cmsweb.cern.ch/phedex/prod/Request::View?request=%s" target="_blank">vw</a>'%phid,
                '<a href="https://cmsweb.cern.ch/phedex/prod/Data::Subscriptions?reqfilter=%s" target="_blank">sub</a>'%phid,
                ])
        return text
            

    def ol(out):
        return '<a href="https://cmsweb.cern.ch/das/request?input=%s" target="_blank"> %s</a>'%(out,out)


    def lap( comment ):
        
        l = time.mktime(time.gmtime())
        spend = l-lap.start
        lap.start =l 
        print "Spend %d [s] for %s"%( spend, comment )
    lap.start = time.mktime(time.gmtime())

    ## start to write it
    html_doc = eosFile('%s/index.html'%monitor_dir)
    print "Updating the status page ..." 

    UC = unifiedConfiguration()

    if not caller:
        try:
            #caller = sys._getframe(1).f_code.co_name
            caller = sys.argv[0].split('/')[-1].replace('.py','')
            print "caller is"
            print caller
        except Exception as es:
            caller = 'none found'
            print "not getting frame"
            print str(es)


    summary_content = {}

    view_not_a_module = ['agentInfo','componentInfo']
    view_modules = ['injector','batchor','transferor','cachor','stagor','assignor','completor','GQ','equalizor','checkor','recoveror','actor','closor']+view_not_a_module

    all_modules = list(set(view_modules + ['actor','addHoc','assignor','batchor','cachor','checkor','closor','completor','efficiencor','equalizor','GQ','htmlor','injector','lockor','messagor','pushor','recoveror','remainor','showError','stagor','stuckor','subscribor','transferor']))

    html_doc.write("""
<html>
<head>
<META HTTP-EQUIV="refresh" CONTENT="900">
<script type="text/javascript">
 function showhide(id) {
    var e = document.getElementById(id);
    e.style.display = (e.style.display == 'block') ? 'none' : 'block';
 }
</script>
</head>
<body>

<br>
Last update on <b>%s(CET), %s(GMT)</b>
<br>
<hr>
<a href=info.txt target=_blank title="Some basic running info">info</a>
<a href=logs/ target=_blank title="Directory containing all the logs">logs</a> 
<a href=http://cms-unified.web.cern.ch/cms-unified/joblogs/ target=_blank title="Directory containing logs of jobs that failed with critical errors">job logs</a> 
<a href=http://cms-unified.web.cern.ch/cms-unified/condorlogs/ target=_blank title="Directory containing condor logs of jobs ">condor logs</a> 
<a href=logs/last.log target=_blank title="Log of the last module that has run">last</a>
<a href=statuses.html title="Unified statuses">statuses</a>
<a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/ target=_blank>prod mon</a>
<a href=https://%s/wmstats/index.html target=_blank>wmstats</a>
<a href=http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt target=_blank>detox</a>
<a href=http://dynamo.mit.edu/dynamo/detox.php target=_blank>dynamo</a>
<a href=locked.html>space</a>
<a href=outofspace.html>out of space</a>
<a href=remaining.html>locked</a>
<a href=logs/subscribor/last.log target=_blank>blocks</a>
<br>
<a href=data.html>json interfaces</a>
<a href=logs/addHoc/last.log>add-hoc op</a>
<a href=https://cmssst.web.cern.ch/cmssst/man_override/cgi/manualOverride.py/prodstatus target=_blank title="Link to a restricted page to override sites status">sites override</a>
<a href=https://cms-unified.web.cern.ch/cms-unified/showlog/?search=warning target=_blank><b><font color=orange>warning</b></font></a>
<a href=https://cms-unified.web.cern.ch/cms-unified/showlog/?search=critical target=_blank><b><font color=red>all critical</b></font></a>
<a href=https://its.cern.ch/jira/projects/CMSCOMPPR/issues target=_blank>JIRA</a>
<a href=toperror.html target=_blank>top errors</a>
<br>
%s
<hr>
<a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/><img src=https://dmytro.web.cern.ch/dmytro/cmsprodmon/images/campaign-RunningCpus.png style="height:150px"></a>
<a href=https://cms-gwmsmon.cern.ch/prodview><img src=https://cms-gwmsmon.cern.ch/prodview/graphs/prioritysummarycpusinuse/weekly style="height:150px" alt="Click here if it does not load"></a>
<a href=https://cms-gwmsmon.cern.ch/prodview><img src=https://cms-gwmsmon.cern.ch/prodview/graphs/prioritysummarycpuspending/weekly/log style="height:150px" alt="Click here if it does not load"></a>
<hr>
<br>

""" %(time.asctime(time.localtime()),
      time.asctime(time.gmtime()),
      reqmgr_url,
      ', '.join(['<a href=https://cms-unified.web.cern.ch/cms-unified/showlog/?search=critical&module=%s&limit=100 target=_blank><b><font color=red>%s</b></font></a>'%(m,m) for m in ['heartbeat']+view_modules])
      )
                   )
        
    text=""
    count=0
    count_by_campaign=defaultdict(lambda : defaultdict(int))
    for wf in session.query(Workflow).filter(Workflow.status.startswith('considered')).all():
        wl = getWL( wf.name )
        count_by_campaign[wl['Campaign']][int(wl['RequestPriority'])]+=1
        #print wf.name
        text+="<li> %s (%d) </li> \n"%(wfl(wf,p=True), int(wl['RequestPriority']))
        count+=1
    text_by_c=""
    for c in count_by_campaign:
        text_by_c+='<li><a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?campaign=%s&in_production=1>%s</a> <a href="https://its.cern.ch/jira/issues/?jql=text~%s AND project = CMSCOMPPR">JIRA</a> (%d) : '%( c,c,c, sum(count_by_campaign[c].values()) )
        for p in sorted(count_by_campaign[c].keys()):
            text_by_c+="%d (%d), "%(p,count_by_campaign[c][p])
        text_by_c+="</li>"

    html_doc.write("""
Worflow next to handle (%d) <a href=https://cms-pdmv.cern.ch/mcm/batches?status=new&page=-1 target="_blank"> batches</a> <a href=logs/injector/last.log target=_blank>log</a> <a href=logs/transferor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('considered')">[Click to show/hide]</a>
<br>
<div id="considered" style="display:none;">
<ul>
<li> By workflow (%d) </li><a href="javascript:showhide('considered_bywf')">[Click to show/hide]</a><div id="considered_bywf" style="display:none;">
 <ul>
 %s
 </ul></div>
<li> By campaigns (%d) </li><a href="javascript:showhide('considered_bycamp')">[Click to show/hide]</a><div id="considered_bycamp" style="display:none;">
 <ul>
 %s
 </ul></div>
</ul>
</div>
"""%(count,
     count, text,
     len(count_by_campaign), text_by_c))
                   
    lap( 'done with considered' )
    text=""
    count=0
    count_by_campaign=defaultdict(lambda : defaultdict(int))
    for wf in session.query(Workflow).filter(Workflow.status=='staging').all():
        wl = getWL( wf.name )
        count_by_campaign[wl['Campaign']][int(wl['RequestPriority'])]+=1
        text+="<li> %s (%d)</li> \n"%(wfl(wf,within=True), int(wl['RequestPriority']))
        count+=1

    text_by_c=""

    summary_content['staging'] = count
    summary_content['staging_by_campaign'] = len(count_by_campaign)

    for c in count_by_campaign:
        text_by_c+='<li><a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?campaign=%s&in_production=1>%s</a> <a href="https://its.cern.ch/jira/issues/?jql=text~%s AND project = CMSCOMPPR">JIRA</a> (%d) : '%( c,c,c, sum(count_by_campaign[c].values()) )
        for p in sorted(count_by_campaign[c].keys()):
            text_by_c+="%d (%d), "%(p,count_by_campaign[c][p])
        text_by_c+="</li>"


    html_doc.write("""
Worflow waiting in staging (%d) <a href=logs/transferor/last.log target=_blank>log</a> <a href=logs/stagor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('staging')">[Click to show/hide]</a>
<br>
<div id="staging" style="display:none;">
<ul>
<li> By workflow (%d) </li><a href="javascript:showhide('staging_bywf')">[Click to show/hide]</a><div id="staging_bywf" style="display:none;">                                                                                                                                                                       
 <ul>            
 %s
 </ul></div>
<li> By campaigns (%d) </li><a href="javascript:showhide('staging_bycamp')">[Click to show/hide]</a><div id="staging_bycamp" style="display:none;">                                                                                                                                                                  
 <ul>                                                                                                                                                                                                                                                                                                                      
 %s                                                                                                                                                                                                                                                                                                                        
 </ul></div>                                                                                                                                                                                                                                                                                                               
</ul>      
</div>
"""%(count, 
     count, text,
     len(count_by_campaign), text_by_c))

    lap ( 'done with staging' )

    text_bytr="<ul>"
    count=0
    transfer_per_wf = defaultdict(list)
    all_active = sorted(set([ts.phedexid for ts in session.query(TransferImp).filter(TransferImp.active == True).all()]))
    for phedexid in all_active:
        hide = True
        t_count = 0
        stext=""
        for imp in session.query(TransferImp).filter(TransferImp.phedexid == phedexid).all():
            w = imp.workflow
            if not w: continue
            hide &= (w.status != 'staging' )
            if w.status in ['considered','staging','staged']:
                stext += "<li> %s </li>\n"%( wfl(w,status=True))
                transfer_per_wf[w].append( imp.phedexid )
                t_count +=1
        stext = '<li> %s serves %d workflows<br><a href="javascript:showhide(\'%s\')">[show/hide]</a> <div id="%s" style="display:none;"><ul>\n'%( phl(phedexid),
                                                                                                                                                   t_count,
                                                                                                                                                   phedexid,
                                                                                                                                                   phedexid) + stext

        stext+="</ul></li>\n"
        if hide:
            #text+="<li> %s not needed anymore to start running (does not mean it went through completely)</li>"%phl(ts.phedexid)
            pass
        else:
            count+=1
            text_bytr+=stext
    text_bytr+="</ul>"
    
    text_bywf="<ul>"
    for wf in transfer_per_wf:
        text_bywf += "<li> %s </li>"%(wfl(wf,within=True))
        text_bywf += '<a href=javascript:showhide("transfer_%s")>[Click to show/hide] %d transfers</a>'% (wf.name, len(transfer_per_wf[wf]))
        text_bywf += '<div id="transfer_%s" style="display:none;">'% wf.name
        text_bywf += "<ul>"
        for pid in sorted(transfer_per_wf[wf]):
            text_bywf += "<li> %s </li>"%(phl(pid))
        text_bywf += "</ul></div><hr>"
    text_bywf += '</ul>'

    try:
        stuck_transfer = json.loads(eosRead('%s/stuck_transfers.json'%monitor_pub_dir))
    except:
        stuck_transfer = {}
        print "eos is screwing with us"

    html_doc.write("""
Transfer on-going (%d) <a href=http://cmstransferteam.web.cern.ch/cmstransferteam/ target=_blank>dashboard</a> <a href=logs/transferor/last.log target=_blank>log</a> <a href=logs/stagor/last.log target=_blank>postlog</a> <a href=public/stuck_transfers.json target=_blank> %d stuck</a>
<a href="javascript:showhide('transfer')">[Click to show/hide]</a>
<br>
<div id="transfer" style="display:none;">
 <ul>
  <li> By Workflow
    <a href="javascript:showhide('transfer_bywf')">[Click to show/hide]</a>
    <div id="transfer_bywf" style="display:none;">
%s
    </div>
  </li>
  <li> By transfer request
    <a href="javascript:showhide('transfer_byreq')">[Click to show/hide]</a>
    <div id="transfer_byreq" style="display:none;"> 
%s
    </div>
  </li>
 </ul>
</div>
"""%(count,
     len( stuck_transfer ),
     text_bywf,
     text_bytr))
    summary_content['stuck_placement'] = len(stuck_transfer)

    lap( 'done with transfers' )

    text=""
    count=0
    count_by_campaign=defaultdict(lambda : defaultdict(int))
    for wf in session.query(Workflow).filter(Workflow.status=='staged').all():
        wl = getWL( wf.name )
        count_by_campaign[wl['Campaign']][int(wl['RequestPriority'])]+=1
        text+="<li> %s </li> \n"%wfl(wf,p=True)
        count+=1
    text_by_c=""
    for c in count_by_campaign:
        text_by_c+='<li><a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?campaign=%s&in_production=1>%s</a> <a href="https://its.cern.ch/jira/issues/?jql=text~%s AND project = CMSCOMPPR">JIRA</a> (%d) : '%( c,c,c, sum(count_by_campaign[c].values()) )
        for p in sorted(count_by_campaign[c].keys()):
            text_by_c+="%d (%d), "%(p,count_by_campaign[c][p])
        text_by_c+="</li>"

    html_doc.write("""Worflow ready for assigning (%d) <a href=logs/stagor/last.log target=_blank>log</a> <a href=logs/assignor/last.log target=_blank>postlog</a> <a href=GQ.txt target=_blank> GQ</a>
<a href="javascript:showhide('staged')">[Click to show/hide]</a>
<br>
<div id="staged" style="display:none;">
<br>
<ul>
<li> By workflow (%d) </li><a href="javascript:showhide('staged_bywf')">[Click to show/hide]</a><div id="staged_bywf" style="display:none;">                                                                                                                                                                             
 <ul>                                                                                                                                                                                                                                                                                                                      
 %s                                                                                                                                                                                                                                                                                                                        
 </ul></div>                                                                                                                                                                                                                                                                                                               
<li> By campaigns (%d) </li><a href="javascript:showhide('staged_bycamp')">[Click to show/hide]</a><div id="staged_bycamp" style="display:none;">                                                                                                                                                                        
 <ul>                                                                                                                                                                                                                                                                                                                      
 %s                                                                                                                                                                                                                                                                                                                        
 </ul></div>
</ul>
</div>
"""%(count, 
     count, text,
     len(count_by_campaign), text_by_c))

    lap( 'done with staged' )
    
    lines=[]
    count_by_campaign=defaultdict(lambda : defaultdict(int))
    count = 0
    for wf in session.query(Workflow).filter(Workflow.status=='away').all():
        wl = getWL( wf.name )
        count_by_campaign[wl['Campaign']][int(wl['RequestPriority'])]+=1
        #color = 'orange' if wf.name in relvals else 'black' ## this difference of color can be put back somehow using batchInfo
        color = 'black'
        lines.append("<li> <font color=%s>%s</font> <hr></li>"%(color,wfl(wf,view=True,ongoing=True)))
        count += 1
    text_by_c=""

    summary_content['ongoing'] = count
    summary_content['ongoing_by_campaign'] = len(count_by_campaign)

    for c in sorted(count_by_campaign.keys()):
        text_by_c+="""
<li> <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?campaign=%s>%s</a> <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?in_production=1&campaign=%s>(%d)</a> 
<a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/campaign.php?campaign=%s>mon</a>
<a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?in_production=1&rsort=6&status=running&campaign=%s>top</a>
<a href=https://cms-pdmv.cern.ch/pmp/historical?r=%s target=_blank>pmp</a> 
"""%( c,c,c,
      sum(count_by_campaign[c].values()),c,c,c )
        for p in sorted(count_by_campaign[c].keys()):
            text_by_c+="%d (%d), "%(p,count_by_campaign[c][p])
        text_by_c += '<img src=https://dmytro.web.cern.ch/dmytro/cmsprodmon/images/%s-history_nevents-limit-30.png style="height:70px">'% (c) 
        text_by_c += '<img src=https://dmytro.web.cern.ch/dmytro/cmsprodmon/images/%s-history_requests-limit-30.png style="height:70px">'% (c) 
        text_by_c+="</li>"

    lines.sort()
    html_doc.write("""
Worflow on-going (%d) <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests_in_production.php target=_blank>ongoing</a> <a href=https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=summary target=_blank>elog</a> <a href=https://cms-gwmsmon.cern.ch/prodview target=_blank>queues</a> <a href=logs/assignor/last.log target=_blank>log</a> <a href=logs/checkor/last.log target=_blank>postlog</a> <a href=logs/equalizor/last.log target=_blank>equ</a> <a href=logs/completor/last.log target=_blank>comp</a> <a href="https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?in_production=1&rsort=6&older=3">lasting</a>
<a href="javascript:showhide('away')">[Click to show/hide]</a>
<br>
<div id="away" style="display:none;">
<ul> 
<li>By workflow (%d) </li>
<a href="javascript:showhide('away_bywf')">[Click to show/hide]</a><div id="away_bywf" style="display:none;">
<ul>
%s
</ul></div>
<li> By campaigns (%d) </li><a href="javascript:showhide('away_bycamp')">[Click to show/hide]</a><div id="away_bycamp" style="display:none;">
<ul>
%s
</ul></div>
</ul>
</div>
"""%(len(lines),
     len(lines),
     '\n'.join(lines),
     len(count_by_campaign),
     text_by_c
     ))


    lap ( 'done with away' )

    text=""
    count=0
    #for wf in session.query(Workflow).filter(Workflow.status == 'assistance-custodial').all():
    for wf in session.query(Workflow).filter(Workflow.status.startswith('assistance')).filter(Workflow.status.contains('custodial')).all():
        text+="<li> %s </li> \n"%wfl(wf,view=True,update=True,status=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""Worflow that are closing (%d)
<a href=closeout.html target=_blank>closeout</a> 
<a href=logs/checkor/last.log target=_blank>log</a> <a href=logs/closor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('closing')">[Click to show/hide]</a>
<br>
<div id="closing" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    lap ( 'done with closing' )

    assistance_by_type = defaultdict(list)
    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status.startswith('assistance-')).all():
        assistance_by_type[wf.status].append( wf )
        count+=1
    for assistance_type in sorted(assistance_by_type.keys()):
        text += "<li> %s (%d) <a href=\"javascript:showhide('%s')\">[Click to show/hide]</a><br><div id=\"%s\" style=\"display:none;\"><ul>"%( assistance_type,
                                                                                                                                               len(assistance_by_type[assistance_type]),
                                                                                                                                               assistance_type,
                                                                                                                                               assistance_type,
                                                                                                                                               )
        for wf in assistance_by_type[assistance_type]:
            text+="<li> %s <hr></li> \n"%wfl(wf,view=True,within=True,status=True,update=True)
        text += "</ul></div></li>\n"
    html_doc.write("""Worflow which need assistance (%d)
<a href=assistance.html target=_blank>assistance</a> 
<a href=logs/checkor/last.log target=_blank>log</a> <a href=logs/recoveror/last.log target=_blank>postlog</a>
<a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?in_production=1&rsort=6&older=3&status=validation target=_blank>lasting</a>
<a href="javascript:showhide('assistance')">[Click to show/hide]</a>
<br>
<div id="assistance" style="display:none;">
<br>
<ul>
%s
</ul>
</div>
"""%(count, text))
    
    lap ( 'done with assistance' )

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status == 'close').all():
        text+="<li> %s </li> \n"%wfl(wf)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""Worflow ready to close (%d)
<a href=logs/checkor/last.log target=_blank>log</a> <a href=logs/closor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('close')">[Click to show/hide]</a>
<br>
<div id="close" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    lap ( 'done with annoucing' )

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='trouble').all():
        text+="<li> %s </li> \n"%wfl(wf)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""Worflow with issue (%d) <a href=logs/closor/last.log target=_blank>log</a> <a href=logs/injector/last.log target=_blank>postlog</a>
<a href="javascript:showhide('trouble')">[Click to show/hide]</a>
<br>
<div id="trouble" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    lap ( 'done with trouble' )

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='forget').all():
        text+="<li> %s </li> \n"%wfl(wf)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worflow to forget (%d) <a href=logs/injector/last.log target=_blank>log</a> <a href=logs/lockor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('forget')">[Click to show/hide]</a>
<br>
<div id="forget" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    lap ( 'done with forget' )

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='done').all():
        text+="<li> %s </li> \n"%wfl(wf)#,ms=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worflow through (%d) <a href=logs/closor/last.log target=_blank>log</a> <a href=logs/lockor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('done')">[Click to show/hide]</a>
<br>
<div id="done" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    lap ( 'done with done' )


    wfs = session.query(Workflow).filter(Workflow.status.endswith('-unlock')).all()
    html_doc.write(" Workflows unlocked : %s <a href=logs/lockor/last.log target=_blank>log</a><br>"%(len(wfs)))
    lap ( 'done with unlocked' )



    text=""
    lines_thisweek=[]
    lines_lastweek=[]
    now = time.mktime(time.gmtime())
    this_week = int(time.strftime("%W",time.gmtime()))
    start_time_two_weeks_ago = time.mktime(time.gmtime(now - (20*24*60*60))) # 20
    last_week =  int(time.strftime("%W",time.gmtime(now - ( 7*24*60*60))))

    all_locks = [l.item.split('#')[0] for l in session.query(Lock).filter(Lock.lock == True).all()]
    waiting_custodial = json.loads(eosRead('%s/waiting_custodial.json'%monitor_dir))
    all_pending_approval_custodial = dict([(k,item) for k,item in waiting_custodial.items() if 'nodes' in item and not any([node['decided'] for node in item['nodes'].values()]) ])
    n_pending_approval = len( all_pending_approval_custodial )
    #n_pending_approval = len([item for item in waiting_custodial.values() if 'nodes' in item and not any([node['decided'] for node in item['nodes'].values() ])])
    missing_approval_custodial = json.loads(eosRead('%s/missing_approval_custodial.json'%monitor_dir))

    try:
        stuck_custudial = json.loads(eosRead('%s/stuck_custodial.json'%monitor_pub_dir))
    except:
        stuck_custudial = {}
        print "eos is screwing with us"

    try:
        lagging_custudial = json.loads(eosRead('%s/lagging_custodial.json'%monitor_dir))
    except:
        lagging_custudial = {}
        print "eos is screwing with us"

    if len(stuck_custudial):
        stuck_string = ', <font color=red>%d appear to be <a href=public/stuck_custodial.json>stuck</a></font>'% len(stuck_custudial)
    else:
        stuck_string = ''
    if len(missing_approval_custodial):
        long_approve_string = ', <font color=red>%d more than %d days</font>'%( len(missing_approval_custodial), UC.get('transfer_timeout'))
    else:
        long_approve_string = ''
    

    output_within_two_weeks=session.query(Output).filter(Output.date>=start_time_two_weeks_ago).all()
    waiting_custodial_string=""
    waiting_custodial_strings=[]
    for ds in waiting_custodial:
        out = None
        ## lots of it will be within two weeks
        of = filter(lambda odb: odb.datasetname == ds, output_within_two_weeks)
        if of:
            out = of[0]
        else:
            out = session.query(Output).filter(Output.datasetname == ds).first()
        if out:
            info = waiting_custodial[out.datasetname]
            action = 'going'
            if out.datasetname in all_pending_approval_custodial:
                action = '<font color=red>pending</font>'
            try:
                size = str(info['size'])
            except:
                size = "x"

            destination = ",".join(info['nodes'].keys())
            if not destination:
                destination ='<font color=red>NO SITE</font>'

            a_waiting_custodial_string = "<li>on week %s : %s %s</li>"%(
                time.strftime("%W (%x %X)",time.gmtime(out.date)),
                ol(out.datasetname),
                ' %s [GB] %s to %s on %s (<a href="https://cmsweb.cern.ch/phedex/datasvc/xml/prod/requestlist?dataset=%s&node=T*MSS">%d missing</a>)'%( size, action, destination, time.asctime(time.gmtime(info['checked'])), out.datasetname, info['nmissing'])
                )
            waiting_custodial_strings.append( (out.date, a_waiting_custodial_string) )

        waiting_custodial_strings.sort( key = lambda i:i[0] )
        waiting_custodial_string="\n".join( [i[1] for i in waiting_custodial_strings] )
    #start_time_two_weeks_ago = time.mktime(time.strptime("15-0-%d"%(this_week-2), "%y-%w-%W"))
    per_day_this_week = defaultdict(int)
    for out in output_within_two_weeks:
        if not out.workflow: 
            print "This is a problem with",out.datasetname
            continue
        if  out.workflow.status in ['done-unlock','done','clean','clean-out','clean-unlock']:
            custodial=''
            if out.datasetname in waiting_custodial:
                info = waiting_custodial[out.datasetname]
                try:
                    try:
                        size = str(info['size'])
                    except:
                        size = "x"
                    destination = ",".join(info['nodes'].keys())
                    if not destination:
                        destination ='<font color=red>NO SITE</font>'
                    action = 'going'
                    if out.datasetname in all_pending_approval_custodial:
                        action = '<font color=red>pending</font>'

                    
                    custodial=' %s [GB] %s to %s on %s (<a href="https://cmsweb.cern.ch/phedex/datasvc/xml/prod/requestlist?dataset=%s&node=T*MSS">%d missing</a>)'%( size, action, destination, time.asctime(time.gmtime(info['checked'])), out.datasetname, info['nmissing'])
                except Exception as e:
                    #print info
                    #print str(e)
                    pass
            elif out.datasetname in all_locks:
                custodial='<font color=green>LOCKED</font>'
            out_week = int(time.strftime("%W",time.gmtime(out.date)))
            out_day = int(time.strftime("%j",time.gmtime(out.date)))
            ##only show current week, and the previous.
            if last_week==out_week:
                lines_lastweek.append("<li>on week %s : %s %s</li>"%(
                        time.strftime("%W (%x %X)",time.gmtime(out.date)),
                        ol(out.datasetname),
                        custodial
                        )
                             )
            if this_week==out_week:
                per_day_this_week[out_day]+=1
                lines_thisweek.append("<li>on week %s : %s %s</li>"%(
                        time.strftime("%W (%x %X)",time.gmtime(out.date)),
                        ol(out.datasetname),
                        custodial
                        )
                             )
    lines_thisweek.sort()
    lines_lastweek.sort()

    per_day_s = ", ".join([ "day %s (%d)"%( day, per_day_this_week[day]) for day in sorted(per_day_this_week.keys()) ])

    html_doc.write("""Output produced (%d) <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?in_disagreement=1 target=_blank>disagreements</a>
<a href="javascript:showhide('output')">[Click to show/hide]</a>
<br>
<div id="output" style="display:none;">
<br>
<ul>
<li> %d waiting to go to tape</li>
<ul>
<li> %d waiting for tape approval%s</li>
<li> %d are not completed after %d days%s</li>
<li> Full list (%d) <a href="javascript:showhide('waiting-custodial')">[Click to show/hide]</a>
<div id="waiting-custodial" style="display:none;">
<ul>
%s
</ul>
</div>
</li>
</ul>
<li> Last week (%d) </li><a href="javascript:showhide('output_lastweek')">[Click to show/hide]</a><div id="output_lastweek" style="display:none;"><ul>
%s
</ul></div>
<li> This week (%d) %s </li><a href="javascript:showhide('output_thisweek')">[Click to show/hide]</a><div id="output_thisweek" style="display:none;"><ul>
%s
</ul></div></div>
"""%( len(lines_lastweek)+len(lines_thisweek),
      len(waiting_custodial),
      n_pending_approval,long_approve_string,
      len(lagging_custudial),UC.get('transfer_timeout'),stuck_string,
      len(waiting_custodial),waiting_custodial_string,
      len(lines_lastweek),
     '\n'.join(lines_lastweek),
      len(lines_thisweek), per_day_s, 
     '\n'.join(lines_thisweek))
                   )

    summary_content['last_week'] = len(lines_lastweek)

    lap ( 'done with output' )


    html_doc.write("""Job installed
<a href="javascript:showhide('acron')">[Click to show/hide]</a>
<br>
<div id="acron" style="display:none;">
<br>""")

    ## dump of acrontab
    html_doc.write("""
<pre>
%s

%s
</pre>
"""%(os.getenv('USER'),
     os.popen('acrontab -l | grep -i unified | grep -v \#').read()))


    per_module = defaultdict(list)
    last_module = defaultdict( str )
    ssi = StartStopInfo()
    now = time.mktime(time.localtime())## the date in the log is in local time
    ssi.purge(now, 15 ) ## remove all >15 days old doc
    
    for module_name in all_modules:
        if module_name in ['messagor','cleanor']: continue
        per_module[module_name] = ssi.get(module_name, metric='lap')
        last_module[ module_name] =  ssi.get(module_name, metric='start')
        if last_module[ module_name]:
            last_module[ module_name] = max(last_module[ module_name])
        else:
            print "no mongod record of SS for",module_name
            last_module[ module_name] = None


    html_doc.write("Module running time<br>")
    html_doc.write("<table border=1><thead><tr><th>Module</th><th>Last Ran</th><th>Last Runtime</th><th>Avg Runtime</th></tr></thead>")



    for m in sorted(last_module.keys()):
        last_time = last_module[m]         
        if not last_time: continue
        heart_beat_time_out = 12
        since_last = now-last_time
        if since_last > (heart_beat_time_out*60*60): #6h heart beat
            sendLog('heartbeat',"The module %s has not ran in %s hours, now %s"%(m, heart_beat_time_out, display_time( since_last )), level='critical')
        else:
            print "module %s has ran last since %s"%( m , display_time( since_last ))
        last_module[m] = "%s ago"%( display_time( since_last ) )

    for m in sorted(per_module.keys()):
        #,spends in per_module.items():
        spends = per_module[m]
        if spends:
            avg = sum(spends)/float(len(spends))
            lasttime =  spends[-1]
        else:
            avg = lasttime = 0

        html_doc.write("""
<tr>
 <td width=300>%s</td>
 <td width=300>%s</td>   
 <td width=300>%s</td>   
 <td width=300>%s</td>
</tr>"""%(m, 
          last_module[m],
          display_time(lasttime),
          display_time(avg)
          ))
    html_doc.write("</table>")


    html_doc.write("</div>\n")
    lap ( 'done with jobs' )


    text=""
    count=0
    CI = campaignInfo()
    #for (c,info) in CI.campaigns.items():
    for c in sorted(CI.campaigns.keys()):
        info = CI.campaigns[c]
        #if 'go' in info and info['go']:
        if 'go' in info and info['go']:
            text+="<li><font color=green>%s</font>"%c
        else:
            text+="<li><font color=red>%s</font>"%c
        text += '<img src=https://dmytro.web.cern.ch/dmytro/cmsprodmon/images/%s-history_nevents-limit-30.png style="height:70px">'% (c) 
        text += '<img src=https://dmytro.web.cern.ch/dmytro/cmsprodmon/images/%s-history_requests-limit-30.png style="height:70px">'% (c) 
        text += """
<a href="javascript:showhide('campaign_%s')">[Click to show/hide]</a><br><div id="campaign_%s" style="display:none;">
"""%( c, c )
        text +=  "<br><pre>%s</pre>  </div></li>"%json.dumps( info, indent=2)
        count+=1

    html_doc.write("""Campaign configuration
<a href="javascript:showhide('campaign')">[Click to show/hide]</a>
<br>
<div id="campaign" style="display:none;">
<br>
<ul>
%s
</ul></div>
"""%(text))



    text=""
    count=0
    n_column = 4
    SI = siteInfo()
    date1m = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(30*24*60*60)) )
    date7d = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(7*24*60*60)) )
    date1d = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(24*60*60)) )
    date1h = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(1*60*60)) )
    date5h = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(5*60*60)) )
    now = time.strftime('%Y-%m-%d+%H:%M', time.gmtime())
    upcoming = json.loads( eosRead('%s/GQ.json'%monitor_dir))

    text +='<ul>'
# """
#<ul><li>Sites in use<br><a href="javascript:showhide('site_types')">[Click to show/hide]</a></li>
#<ul>"""
    for_max_running = dataCache.get('gwmsmon_prod_site_summary')
    upcoming_by_site = defaultdict( lambda : defaultdict(int))
    available_ratios = defaultdict(float)
    upcoming_ratios = defaultdict(float)

    for team,agents in getAllAgents(reqmgr_url).items():
        for agent in agents:
            if not 'WMBS_INFO' in agent: continue
            if not 'sitePendCountByPrio' in agent['WMBS_INFO']: continue
            for site in agent['WMBS_INFO']['sitePendCountByPrio']:
                a = sum(agent['WMBS_INFO']['sitePendCountByPrio'][site].values())
                #print site,team,a
                #print a
                #print agent['WMBS_INFO']['sitePendCountByPrio'][site]
                if a: upcoming_by_site[team][site] += a
        

    try:
        sites_full = json.loads(eosRead('%s/sites_full.json'%base_eos_dir))
    except:
        sites_full = []
    for t in ['sites_T0s_all','sites_T1s_all','sites_T2s_all','sites_T3s_all']:
#        text+="""
#<li>%s<a href="javascript:showhide('%s')">[Click to show/hide]</a><br>
#<div id="%s" style="display:none;">
#<table border=1>
#"""%( t, t, t)
        text +='<li>%s<div id="%s"><table border=1>'%( t, t )
        c=0

        for site in sorted(getattr(SI,t)):
            site_se = SI.CE_to_SE(site)
            cpu = SI.cpu_pledges[site] if site in SI.cpu_pledges else 'N/A'
            disk = SI.disk[site_se] if site_se in SI.disk else 'N/A'
            if c==0:
                text+="<tr>"
            if not disk:
                ht_disk = '<a href=remaining_%s.html><font color=red>Disk available: %s</font></a>'%(SI.CE_to_SE(site),disk)
            else:
                ht_disk = 'Disk available: %s'%disk

            up_com = ""
            
            usage = for_max_running[site]['CpusInUse'] if site in for_max_running else 0
            ht_cpu = 'CPU current/max: %s / %s'%(usage,cpu)
            if site_se in upcoming:
                u = sum(upcoming[site_se][camp] for camp in upcoming[site_se])
                up_com += "<br><a href=GQ.json>Jobs available  %d</a> (av./pl.=%.2f)"%(u, (u/float(cpu) if usage else 0)) 
                if usage: available_ratios[site] = u / float(cpu)
                ## should there be an alarm here if a site is underdoing
                if (usage)<(0.8*cpu) and (u > usage):
                    ht_cpu = '<font color=red>%s</font>'%(ht_cpu)

            for team in ['production','relval']:
                if site in upcoming_by_site[team]:
                    if upcoming_by_site[team][site]:
                        den = max(float(cpu),float(usage))
                        if usage and team=='production' and upcoming_by_site[team][site] > den: 
                            upcoming_ratios[site] = upcoming_by_site[team][site] / den
                        up_com +='<br>%s jobs upcoming %d'%(team, upcoming_by_site[team][site])
            text+='<td>'
            if site in SI.sites_ready:
                text+= '<b>%s</b><br>'%site
            elif site in sites_full:
                text+= '<font color=orange><b>%s</b></font><br>'%site
            else:
                text+= '<font color=red><b>%s</b></font><br>'%site                
            #text+='<a href=http://dashb-ssb.cern.ch/dashboard/templates/sitePendingRunningJobs.html?site=%s>%s</a><br>'%(site,site)
            text+='<a href="https://cms-gwmsmon.cern.ch/prodview/%s" target="_blank"><img src="https://cms-gwmsmon.cern.ch/prodview/graphs/%s/daily" style="height:75px"></a>'%( site,site )
            text+='<img src="https://cms-gwmsmon.cern.ch/totalview/graphs/%s/fairshare/daily" style="height:75px"><br>'%(site)
            text+='<img src="https://cms-gwmsmon.cern.ch/prodview/graphs/siteprioritysummarycpuspending/%s/daily" style="height:75px">'%(site)
            text+='<img src="https://cms-gwmsmon.cern.ch/prodview/graphs/siteprioritysummarycpusinuse/%s/daily" style="height:75px"><br>'%(site)

            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#user=&refresh=0&table=Jobs&p=1&records=25&activemenu=1&site=%s&submissiontool=&check=submitted&sortby=activity&scale=linear&bars=20&date1=%s&date2=%s">1m</a>'%( site,date1m,now )
            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#user=&refresh=0&table=Jobs&p=1&records=25&activemenu=1&site=%s&submissiontool=&check=submitted&sortby=activity&scale=linear&bars=20&date1=%s&date2=%s">1w</a>'%( site,date7d,now )
            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#user=&refresh=0&table=Jobs&p=1&records=25&activemenu=1&site=%s&submissiontool=&check=submitted&sortby=activity&scale=linear&bars=20&date1=%s&date2=%s">1d</a>'%( site,date1d,now )
            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#user=&refresh=0&table=Jobs&p=1&records=25&activemenu=1&site=%s&submissiontool=&check=submitted&sortby=activity&scale=linear&bars=20&date1=%s&date2=%s">5h</a>'%( site,date5h,now )
            text+=', <a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#user=&refresh=0&table=Jobs&p=1&records=25&activemenu=1&site=%s&submissiontool=&check=submitted&sortby=activity&scale=linear&bars=20&date1=%s&date2=%s">1h</a>'%( site,date1h,now )
            text+=', <a href="https://cms-site-readiness.web.cern.ch/cms-site-readiness/SiteReadiness/HTML/SiteReadinessReport.html#%s">SAM</a><br>'%( site )
            #text+='<a href=http://dashb-ssb.cern.ch/dashboard/templates/sitePendingRunningJobs.html?site=%s>dashb</a><br>'%(site,site)
            text+='%s<br>'%(ht_cpu)
            text+='%s%s'%(ht_disk,up_com)
            text+='</td>'
            if c==n_column:
                c=0
            else:
                c+=1
        text+="</table></div></li>"


    def outliers( val_d , trunc=0.9, N=4):
        sub_vals = filter( lambda v :v< trunc*max(val_d.values()), val_d.values())
        outl = {}
        if sub_vals:
            avg = sum(sub_vals) / len(sub_vals)
            std = (sum([(x-avg)**2 for x in sub_vals]) / len(sub_vals))**0.5
            upper = (avg+N*std)
            print avg,std,upper
            outl = dict([(k,v) for (k,v) in val_d.items() if v> upper] )
        return outl

    print "These are the possible outliers"
    print available_ratios
    print json.dumps(upcoming_ratios, indent=2)
    outlier_upcoming =  outliers( upcoming_ratios )
    print outlier_upcoming
    if outlier_upcoming:
        sendLog('GQ','There is an inbalance of upcoming work at %s'%(', '.join([site for site in sorted(outlier_upcoming.keys())])),level='critical')
        #open('%s/sites_full.json'%base_eos_dir,'w').write( json.dumps( outlier_upcoming.keys() ))
        oo = eosFile('%s/sites_full.json'%base_eos_dir)
        oo.write( json.dumps( outlier_upcoming.keys() ) )
        oo.close()
        
    def site_div_header(desc):
        div_name = 'site_'+desc.replace(' ','_')
        return """
<li>%s <a href="javascript:showhide('%s')">[Click to show/hide]</a><br>
<div id="%s" style="display:none;">
<ul>
"""%( desc, div_name, div_name)
      
    text += site_div_header("Sites with good IO")
    for site in sorted(SI.sites_with_goodIO):
        text+="<li>%s"% site
    text += "</ul></div></li>"

    text += site_div_header("Sites enabled for multicore pilots")
    for site in sorted(SI.sites_mcore_ready):
        text+="<li>%s"% site
    text += "</ul></div></li>"

    text += site_div_header("Sites in auto-approved transfer")
    for site in sorted(SI.sites_auto_approve):
        text+="<li>%s"% site
    text += "</ul></div></li>"

    text += site_div_header("Sites with vetoe transfer")
    for site in sorted(SI.sites_veto_transfer):
        text+="<li>%s (free : %s)"% (site , SI.disk.get(site, 'N/A'))
    text += "</ul></div></li>"

    text += site_div_header("Sites banned from production")
    for site in sorted(SI.sites_banned):
        text+="<li>%s"% site
    text += "</ul></div></li>"

    text += site_div_header("Sites not ready")
    for site in sorted(SI.sites_not_ready):
        text+='<li> %s <a href="https://cms-site-readiness.web.cern.ch/cms-site-readiness/SiteReadiness/HTML/SiteReadinessReport.html#%s">SAM</a><br>'%( site, site )
    text += "</ul></div></li>"

    text += site_div_header("Sites ready in agents")
    for site in sorted(SI.sites_ready_in_agent):
        text+='<li> %s <a href="https://cms-site-readiness.web.cern.ch/cms-site-readiness/SiteReadiness/HTML/SiteReadinessReport.html#%s">SAM</a><br>'%( site, site )
    text += "</ul></div></li>"


    text += site_div_header("Approximate Free Tape")
    for mss in SI.storage:
        ## this does not work anymore since unified does not check on tape anymore
        #waiting = 0
        #try:
        #    waiting = float(os.popen("grep '%s is pending . Created since' %s/logs/lockor/last.log  -B 3 | grep size | awk '{ sum+=$6 ; print sum }' | tail -1" % (mss,monitor_dir)).readline())
        #except Exception as e:
        #    print str(e)
        #oldest = ""
        #os.system('grep pending %s/logs/lockor/last.log | sort -u > %s/logs/pending.log'%(monitor_dir,monitor_dir))
        #try:
        #    oldest = os.popen("grep '%s is pending . Created since ' %s/logs/lockor/last.log | sort | awk '{print $10, $11, $12, $13, $14 }' | head -1"% (mss,monitor_dir)).readline()
        #except Exception as e:
        #    print str(e)
        #waiting /= 1024.
        #text+="<li>%s : %d [TB]. Waiting for approval %d [TB] since %s </li>"%(mss, SI.storage[mss], waiting, oldest)

        text+="<li>%s : %d [TB]</li>"%(mss, SI.storage[mss])

    text += "</ul></div></li>"


    equalizor = json.loads(eosRead('%s/equalizor.json'%monitor_pub_dir))['reversed_mapping']
    text += site_div_header("Xrootd mapping")
    text += "<li><table border=1><thead><tr><th>Sites</th><th>Can read from</th></tr></thead>\n"
    for site in sorted(equalizor):
        text += '<tr><td align=middle>%s</td><td><ul>'% site
        for src in sorted(equalizor[site]):
            text += '<li>%s</li>'%src
        text += '</ul></td><tr>'
    text += '</table></li></ul></div></li>'
    
    text += '</ul>'

    lap ( 'done with sites' )

    #open('%s/siteInfo.json'%monitor_pub_dir,'w').write(json.dumps(dict([(t,getattr(SI,t)) for t in ['sites_T0s','sites_T1s','sites_T2s','sites_with_goodIO']]),indent=2))
    oo = eosFile('%s/siteInfo.json'%monitor_pub_dir)
    oo.write(json.dumps(dict([(t,getattr(SI,t)) for t in ['sites_T0s','sites_T1s','sites_T2s','sites_with_goodIO']]),indent=2))
    oo.close()

    lap ( 'done with sites json' )

    chart_data = defaultdict(list)
    out_of_space = set()
    for site in SI.quota:
        if not SI.disk[site]: out_of_space.add(site)
        
    for site in SI.quota:
        chart_data[site].append("""
var data_%s = google.visualization.arrayToDataTable([ 
['Overall', 'Space in TB'],
//['Quota' , %s],
['Locked' , %s],
['Free' , %s],
['Buffer', %s],
['Queue', %s]
]);
"""%( site,
      SI.quota[site],
      SI.locked[site],
      SI.disk[site],
      SI.free_disk[site],
      SI.queue[site] if site in SI.queue else 0
      ))
        chart_data[site].append("""
var chart_%s = new google.visualization.PieChart(document.getElementById('donutchart_%s'));
chart_%s.draw(data_%s, {title: '%s %s [TB]', pieHole:0.4, slices:{0:{color:'red'},1:{color:'green'},2:{color:'orange'},3:{color:'blue'}}});
"""%(site,site,
     site,site,
     site,SI.quota[site]))
        chart_data[site].append("""
<div id="donutchart_%s" style="height: 200px;width: 300px"></div>
"""%(site))


        
    ## make the locked/available donut chart
    #donut_html = open('%s/locked.html'%monitor_dir,'w')
    #tight_donut_html = open('%s/outofspace.html'%monitor_dir,'w')
    #remain_reason_html = open('%s/remaining.html'%monitor_dir,'w')
    donut_html = eosFile('%s/locked.html'%monitor_dir)
    tight_donut_html = eosFile('%s/outofspace.html'%monitor_dir)
    remain_reason_html = eosFile('%s/remaining.html'%monitor_dir)

    tables = "\n".join([info[0] for site,info in chart_data.items()])
    draws = "\n".join([info[1] for site,info in chart_data.items()])
    #divs = "\n".join([info[2] for site,info in chart_data.items()])

    oos_tables = "\n".join([info[0] for site,info in chart_data.items() if site in out_of_space])
    oos_draws = "\n".join([info[1] for site,info in chart_data.items() if site in out_of_space])
    #oos_divs = "\n".join([info[2] for site,info in chart_data.items() if site in out_of_space])

    
    divs_table="<table border=0>"
    oos_divs_table="<table border=0>"
    rem_divs_table="<table border=0>"
    i_oos=1
    by_reason_at_site = defaultdict( lambda : defaultdict(float))
    all_reasons = set()
    by_reason_all_sites = defaultdict(float)
    counting_oos = 0
    RDI = remainingDatasetInfo()
    for c,site in enumerate(sorted(chart_data.keys())):
        rem=""
        bgcol = ""
        if site in out_of_space:
            rem = "<br><a href=remaining_%s.html>remaining datasets</a>"% site
            bgcol ='bgcolor="red"'
            counting_oos += 1
            
        if c%5==0:
            divs_table += "<tr>"
        if i_oos%5==0:
            oos_divs_table += "<tr>"
            
        divs_table += "<td %s>%s%s</td>"%(bgcol,chart_data[site][2], rem)
        if site in out_of_space:
            oos_divs_table += "<td>%s%s</td>"%(chart_data[site][2], rem)
            i_oos+=1

            remaining_reasons = RDI.get( site )

            for ds,info in remaining_reasons.items():
                all_reasons.update( info['reasons'] )
                for reason in info['reasons']:
                    if reason == 'lock': continue
                    by_reason_at_site[site][ reason ]+= (info['size'] / 1024.)
                    by_reason_all_sites[ reason ] += (info['size'] / 1024.)


    summary_content['out_of_space'] = counting_oos

    rem_chart_data = defaultdict(list)
    ## now make bar charts DATA per site
    for i_oos,site in enumerate(sorted(by_reason_at_site.keys())):
#        rem_chart_data[site].append("""
#var data_remain_%s = google.visualization.arrayToDataTable([
#['Overall', 'Space in TB'],
#%s
#]);
#"""%( site,
#      ','.join(["['%s' , %s]"%( reason, by_reason_at_site[site][reason]) for reason in sorted(all_reasons)])))

        rem_chart_data[site].append("""
var data_remain_%s = google.visualization.arrayToDataTable([
['Overall', %s],
['%s' , %s]
]);
"""%( site,
      ','.join(['"%s"'%reason for reason in sorted(all_reasons)   ]),
      site,
      ','.join(['%s'%( by_reason_at_site[site][reason]) for reason in sorted(all_reasons)])))
        
        

        rem_chart_data[site].append("""
var remaining_bar_%s = new google.visualization.ColumnChart(document.getElementById('remainbars_%s'));
remaining_bar_%s.draw(data_remain_%s, {title: '%s [TB]'});
"""%(site, site,
     site, site, site))
        rem_chart_data[site].append("""
<div id="remainbars_%s" style="height: 300px;width: 400px"></div>
"""%(site))

    ## now make bar charts per site
    for i_oos,site in enumerate(sorted(by_reason_at_site.keys())):
        rem_link = "<br><a href=remaining_%s.html>remaining datasets</a>"% site if site in out_of_space else ""
        #rem_link = "<br><a href=remaining_%s.html>remaining datasets</a>"% site
        print rem_link
        rem_divs_table += "<td>%s %s</td>"%(rem_chart_data[site][2], rem_link)
        
        if (i_oos+1)%5==0:
            rem_divs_table += "<tr>"
    
    rem_tables = "\n".join([info[0] for site,info in rem_chart_data.items()])
    rem_draws = "\n".join([info[1] for site,info in rem_chart_data.items()])
            
    divs_table += "</table>"
    oos_divs_table += "</table>"
    rem_divs_table += "</table>"

    donut_html.write("""
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
%s

%s
      }
    </script>
  </head>
  <body>
%s
  </body>
</html>
"""%( tables,draws,divs_table   ))

    tight_donut_html.write("""
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
%s

%s
      }
    </script>
  </head>
  <body>
%s
  </body>
</html>
"""%( oos_tables,oos_draws,oos_divs_table   ))
                     
    remain_reason_html.write("""
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
%s

%s
      }
    </script>
  </head>
  <body>
%s
  </body>
</html>
"""%( rem_tables,rem_draws,rem_divs_table   ))

    donut_html.close()
    tight_donut_html.close()
    remain_reason_html.close()


    html_doc.write("""Site configuration
<a href="javascript:showhide('site')">[Click to show/hide]</a>
<br>
<div id="site" style="display:none;">
<br>
%s
</div>
"""%(text))

    lap ( 'done with space' )


    text = ""
    for param in UC.configs:
        text +="<li>%s</li><ul>\n"% param
        for sub in sorted(UC.configs[param].keys()):
            text +="<li> %s : %s </li>\n"%( sub, UC.configs[param][sub] )
        text += '</ul>\n'
        
    html_doc.write("""Unified configuration
<a href="javascript:showhide('config')">[Click to show/hide]</a>
<br>
<div id="config" style="display:none;">
<br>
<ul>
%s
</ul></div>                                                                                                                                                                                                                                                                                                                
"""%(text))

    lap ( 'done with configuration' )



    html_doc.write("""Agent Health
<a href="javascript:showhide('agent')">[Click to show/hide]</a>
<div id="agent" style="display:none;">
<ul>
<li><a href="https://its.cern.ch/jira/issues/?jql=summary~drain%20AND%20project%20=%20CMSCOMPPR%20AND%20status%20!=%20CLOSED">All agents in drain JIRA<a/> </li>
<li><a href="https://its.cern.ch/jira/issues/?jql=summary~ready%20AND%20project%20=%20CMSCOMPPR%20AND%20status%20!=%20CLOSED">All agents ready JIRA<a/> </li>
<li><a href="https://trello.com/b/4np6TByB/production-wmagent-status">Agents status board<a/></li>
</ul>
<br>
<table border=1><thead>
<tr><td>Agent</td><td>Running/Pending hourly (<b>jobs</b>)</td><td>Running/Pending daily (<b>CPUs</b>)</td><td>Status</td><td>Creat./Pend.</td></tr></thead>
""")


    #####
    ### this is more than monitoring
    AI = agentInfo( url = reqmgr_url,
                    verbose=False,
                    busy_fraction = UC.get('busy_agent_fraction'),
                    idle_fraction = UC.get('idle_agent_fraction') ,
                    wake_up_draining=True
                    )
    AI.poll(acting=True)
    JC = JIRAClient()
    now = time.mktime(time.gmtime())
    #####
    #speed_d = json.loads( eosRead('%s/speed_draining.json'%base_eos_dir))
    speed_d = list(agent_speed_draining())
    component_auto_restart = ["ErrorHandler", "JobSubmitter", "CouchServer" ]
    agent_comment_graceperiod = 4 ## ping the JIRA with a comment > N hours

    for team,agents in getAllAgents(reqmgr_url).items():
        if not team in ['production','relval','highprio']: continue
        html_doc.write("<tr><td bgcolor=lightblue>%s</td></tr>"% team)
        for agent in agents:
            bgcolor=''
            name= agent['agent_url'].split(':')[0]
            short_name = name.split('.')[0]
            dash_name = name.replace('.','-')
            if agent['drain_mode'] == True: bgcolor = 'bgcolor=orange'
            if agent['status'] in ['error','down']: 
                ## do you want to send a critical message !
                sendLog('htmlor','Agent %s with %d component down: %s'%( name,
                                                                         len(agent['down_components']),
                                                                         ", ".join(agent['down_components'])), level='critical')
                bgcolor = 'bgcolor=red'
            uas = AI.agentStatus( name )
            if uas == 'standby':
                bgcolor = 'bgcolor=green'

            message = "%s"%name
            for component in agent['down_components']:
                message += '<br><b>%s</b>'%component
                for det in agent['down_component_detail']:
                    if type(det)==dict and det['name'] == component:
                        message += '<br>%s'% det['error_message']
                        this_week = str(int(time.strftime("%W", time.gmtime())))
                        alert_type = agent['status']
                        if component in component_auto_restart: continue
                        if 'thread heartbeat' in det['error_message']:
                            alert_type = 'heartbeat'

                        #alert_summary = '%s %s %s week %s'%( short_name, component, alert_type, this_week)
                        alert_summary = '%s %s %s'%( short_name, component, alert_type)
                        #alert_summary = '%s %s issue'%( short_name, component )
                        jiras = JC.find( {'summary' : alert_summary })
                        if len(jiras)==0:
                            print "creating a JIRA for", alert_summary
                            j = JC.create(
                                {
                                    'summary' : alert_summary,
                                    'description' : det['error_message'],
                                    'label' : 'AgentDoc'
                                })
                        else:
                            ## find the last such, and add a comment after the grace period
                            j = sorted(jiras, key= lambda o:JC.created(o))[-1]
                            reopened = JC.reopen(j.key)
                            ## add a comment to that JIRA : experimental
                            last_comment_time = JC.time_to_time(j.fields.comment.comments[-1].updated) if hasattr(j.fields, 'comment') else JC.created(j)
                            seconds_since = now - last_comment_time
                            ## 4h at least between pings in the agent comment
                            if reopened or (seconds_since > (agent_comment_graceperiod*60*60)):
                                JC.comment(j.key, det['error_message'])
                            else:
                                print "last comment in the JIRA %s was %s ago"%( j.key, display_time(seconds_since))

            message += '<br><a href="https://cms-logbook.cern.ch/elog/GlideInWMS/?mode=summary&reverse=0&reverse=1&npp=20&subtext=%s">gwms elog</a>, <a href="https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=summary&reverse=0&reverse=1&npp=20&subtext=%s">elog</a>, <a href="https://its.cern.ch/jira/issues/?jql=text~%s* AND project = CMSCOMPPR AND status != CLOSED">jira</a>'%( short_name, short_name, short_name )
            message += '<br>Unified status : %s'% uas
            if short_name in speed_d or name in speed_d:
                message += '<br><b>Speed draining by priority increase</b>'

            pend_txt="<ul>"
            by_site = defaultdict(int)
            for site in agent['WMBS_INFO']['sitePendCountByPrio']:
                by_site[site] = sum(agent['WMBS_INFO']['sitePendCountByPrio'][site].values())

            top5 = sorted(by_site.items(), key = lambda v: v[1], reverse=True)[:5]
            for (site,p) in top5:
                pend_txt+="<li> %s : %s "%( site, p)
            pend_txt+="</ul>"
                
            html_doc.write("""
<tr><td %s>%s</td>
<td><img src=https://cms-gwmsmon.cern.ch/poolview/graphs/%s/daily></td>
<td><img src=https://cms-gwmsmon.cern.ch/poolview/graphs/cpus/%s/daily></td>
<td><img src=https://cms-gwmsmon.cern.ch/poolview/graphs/scheddwarning/%s/daily></td>
<td>%s</td>
</tr>
"""%( 
                    bgcolor,
                    message,
                    name.replace('.','-'),
                    name.replace('.','-'),
                    name.replace('.','-'),
                    pend_txt
                    ))
    html_doc.write("</table><br></div>")

    lap( 'done with agents' )


    print "... done with status page."
    html_doc.write("""
</body>
</html>
""")

    html_doc.close()

    SH = statusHistory()
    s_count = defaultdict(int)
    now = time.gmtime()
    for wf in session.query(Workflow).all():
        s_count[wf.status]+=1
    SH.add( now, dict(s_count))
    SH.trim( now, 7 )

    wfs = {}
    for wfo in session.query(Workflow).filter(Workflow.status.contains('manual')).all():
        wfs[wfo.name] = (wfo.status,wfo.wm_status)
    eosFile('%s/statuses.json'%monitor_pub_dir).write(json.dumps( wfs )).close()
    


    this_week = str(int(time.strftime("%W", time.gmtime())))
    last_week = str(int(time.strftime("%W", time.gmtime()))-1)


    eosFile(time.strftime("%s/summary_%%Y_"%(monitor_dir), time.gmtime())+this_week+".json").write(json.dumps(summary_content, indent=2)).close()
    os.system(time.strftime("cp %s/summary_%%Y_"%(monitor_dir), time.gmtime())+this_week+".json %s/summary.txt"%(monitor_pub_dir))
    os.system(time.strftime("cp %s/summary_%%Y_"%(monitor_dir), time.gmtime())+last_week+".json %s/last_summary.txt"%(monitor_pub_dir))


if __name__ == "__main__":
    htmlor()
    CI = closeoutInfo()
    CI.html()

