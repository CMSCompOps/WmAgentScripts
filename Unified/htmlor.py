#!/usr/bin/env python
from assignSession import *
import time
from utils import getWorkLoad, campaignInfo, siteInfo, getWorkflows, unifiedConfiguration, getPrepIDs
import os
import json
from collections import defaultdict

def htmlor():
    cache = getWorkflows('cmsweb.cern.ch','assignment-approved', details=True)
    def getWL( wfn ):
        cached = filter(lambda d : d['RequestName']==wfn, cache)
        if cached:
            wl = cached[0]
        else:
            wl = getWorkLoad('cmsweb.cern.ch',wfn)
        return wl

    def wfl(wf,view=False,p=False,ms=False,within=False,ongoing=False,status=False,update=False):
        wfn = wf.name
        wfs = wf.wm_status
        wl = None
        pid = None
        pids=filter(lambda seg: seg.count('-')==2, wf.name.split('_'))
        if len(pids):
            pids = pids[:1]
            pid=pids[0]
            
        if not pids:
            wl = getWL( wf.name )
            pids = getPrepIDs( wl )
            pid = pids[0]

        text=', '.join([
                #wfn,
                '<a href="https://cmsweb.cern.ch/reqmgr/view/details/%s" target="_blank">%s</a>'%(wfn,wfn),
                '(%s) <br>'%wfs])
        text+=', '.join([
                '<a href="https://cmsweb.cern.ch/reqmgr/view/details/%s" target="_blank">dts</a>'%wfn,
                '<a href=https://cmsweb.cern.ch/reqmgr/view/showWorkload?requestName=%s target="_blank">wkl</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/%s" target="_blank">wfc</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/reqmgr/reqMgr/request?requestName=%s" target="_blank">dwkc</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/reqmgr/view/splitting/%s" target="_blank">spl</a>'%wfn,
                '<a href="https://cms-pdmv.cern.ch/stats/?RN=%s" target="_blank">vw</a>'%wfn,
                '<a href="https://cms-pdmv.cern.ch/stats/restapi/get_one/%s" target="_blank">vwo</a>'%wfn,
                '<a href="https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=full&reverse=0&reverse=1&npp=20&subtext=%s&sall=q" target="_blank">elog</a>'%pid,
                '<a href="http://cms-gwmsmon.cern.ch/prodview/%s" target="_blank">pv</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/reqmgr/reqMgr/outputDatasetsByRequestName/%s" target="_blank">out</a>'%wfn,
                '<a href="closeout.html#%s" target="_blank">clo</a>'%wfn,
                '<a href="statuses.html#%s" target="_blank">st</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s" target="_blank">perf</a>'%wfn
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
                text+=', <a href="https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s" target="_blank">ac</a>'%(pid)
                
        if status:
            if wf.status.startswith('assistance'):
                text+=', <a href="assistance.html#%s" target="_blank">assist</a>'%wfn
            text+=' : %s '%(wf.status)

        if view and wfs!='acquired':
            text+='<a href="https://cms-pdmv.web.cern.ch/cms-pdmv/stats/growth/%s.gif" target="_blank"><img src="https://cms-pdmv.web.cern.ch/cms-pdmv/stats/growth/%s.gif" style="height:50px"></a>'%(wfn.replace('_','/'),wfn.replace('_','/'))
        if ongoing:
            text+='<a href="http://cms-gwmsmon.cern.ch/prodview/%s" target="_blank"><img src="http://cms-gwmsmon.cern.ch/prodview/graphs/%s/daily" style="height:50px"></a>'%(wfn,wfn)

        if ongoing:
            date1 = time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(15*24*60*60)) )
            date2 = time.strftime('%Y-%m-%d+%H:%M', time.gmtime())
            text+='<a href="http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/#table=Jobs&date1=%s&date2=%s&sortby=site&task=wmagent_%s">dashb</a>'%( date1, date2, wfn )

        text+="<hr>"
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

    
    ## start to write it
    #html_doc = open('/afs/cern.ch/user/v/vlimant/public/ops/index.html','w')
    html_doc = open('/afs/cern.ch/user/c/cmst2/www/unified/index.html','w')
    print "Updating the status page ..." 
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

Last update on %s(CET), %s(GMT), <a href=logs/ target=_blank>logs</a> <a href=logs/last.log target=_blank>last</a> <a href=statuses.html>statuses</a> <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/ target=_blank>prod mon</a> <a href=https://cmsweb.cern.ch/wmstats/index.html target=_blank>wmstats</a> <a href=http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt target=_blank>detox</a> <a href=locked.html>space</a> <a href=https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsWorkflowL3Responsibilities#Automatic_Assignment_and_Unified>what am I</a> <a href=logs/addHoc/last.log>add-hoc op</a> <br><br>

""" %(time.asctime(time.localtime()),
      time.asctime(time.gmtime())))

    text=""
    count=0
    count_by_campaign=defaultdict(lambda : defaultdict(int))
    for wf in session.query(Workflow).filter(Workflow.status=='considered').all():
        wl = getWL( wf.name )
        count_by_campaign[wl['Campaign']][int(wl['RequestPriority'])]+=1
        text+="<li> %s </li> \n"%wfl(wf,p=True)
        count+=1
    text_by_c=""
    for c in count_by_campaign:
        text_by_c+="<li> %s (%d) : "%( c, sum(count_by_campaign[c].values()) )
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
                   

    text=""
    count=0
    count_by_campaign=defaultdict(lambda : defaultdict(int))
    for wf in session.query(Workflow).filter(Workflow.status=='staging').all():
        wl = getWL( wf.name )
        count_by_campaign[wl['Campaign']][int(wl['RequestPriority'])]+=1
        text+="<li> %s </li> \n"%wfl(wf,within=True)
        count+=1

    text_by_c=""
    for c in count_by_campaign:
        text_by_c+="<li> %s (%d) : "%( c, sum(count_by_campaign[c].values()) )
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


    text=""
    count=0
    for ts in session.query(Transfer).all():
        stext='<li> %s serves </li><a href="javascript:showhide(\'%s\')">[show/hide]</a> <div id="%s" style="display:none;"><ul>'%( phl(ts.phedexid), ts.phedexid, ts.phedexid )
        hide = True
        for pid in ts.workflows_id:
            w = session.query(Workflow).get(pid)
            hide &= (w.status != 'staging' )
            stext+="<li> %s </li>\n"%( wfl(w,status=True))
        stext+="</ul></div>\n"
        if hide:
            #text+="<li> %s not needed anymore to start running (does not mean it went through completely)</li>"%phl(ts.phedexid)
            pass
        else:
            count+=1
            text+=stext
    text+="</ul></div>"
    html_doc.write("""
Transfer on-going (%d) <a href=https://transferteam.web.cern.ch/transferteam/dashboard/ target=_blank>dashboard</a> <a href=logs/transferor/last.log target=_blank>log</a> <a href=logs/stagor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('transfer')">[Click to show/hide]</a>
<br>
<div id="transfer" style="display:none;">
<br>
<ul>"""%count)
    html_doc.write(text)



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
        text_by_c+="<li> %s (%d) : "%( c, sum(count_by_campaign[c].values()) )
        for p in sorted(count_by_campaign[c].keys()):
            text_by_c+="%d (%d), "%(p,count_by_campaign[c][p])
        text_by_c+="</li>"

    html_doc.write("""Worflow ready for assigning (%d) <a href=logs/stagor/last.log target=_blank>log</a> <a href=logs/assignor/last.log target=_blank>postlog</a>
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


    lines=[]
    for wf in session.query(Workflow).filter(Workflow.status=='away').all():
        lines.append("<li> %s </li>"%wfl(wf,view=True,ongoing=True))
    lines.sort()
    html_doc.write("""
Worflow on-going (%d) <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests_in_production.php target=_blank>ongoing</a> <a href=https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=summary target=_blank>elog</a> <a href=http://cms-gwmsmon.cern.ch/prodview target=_blank>queues</a> <a href=logs/assignor/last.log target=_blank>log</a> <a href=logs/checkor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('away')">[Click to show/hide]</a>
<br>
<div id="away" style="display:none;">
<br>
<ul>
%s
</ul>
</div>
"""%(len(lines),'\n'.join(lines)))

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status == 'assistance').all():
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

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status.startswith('assistance-')).all():
        text+="<li> %s </li> \n"%wfl(wf,view=True,within=True,status=True,update=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""Worflow which need assistance (%d)
<a href=assistance.html target=_blank>assistance</a> 
<a href=logs/checkor/last.log target=_blank>log</a> <a href=logs/recoveror/last.log target=_blank>postlog</a>
<a href="javascript:showhide('assistance')">[Click to show/hide]</a>
<br>
<div id="assistance" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)
    
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



    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='forget').all():
        text+="<li> %s </li> \n"%wfl(wf)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worflow to forget (%d) <a href=logs/injector/last.log target=_blank>log</a> <a href=logs/outcleanor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('forget')">[Click to show/hide]</a>
<br>
<div id="forget" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='done').all():
        text+="<li> %s </li> \n"%wfl(wf)#,ms=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worflow through (%d) <a href=logs/closor/last.log target=_blank>log</a> <a href=logs/cleanor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('done')">[Click to show/hide]</a>
<br>
<div id="done" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='clean').all():
        text+="<li> %s </li> \n"%wfl(wf)#,ms=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worflow clean for input (%d) <a href=logs/cleanor/last.log target=_blank>log</a> <a href=logs/outcleanor/last.log target=_blank>postlog</a>
<a href="javascript:showhide('clean')">[Click to show/hide]</a>
<br>
<div id="clean" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)


    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status.endswith('-out')).all():
        text+="<li> %s </li> \n"%wfl(wf,status=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worflow clean for output (%d) <a href=logs/outcleanor/last.log target=_blank>log</a>
<a href="javascript:showhide('cleanout')">[Click to show/hide]</a>
<br>
<div id="cleanout" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)






    text=""
    lines_thisweek=[]
    lines_lastweek=[]
    now = time.mktime(time.gmtime())
    this_week = int(time.strftime("%W",time.gmtime()))
    for out in session.query(Output).all():
        if not out.workflow: 
            print "This is a problem with",out.datasetname
            continue
        if  out.workflow.status in ['done','clean','clean-out']:
            out_week = int(time.strftime("%W",time.gmtime(out.date)))
            ##only show current week, and the previous.
            if (this_week-out_week)==1:
                lines_lastweek.append("<li>on week %s : %s </li>"%(
                        time.strftime("%W (%x %X)",time.gmtime(out.date)),
                        ol(out.datasetname),
                        )
                             )
            if (this_week-out_week)==0:
                lines_thisweek.append("<li>on week %s : %s </li>"%(
                        time.strftime("%W (%x %X)",time.gmtime(out.date)),
                        ol(out.datasetname),
                        )
                             )
    lines_thisweek.sort()
    lines_lastweek.sort()

    html_doc.write("""Output produced (%d) <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?in_disagreement=1 target=_blank>disagreements</a>
<a href="javascript:showhide('output')">[Click to show/hide]</a>
<br>
<div id="output" style="display:none;">
<br>
<ul>
<li> Last week (%d) </li><a href="javascript:showhide('output_lastweek')">[Click to show/hide]</a><div id="output_lastweek" style="display:none;"><ul>
%s
</ul></div>
<li> This week (%d) </li><a href="javascript:showhide('output_thisweek')">[Click to show/hide]</a><div id="output_thisweek" style="display:none;"><ul>
%s
</ul></div></div>
"""%( len(lines_lastweek)+len(lines_thisweek),
      len(lines_lastweek),
     '\n'.join(lines_lastweek),
      len(lines_thisweek),
     '\n'.join(lines_thisweek))
                   )

    html_doc.write("""Job installed
<a href="javascript:showhide('acron')">[Click to show/hide]</a>
<br>
<div id="acron" style="display:none;">
<br>
<pre>
%s
</pre></div>
"""%(os.popen('acrontab -l | grep Unified').read()))

    text=""
    count=0
    for (c,info) in campaignInfo().campaigns.items():
        #if 'go' in info and info['go']:
        text+="<li>%s <br> <pre>%s</pre>  </li>"%( c, json.dumps( info, indent=2))
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
    for t in SI.types():
        text+="<li>%s<table border=1>"%t
        c=0
        for site in getattr(SI,t):
            cpu = SI.cpu_pledges[site] if site in SI.cpu_pledges else 'N/A'
            disk = SI.disk[SI.CE_to_SE(site)] if SI.CE_to_SE(site) in SI.disk else 'N/A'
            if c==0:
                text+="<tr>"
            text+='<td><a href=http://dashb-ssb.cern.ch/dashboard/templates/sitePendingRunningJobs.html?site=%s>%s</a><br><a href="http://cms-gwmsmon.cern.ch/prodview/%s" target="_blank"><img src="http://cms-gwmsmon.cern.ch/prodview/graphs/%s/daily" style="height:50px"></a><br>CPU pledge: %s<br>Disk available: %s</td>'%(site,site,site,site,cpu,disk)
            if c==n_column:
                c=0
            else:
                c+=1
        text+="</table></li>"

    open('/afs/cern.ch/user/c/cmst2/www/unified/siteInfo.json','w').write(json.dumps(dict([(t,getattr(SI,t)) for t in SI.types()]),indent=2))

    chart_data = defaultdict(list)
    for site in SI.quota:
        chart_data[site].append("""
var data_%s = google.visualization.arrayToDataTable([ 
['Overall', 'Space in TB'],
//['Quota' , %s],
['Locked' , %s],
['Free' , %s]
]);
"""%( site,
      SI.quota[site], SI.locked[site], SI.disk[site],
      ))
        chart_data[site].append("""
var chart_%s = new google.visualization.PieChart(document.getElementById('donutchart_%s'));
chart_%s.draw(data_%s, {title: '%s', pieHole:0.4, slices:{0:{color:'red'},1:{color:'green'}}});
"""%(site,site,site,site,site))
        chart_data[site].append("""
<div id="donutchart_%s" style="height: 200px;"></div>
"""%(site))
        
    ## make the locked/available donut chart
    donut_html = open('/afs/cern.ch/user/c/cmst2/www/unified/locked.html','w')
    tables = "\n".join([info[0] for site,info in chart_data.items()])
    draws = "\n".join([info[1] for site,info in chart_data.items()])
    divs = "\n".join([info[2] for site,info in chart_data.items()])

    
    divs_table="<table border=0>"
    for c,site in enumerate(sorted(chart_data.keys())):
        if c%6==0:
            divs_table += "<tr>"
        divs_table += "<td>%s</td>"%(chart_data[site][2])
    divs_table += "</table>"

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
"""%( tables,draws,divs_table   )
                     )
    donut_html.close()

    html_doc.write("""Site configuration
<a href="javascript:showhide('site')">[Click to show/hide]</a>
<br>
<div id="site" style="display:none;">
<br>
<ul>
%s
</ul></div>
"""%(text))


    UC = unifiedConfiguration()
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




    print "... done with status page."
    html_doc.write("""
</body>
</html>
""")

    html_doc.close()

    html_doc = open('/afs/cern.ch/user/c/cmst2/www/unified/statuses.html','w')
    html_doc.write("""                                                                                                                                                                                                                                                                                                      <html>        
<table border=1>
<thead>
<tr>
<th> workflow </th><th> status </th><th> wm status</th>
</tr>
</thead>
""")
    wfs = {}
    for wfo in session.query(Workflow).all():
        wfs[wfo.name] = (wfo.status,wfo.wm_status)
    open('/afs/cern.ch/user/c/cmst2/www/unified/statuses.json','w').write(json.dumps( wfs ))
    for wfn in sorted(wfs.keys()):
        html_doc.write('<tr><td><a id="%s">%s</a></td><td>%s</td><td>%s</td></tr>\n'%( wfn, wfn, wfs[wfn][0],  wfs[wfn][1]))
    html_doc.write("</table>")
    html_doc.write("<br>"*100)
    html_doc.write("end of page</html>")
    html_doc.close()

if __name__ == "__main__":
    htmlor()

