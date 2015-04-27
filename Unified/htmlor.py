#!/usr/bin/env python
from assignSession import *
import time
from utils import getWorkLoad, campaignInfo, siteInfo
import os
import json

def htmlor():

    def wfl(wf,view=False,p=False,ms=False,within=False,ongoing=False,status=False):
        wfn = wf.name
        if ongoing:
            wl = getWorkLoad('cmsweb.cern.ch',wfn)
            wf.wm_status = wl['RequestStatus']
            if wf.wm_status in ['failed','aborted','aborted-archived','rejected','rejected-archived']:
                wf.status = 'trouble'
            session.commit()
        wfs = wf.wm_status
        pid = None
        pids=filter(lambda seg: seg.count('-')==2, wf.name.split('_'))
        if len(pids):
            pid=pids[0]
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
                '<a href="http://hcc-briantest.unl.edu/prodview/%s" target="_blank">pv</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/reqmgr/reqMgr/outputDatasetsByRequestName/%s" target="_blank">out</a>'%wfn,
                '<a href="http://jbadillo.web.cern.ch/jbadillo/closeout.html#%s" target="_blank">jclo</a>'%wfn,
                '<a href="closeout.html#%s" target="_blank">clo</a>'%wfn,
                '<a href="statuses.html#%s" target="_blank">st</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s" target="_blank">perf</a>'%wfn
                ])
        if within and (not view or wfs=='completed'):
            wl = getWorkLoad('cmsweb.cern.ch',wfn)
            if 'InputDataset' in wl:
                dataset = wl['InputDataset']
                text+=', <a href=https://cmsweb.cern.ch/das/request?input=%s target=_blank>input</a>'%dataset
                text+=', <a href=https://cmsweb.cern.ch/phedex/prod/Data::Subscriptions#state=create_since=0;filter=%s target=_blank>sub</a>'%dataset

        if p:
            wl = getWorkLoad('cmsweb.cern.ch',wfn)
            text+=', (%s)'%(wl['RequestPriority'])

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
            text+='<a href="http://hcc-briantest.unl.edu/prodview/%s" target="_blank"><img src="http://hcc-briantest.unl.edu/prodview/graphs/%s/daily" style="height:50px"></a>'%(wfn,wfn)
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
<script type="text/javascript">
 function showhide(id) {
    var e = document.getElementById(id);
    e.style.display = (e.style.display == 'block') ? 'none' : 'block';
 }
</script>
</head>
<body>

Last update on %s(CET), %s(GMT), <a href=logs/ target=_blank> logs</a> <br><br>

""" %(time.asctime(time.localtime()),
      time.asctime(time.gmtime())))

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='considered').all():
        text+="<li> %s </li> \n"%wfl(wf,p=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worlfow next to handle <a href=https://cms-pdmv.cern.ch/mcm/batches?status=new&page=-1 target="_blank"> batches</a> (%d) <a href=logs/injector/last.log target=_blank>log</a>
<a href="javascript:showhide('considered')">[Click to show/hide]</a>
<br>
<div id="considered" style="display:none;">
<ul>
"""%count)
    html_doc.write(text)

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='staging').all():
        text+="<li> %s </li> \n"%wfl(wf,within=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worlfow waiting in staging (%d) <a href=logs/transferor/last.log target=_blank> log</a>
<a href="javascript:showhide('staging')">[Click to show/hide]</a>
<br>
<div id="staging" style="display:none;">
<ul>
"""%count)
    html_doc.write(text)

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
Transfer on-going (%d) <a href=https://transferteam.web.cern.ch/transferteam/dashboard/ target=_blank>dashboard</a> <a href=logs/transferor/last.log target=_blank>log</a>
<a href="javascript:showhide('transfer')">[Click to show/hide]</a>
<br>
<div id="transfer" style="display:none;">
<br>
<ul>"""%count)
    html_doc.write(text)



    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='staged').all():
        text+="<li> %s </li> \n"%wfl(wf,p=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""Worlfow ready for assigning (%d) <a href=logs/stagor/last.log target=_blank>log</a>
<a href="javascript:showhide('staged')">[Click to show/hide]</a>
<br>
<div id="staged" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    lines=[]
    for wf in session.query(Workflow).filter(Workflow.status=='away').all():
        lines.append("<li> %s </li>"%wfl(wf,view=True,ongoing=True))
    lines.sort()
    html_doc.write("""
Worlfow on-going (%d) <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests_in_production.php target=_blank>ongoing</a> <a href=https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=summary target=_blank>elog</a> <a href=http://hcc-briantest.unl.edu/prodview target=_blank>queues</a> <a href=logs/assignor/last.log target=_blank>log</a>
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
        text+="<li> %s </li> \n"%wfl(wf,view=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""Worlfow that are closing (%d)
<a href=closeout.html target=_blank>closeout</a> 
<a href=logs/checkor/last.log target=_blank>log</a>
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
        text+="<li> %s </li> \n"%wfl(wf,view=True,within=True,status=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""Worlfow which need assistance (%d)
<a href=assistance.html target=_blank>assistance</a> 
<a href=logs/checkor/last.log target=_blank>log</a>
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
    html_doc.write("""Worlfow ready to close (%d)
<a href=logs/checkor/last.log target=_blank>log</a>
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
    html_doc.write("""Worlfow with issue (%d) <a href=logs/closor/last.log target=_blank>log</a>
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
Worlfow to forget (%d)
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
Worlfow through (%d) <a href=logs/closor/last.log target=_blank>log</a>
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
Worlfow clean for input (%d) <a href=logs/cleanor/last.log target=_blank>log</a>
<a href="javascript:showhide('clean')">[Click to show/hide]</a>
<br>
<div id="clean" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    text=""
    lines=[]
    now = time.mktime(time.gmtime())
    this_week = int(time.strftime("%W",time.gmtime()))
    for out in session.query(Output).all():
        if not out.workflow: 
            print "This is a problem with",out.datasetname
            continue
        if  out.workflow.status == 'done':
            out_week = int(time.strftime("%W",time.gmtime(out.date)))
            ##only show current week, and the previous.
            if (this_week-out_week)<=1:
            #if (now-out.date) <= (10.*24.*60.*60.):
                lines.append("<li>on week %s : %s </li>"%(
                        time.strftime("%W (%x %X)",time.gmtime(out.date)),
                        ol(out.datasetname),
                        )
                             )
    lines.sort()

    html_doc.write("""Output produced <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?in_disagreement=1 target=_blank>disagreements</a> (%d)
<a href="javascript:showhide('output')">[Click to show/hide]</a>
<br>
<div id="output" style="display:none;">
<br>
<ul>
%s
</ul></div>
"""%(len(lines),'\n'.join(lines)))

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
        #text+="<li>%s<ul>"%t
        #for site in getattr(SI,t):
        #    text+="<li><a href=http://hcc-briantest.unl.edu/prodview/%s>%s<a/> </li>"%( site, site)
        #    text+='<a href="http://hcc-briantest.unl.edu/prodview/%s" target="_blank"><img src="http://hcc-briantest.unl.edu/prodview/graphs/%s/daily" style="height:50px"></a>'%(site,site)
        #text+="</ul></li>"
        
        text+="<li>%s<table border=1>"%t
        c=0
        for site in getattr(SI,t):
            if c==0:
                text+="<tr>"
            text+='<td>%s<br><a href="http://hcc-briantest.unl.edu/prodview/%s" target="_blank"><img src="http://hcc-briantest.unl.edu/prodview/graphs/%s/daily" style="height:50px"></a></td>'%(site,site,site)
            if c==n_column:
                c=0
            else:
                c+=1
        text+="</table></li>"

    html_doc.write("""Site configuration
<a href="javascript:showhide('site')">[Click to show/hide]</a>
<br>
<div id="site" style="display:none;">
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
    for wfn in sorted(wfs.keys()):
        html_doc.write('<tr><td><a id="%s">%s</a></td><td>%s</td><td>%s</td></tr>'%( wfn, wfn, wfs[wfn][0],  wfs[wfn][1]))
    html_doc.write("</table></html>")
    html_doc.close()

if __name__ == "__main__":
    htmlor()

