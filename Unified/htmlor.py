from assignSession import *
import time
from utils import getWorkLoad, campaignInfo
import os
import json

def htmlor():

    def wfl(wf,v=False,p=False,ms=False):
        wfn = wf.name
        wfs = wf.wm_status
        pid = None
        pids=filter(lambda seg: seg.count('-')==2, wf.name.split('_'))
        if len(pids):
            pid=pids[0]
        text=', '.join([
                #wfn,
                '<a href="https://cmsweb.cern.ch/reqmgr/view/details/%s" target="_blank">%s</a>'%(wfn,wfn),
                '(%s)'%wfs,
                '<a href="https://cmsweb.cern.ch/reqmgr/view/details/%s" target="_blank">dts</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/reqmgr/reqMgr/request?requestName=%s" target="_blank">wkl</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/%s" target="_blank">wfc</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/reqmgr/view/splitting/%s" target="_blank">spl</a>'%wfn,
                '<a href="https://cms-pdmv.cern.ch/stats/?RN=%s" target="_blank">vw</a>'%wfn,
                '<a href="https://cms-pdmv.cern.ch/stats/restapi/get_one/%s" target="_blank">vwo</a>'%wfn,
                '<a href="https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=full&reverse=0&reverse=1&npp=20&subtext=%s&sall=q" target="_blank">elog</a>'%pid,
                '<a href="http://hcc-briantest.unl.edu/prodview/%s" target="_blank">pv</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/reqmgr/reqMgr/outputDatasetsByRequestName/%s" target="_blank">out</a>'%wfn,
                '<a href="http://jbadillo.web.cern.ch/jbadillo/closeout.html#%s" target="_blank">clo</a>'%wfn,
                '<a href="https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s" target="_blank">perf</a>'%wfn
                ])
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
                

        if v and wfs!='acquired':
            text+='<a href="https://cms-pdmv.web.cern.ch/cms-pdmv/stats/growth/%s.gif" target="_blank"><img src="https://cms-pdmv.web.cern.ch/cms-pdmv/stats/growth/%s.gif" style="height:50px"></a>'%(wfn.replace('_','/'),wfn.replace('_','/'))
            text+='<a href="http://hcc-briantest.unl.edu/prodview/%s" target="_blank"><img src="http://hcc-briantest.unl.edu/prodview/graphs/%s/daily" style="height:50px"></a>'%(wfn,wfn)
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

Last update on %s(CET), %s(GMT) <br><br>

""" %(time.asctime(time.localtime()),
      time.asctime(time.gmtime())))

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='considered').all():
        text+="<li> %s </li> \n"%wfl(wf,p=True)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worlfow next to handle <a href=https://cms-pdmv.cern.ch/mcm/batches?status=new&page=-1 target="_blank"> batches</a> (%d) 
<a href="javascript:showhide('considered')">[Click to show/hide]</a>
<br>
<div id="considered" style="display:none;">
<ul>
"""%count)
    html_doc.write(text)

    text=""
    count=0
    for wf in session.query(Workflow).filter(Workflow.status=='staging').all():
        text+="<li> %s </li> \n"%wfl(wf)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""
Worlfow waiting in staging (%d)
<a href="javascript:showhide('staging')">[Click to show/hide]</a>
<br>
<div id="staging" style="display:none;">
<ul>
"""%count)
    html_doc.write(text)

    text=""
    count=0
    for ts in session.query(Transfer).all():
        stext="<li> %s serves</li> \n<ul>"%phl(ts.phedexid)
        hide = True
        for pid in ts.workflows_id:
            w = session.query(Workflow).get(pid)
            hide &= (w.status in ['staged','away','done','forget'])
            stext+="<li> %s : %s</li>\n"%( wfl(w),w.status)
        stext+="</ul>\n"
        if hide:
            #text+="<li> %s not needed anymore to start running (does not mean it went through completely)</li>"%phl(ts.phedexid)
            pass
        else:
            count+=1
            text+=stext
    text+="</ul></div>"
    html_doc.write("""
Transfer on-going (%d) <a href=https://transferteam.web.cern.ch/transferteam/dashboard/ target=_blank>dashboard</a>
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
    html_doc.write("""Worlfow ready for assigning (%d)
<a href="javascript:showhide('staged')">[Click to show/hide]</a>
<br>
<div id="staged" style="display:none;">
<br>
<ul>
"""%count)
    html_doc.write(text)

    lines=[]
    for wf in session.query(Workflow).filter(Workflow.status=='away').all():
        lines.append("<li> %s </li>"%wfl(wf,v=True))
    lines.sort()
    html_doc.write("""
Worlfow on-going (%d) <a href=https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=summary target=_blank>elog</a> <a href=http://hcc-briantest.unl.edu/prodview target=_blank>queues</a>
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
    for wf in session.query(Workflow).filter(Workflow.status=='trouble').all():
        text+="<li> %s </li> \n"%wfl(wf)
        count+=1
    text+="</ul></div>\n"
    html_doc.write("""Worlfow with issue (%d)
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
Worlfow through (%d)
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
Worlfow clean for input (%d)
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

    html_doc.write("""Output produced <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/requests.php?in_disagreement=1>disagreements</a> (%d)
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
        if 'go' in info and info['go']:
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

    print "... done with status page."
    html_doc.write("""
</body>
</html>
""")

if __name__ == "__main__":
    htmlor()

