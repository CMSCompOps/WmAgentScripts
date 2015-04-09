from assignSession import *
import time
from utils import getWorkLoad
import os
import json

html_doc = open('/afs/cern.ch/user/v/vlimant/public/ops/index.html','w')

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
            '<a href="https://cmsweb.cern.ch/reqmgr/view/splitting/%s" target="_blank">spl</a>'%wfn,
            '<a href="https://cms-pdmv.cern.ch/stats/?RN=%s" target="_blank">vw</a>'%wfn,
            '<a href="https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=full&reverse=0&reverse=1&npp=20&subtext=%s&sall=q" target="_blank">elog</a>'%pid,
            '<a href="http://hcc-briantest.unl.edu/prodview/%s" target="_blank">pv</a>'%wfn
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


html_doc.write("""
<html>
<body>

Last update on %s(CET), %s(GMT) <br><br>

""" %(time.asctime(time.localtime()),
      time.asctime(time.gmtime())))

text=""
count=0
for wf in session.query(Workflow).filter(Workflow.status=='considered').all():
    text+="<li> %s </li> \n"%wfl(wf,p=True)
    count+=1
text+="</ul>\n"
html_doc.write("Worlfow next to handle (%d)<br><ul>\n"%count)
html_doc.write(text)

text=""
count=0
for wf in session.query(Workflow).filter(Workflow.status=='staging').all():
    text+="<li> %s </li> \n"%wfl(wf)
    count+=1
text+="</ul>\n"
html_doc.write("Worlfow waiting in staging (%d)<br><ul>\n"%count)
html_doc.write(text)

html_doc.write("Transfer on-going (<a href=https://transferteam.web.cern.ch/transferteam/dashboard/ target=_blank> transfer team dashboard</a>)<br><ul>\n")

for ts in session.query(Transfer).all():
    text="<li> %s serves</li> \n<ul>"%phl(ts.phedexid)
    hide = True
    for pid in ts.workflows_id:
        w = session.query(Workflow).get(pid)
        hide &= (w.status in ['staged','away','done','forget'])
        text+="<li> %s : %s</li>\n"%( wfl(w),w.status)
    text+="</ul>\n"
    if hide:
        html_doc.write("<li> %s not needed anymore to start running (does not mean it went through completely)</li>"%phl(ts.phedexid))
    else:
        html_doc.write(text)

html_doc.write("</ul>\n")



text=""
count=0
for wf in session.query(Workflow).filter(Workflow.status=='staged').all():
    text+="<li> %s </li> \n"%wfl(wf,p=True)
    count+=1
text+="</ul>\n"
html_doc.write("Worlfow ready for assigning (%d)<br><ul>\n"%count)
html_doc.write(text)

text=""
count=0
for wf in session.query(Workflow).filter(Workflow.status=='away').all():
    text+="<li> %s </li> \n"%wfl(wf,v=True)
    count+=1
text+="</ul>\n"
html_doc.write("Worlfow on-going (%d) <a href=https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=summary target=_blank>elog</a> <a href=http://hcc-briantest.unl.edu/prodview target=_blank>queues</a><br><ul>\n"%count)
html_doc.write(text)

text=""
count=0
for wf in session.query(Workflow).filter(Workflow.status=='trouble').all():
    text+="<li> %s </li> \n"%wfl(wf)
    count+=1
text+="</ul>\n"
html_doc.write("Worlfow with issue (%d)<br><ul>\n"%count)
html_doc.write(text)

text=""
count=0
for wf in session.query(Workflow).filter(Workflow.status=='forget').all():
    text+="<li> %s </li> \n"%wfl(wf)
    count+=1
text+="</ul>\n"
html_doc.write("Worlfow put behind (%d)<br><ul>\n"%count)
html_doc.write(text)

text=""
count=0
for wf in session.query(Workflow).filter(Workflow.status=='done').all():
    text+="<li> %s </li> \n"%wfl(wf)#,ms=True)
    count+=1
text+="</ul>\n"
html_doc.write("Worlfow through (%d)<br><ul>\n"%count)
html_doc.write(text)

html_doc.write("Output produced<br><ul>\n")
now = time.mktime(time.gmtime())
for out in session.query(Output).all():
    if  out.workflow.status == 'done':
        if (now-out.date) <= (7.*24.*60.*60.):
            html_doc.write("<li>on week %s : %s </li>\n"%(
                    time.strftime("%W (%x %X)",time.gmtime(out.date)),
                    ol(out.datasetname),
                    )
                           )
html_doc.write("</ul>\n")      

html_doc.write("""
</body>
</html>
""")
