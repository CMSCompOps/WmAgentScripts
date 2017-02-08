import json
import sys
import os
import sys
import time
from utils import monitor_dir

pid=sys.argv[1]

try:
    data =json.loads(open('%s/datalumi/json/ls.%s.json'%(monitor_dir,pid)).read())
except:
    
    sys.exit(0)

if not data:
    sys.exit(0)

l=''
all_empty=True
for ds in data:
    if data[ds]:
        all_empty=False
        continue

#if all_empty: l='.COMPLETE'

all_ls = set()
for dataset,lss in data.items():
    #if not any([dataset.endswith(tier) for tier in ['MINIAOD','AOD','DQMIO']]): continue
    all_ls.update( lss.keys() )

all_ls= list(all_ls)
print len(all_ls)
print "\t\tmaking HTML"
html = open('%s/datalumi/lumi.%s%s.html'%(monitor_dir,pid,l),'w')

html.write("""
<html>
Updated on %s GMT<br>
Missing <b>%d lumisection</b> summary for <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s>%s</a>, <a href=json/%s> json file of content</a><br>
<table border=1>
<thead>
<tr>
"""%( time.asctime(time.gmtime()), len(all_ls) ,pid, pid, 'ls.%s.json'%pid))




all_ls.sort( key=lambda i :int(i.split(':')[1]))
all_ls.sort( key=lambda i :int(i.split(':')[0]))

datasets = data.keys()
datasets.sort()

html.write('<td>Lumisection</td>\n')
for dataset in datasets:
    html.write('<td>%s</td>\n'% dataset)
html.write('</tr></thead>\n')

html.write('<tr><td> Missing </td>')
for dataset in datasets:
    html.write('<td> total:<b>%d</b> </td>'%len( data[dataset] ))
html.write('</tr>')
cr=0
run_switch=True
for ls in all_ls:
    r=int(ls.split(':')[0])
    l=False
    if cr != r:
        cr = r
        run_switch = not run_switch
        l=True

    #bdr='style="border-top:4px solid black;"' if l else ''
    bdr=''
    green='green' if run_switch else 'lightgreen'
    orange='red' if run_switch else 'pink'
    #html.write('<tr><td %s>%s</td>\n'%('bgcolor=lightblue' if run_switch else '', ls))
    html.write('<tr><td %s %s>%s</td>\n'%(bdr, 'bgcolor=lightblue' if run_switch else '', ls))
    for dataset in datasets:
        lss = data[dataset]
        if ls in lss:
            html.write('<td %s bgcolor=%s> %s </td>\n'%(bdr,orange,lss[ls]))
        else:
            #html.write('<td>&nbsp;</td>\n')
            #html.write('<td></td>\n')
            html.write('<td %s bgcolor=%s></td>\n'%(bdr,green))
    html.write('</tr>\n')

html.write('</table></html>')
html.close()

sys.exit(0)





html.write('<td>Dataset</td>\n')
for ls in all_ls:
    html.write('<td> %s </td>\n'%( ls ))
html.write('</tr></thead>\n')



for dataset in datasets:
    lss = data[dataset]
    #if not any([dataset.endswith(tier) for tier in ['MINIAOD','AOD','DQMIO']]): continue
    html.write('<tr><td>%s</td>\n'% dataset)
    for ls in all_ls:
        if ls in lss:
            html.write('<td> %s </td>\n'%lss[ls])
        else:
            html.write('<td> </td>\n')
    html.write('</tr>\n')

html.write('</table></html>')
html.close()
