import json
import sys
import os
import sys

f=sys.argv[1]

try:
    data =json.loads(open('ls.%s.json'%f).read())
    os.system('cp ls.%s.json /afs/cern.ch/user/c/cmst2/www/unified/datalumi/json/.'%f)
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

html = open('/afs/cern.ch/user/c/cmst2/www/unified/datalumi/lumi.%s%s.html'%(f,l),'w')

html.write("""
<html>
<a href=json/%s> json file of content</a><br>
<a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s> request info </a><br>
<table border=1>
<thead>
<tr>
"""%( 'ls.%s.json'%f, f))



all_ls = set()
for dataset,lss in data.items():
    #if not any([dataset.endswith(tier) for tier in ['MINIAOD','AOD','DQMIO']]): continue
    all_ls.update( lss.keys() )

all_ls= list(all_ls)
print len(all_ls)
all_ls.sort( key=lambda i :int(i.split(':')[1]))
all_ls.sort( key=lambda i :int(i.split(':')[0]))

datasets = data.keys()
datasets.sort()

html.write('<td>Lumisection</td>\n')
for dataset in datasets:
    html.write('<td>%s</td>\n'% dataset)
html.write('</tr></thead>\n')

for ls in all_ls:
    html.write('<tr><td>%s</td>\n'%ls)
    for dataset in datasets:
        lss = data[dataset]
        if ls in lss:
            html.write('<td bgcolor=orange> %s </td>\n'%lss[ls])
        else:
            #html.write('<td>&nbsp;</td>\n')
            #html.write('<td></td>\n')
            html.write('<td bgcolor=lightblue></td>\n')
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
