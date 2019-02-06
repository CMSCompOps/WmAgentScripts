from assignSession import *
import os
import json
#import numpy as np
from utils import siteInfo, getWorkflowByInput, getWorkflowByOutput, getWorkflowByMCPileup, monitor_dir, monitor_pub_dir, eosRead, eosFile, remainingDatasetInfo
import sys
import time
import random 
from collections import defaultdict

url = 'cmsweb.cern.ch'


print time.asctime(time.gmtime())
RDI = remainingDatasetInfo()

if sys.argv[1] == 'parse':
    force = False
    if len(sys.argv)>2:
        force = bool(sys.argv[2])
    locks = [l.item.split('#')[0] for l in session.query(Lock).filter(Lock.lock == True).all()]

    waiting = {}
    stuck = {}
    missing = {} 
    si = siteInfo()
    remainings={}
    sis = si.disk.keys()
    random.shuffle( sis )
    for site in sis:
        space = si.disk[site]
        if space: 
            continue
        print site,"has no disk space left"

        remainings[site]={}

        print site,"has",space,"[TB] left out of",si.quota[site]
        ds = si.getRemainingDatasets(si.CE_to_SE(site))
        #print len(ds)
        taken_size=0.
        sum_waiting=0.
        sum_stuck=0.
        sum_missing=0.
        sum_unlocked=0.
        for (size,dataset) in ds:
            remainings[site][dataset] = {"size" : size, "reasons": []}
            #print "-"*10
            if not dataset in locks:
                #print dataset,"is not locked"
                sum_unlocked += size
                remainings[site][dataset]["reasons"].append('unlock')
            else:
                remainings[site][dataset]["reasons"].append('lock')
            if dataset in waiting:
                #print dataset,"is waiting for custodial"
                sum_waiting+=size
                remainings[site][dataset]["reasons"].append('tape')

            if dataset in stuck:
                sum_stuck+=size
                remainings[site][dataset]["reasons"].append('stuck-tape')
            if dataset in missing:
                sum_missing +=size
                remainings[site][dataset]["reasons"].append('missing-tape')

            statuses = ['assignment-approved','acquired','running-open','running-closed','completed','closed-out']
            actors = getWorkflowByInput( url, dataset , details=True)
            #actors = [wfc['doc'] for wfc in by_input if wfc['key']==dataset]
            using_actors = [actor for actor in actors if actor['RequestStatus'] in statuses]
            if len(using_actors):remainings[site][dataset]["reasons"].append('input')
            actors = getWorkflowByOutput( url, dataset , details=True)
            #actors = [wfc['doc'] for wfc in by_output if wfc['key']==dataset]
            using_actors = [actor for actor in actors if actor['RequestStatus'] in statuses]
            if len(using_actors):remainings[site][dataset]["reasons"].append('output')
            actors = getWorkflowByMCPileup( url, dataset , details=True)
            #actors = [wfc['doc'] for wfc in by_pileup if wfc['key']==dataset]
            using_actors = [actor for actor in actors if actor['RequestStatus'] in statuses]
            if len(using_actors):remainings[site][dataset]["reasons"].append('pilup')
        
            print dataset,remainings[site][dataset]["reasons"]

        #print "\t",sum_waiting,"[GB] could be freed by custodial"
        #print "\t",sum_unlocked,"[GB] is not locked by me"

        RDI.set(site, remainings[site])
        try:
            eosFile('%s/remaining_%s.json'%(monitor_dir,site),'w').write( json.dumps( remainings[site] , indent=2)).close()
        except:
            pass

        ld = remainings[site].items()
        ld.sort( key = lambda i:i[1]['size'], reverse=True)
        table = "<html>Updated %s GMT, <a href=remaining_%s.json>json data</a><br>"%(time.asctime(time.gmtime()),site)

        accumulate = defaultdict(lambda : defaultdict(float))
        for item in remainings[site]:
            tier = item.split('/')[-1]

            for reason in remainings[site][item]['reasons']:
                accumulate[reason][tier] += remainings[site][item]['size']
        table += "<table border=1></thead><tr><th>Reason</th><th>size [TB]</th></thead>"
        for reason in accumulate:
            s=0
            table += "<tr><td>%s</td><td><ul>"% reason
            subitems = accumulate[reason].items()
            subitems.sort(key = lambda i:i[1], reverse=True)

            for tier,ss in subitems:
                table += "<li> %s : %10.3f</li>"%( tier, ss/1024.)
                s+=  ss/1024.
            table+="</ul>total : %.3f</td>"%s

        table += "</table>\n"
        table += "<table border=1></thead><tr><th>Dataset</th><th>Size [GB]</th><th>Label</th></tr></thead>\n"
        for item in ld:
            table+="<tr><td>%s</td><td>%d</td><td><ul>%s</ul></td></tr>\n"%( item[0], item[1]['size'], "<li>".join([""]+item[1]['reasons']))
        table+="</table></html>"
        eosFile('%s/remaining_%s.html'%(monitor_dir,site),'w').write( table ).close()

    #eosFile('%s/remaining.json'%monitor_dir,'w').write( json.dumps( remainings , indent=2)).close()

else:
    si = siteInfo()
    remainings={}
    for site in RDI.sites():
        load = RDI.get(site)
        if si.disk[site] : continue
        print site,si.disk[site],"[TB] free",si.quota[site],"[TB] quota"

        if not load: continue
        tags = ['pilup','input','output','lock','unlock','tape','stuck-tape','missing-tape']
        for tag in tags:
            v = sum([ info['size'] for ds,info in load.items() if tag in info['reasons']]) / 1024.
            print "\t %10f [TB] remaining because of %s"%(v,tag)
    #open('%s/remaining.json'%monitor_dir,'w').write( json.dumps( remainings , indent=2))
    #eosFile('%s/remaining.json'%monitor_dir,'w').write( json.dumps( remainings , indent=2)).close()

