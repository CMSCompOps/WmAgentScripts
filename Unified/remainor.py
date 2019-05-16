#!/usr/bin/env python
from assignSession import *
import os
import json
#import numpy as np
from utils import siteInfo, getWorkflowByInput, getWorkflowByOutput, getWorkflowByMCPileup, monitor_dir, monitor_pub_dir, eosRead, eosFile, remainingDatasetInfo, moduleLock, allCompleteToAnaOps, getDatasetStatus, setDatasetStatus, unifiedConfiguration, ThreadHandler
import sys
import time
import random 
from collections import defaultdict
import threading
import optparse
import copy

class DatasetCheckBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.daemon = True
        for k,v in args.items():
            setattr(self,k,v)

    def run(self):
        url = self.url
        dataset= self.dataset
        reasons = []
        statuses = ['assignment-approved','assigned','acquired','running-open','running-closed','completed','force-complete','closed-out']

        ##print "reqmgr check on ",dataset
        actors = getWorkflowByInput( url, dataset , details=True)
        using_actors = [actor for actor in actors if actor['RequestStatus'] in statuses]
        if len(using_actors):
            reasons.append('input')

        actors = getWorkflowByOutput( url, dataset , details=True)
        using_actors = [actor for actor in actors if actor['RequestStatus'] in statuses]
        if len(using_actors):
            reasons.append('output')

        actors = getWorkflowByMCPileup( url, dataset , details=True)
        using_actors = [actor for actor in actors if actor['RequestStatus'] in statuses]
        if len(using_actors):
            reasons.append('pilup')
        
        self.reasons = reasons


class SiteBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.daemon = True
        for k,v in args.items():
            setattr(self,k,v)
        
    def run(self):
        site = self.site
        print "checking on site",site
        si = self.SI
        UC = self.UC
        RDI = self.RDI
        options = self.options
        locks = self.locks
        waiting = self.waiting
        stuck = self.stuck
        missing = self.missing
        remainings = {}
        
        ds = si.getRemainingDatasets(si.CE_to_SE(site))
        #print len(ds)
        taken_size=0.
        sum_waiting=0.
        sum_stuck=0.
        sum_missing=0.
        sum_unlocked=0.
        n_ds = options.ndatasets
        i_ds = 0
        ds_threads = []
        for i_ds,(size,dataset) in enumerate(ds):
            if n_ds and i_ds>=n_ds: break
            remainings[dataset] = {"size" : size, "reasons": []}
            #print "-"*10
            if not dataset in locks:
                #print dataset,"is not locked"
                sum_unlocked += size
                remainings[dataset]["reasons"].append('unlock')
            else:
                remainings[dataset]["reasons"].append('lock')
            if dataset in waiting:
                #print dataset,"is waiting for custodial"
                sum_waiting+=size
                remainings[dataset]["reasons"].append('tape')

            if dataset in stuck:
                sum_stuck+=size
                remainings[dataset]["reasons"].append('stuck-tape')
            if dataset in missing:
                sum_missing +=size
                remainings[dataset]["reasons"].append('missing-tape')

            ds_threads.append( DatasetCheckBuster( dataset = dataset,
                                                   url = url))

        
        run_threads = ThreadHandler( threads = ds_threads,
                                     label = '%s Dataset Threads'%site,
                                     n_threads = 10 ,
                                     start_wait = 0,
                                     timeout = None,
                                     verbose=True)
        ## start and sync
        run_threads.run()
        #run_threads.start()
        #while run_threads.is_alive():
        #    time.sleep(10)        

        for t in run_threads.threads:
            remainings[t.dataset]["reasons"].extend( t.reasons )
            remainings[t.dataset]["reasons"].sort()
            print t.dataset,remainings[t.dataset]["reasons"]

        #print "\t",sum_waiting,"[GB] could be freed by custodial"
        print "\t",sum_unlocked,"[GB] is not locked by unified"

        print "updating database with remaining datasets"
        RDI.set(site, remainings)
        try:
            eosFile('%s/remaining_%s.json'%(monitor_dir,site),'w').write( json.dumps( remainings , indent=2)).close()
        except:
            pass

        ld = remainings.items()
        ld.sort( key = lambda i:i[1]['size'], reverse=True)
        table = "<html>Updated %s GMT, <a href=remaining_%s.json>json data</a><br>"%(time.asctime(time.gmtime()),site)

        accumulate = defaultdict(lambda : defaultdict(float))
        for item in remainings:
            tier = item.split('/')[-1]

            for reason in remainings[item]['reasons']:
                accumulate[reason][tier] += remainings[item]['size']
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
        only_unlock = set()
        for item in ld:
            ds_name = item[0]
            reasons = item[1]['reasons']
            sub_url = '<a href="https://cmsweb.cern.ch/das/request?input=%s">%s</a>'%(ds_name, ds_name)
            if 'unlock' in reasons:
                sub_url += ', <a href="https://cmsweb.cern.ch/phedex/datasvc/xml/prod/subscriptions?block=%s%%23*&node=%s">block</a>'%(ds_name, site)
            if 'unlock' in reasons or 'input' in reasons:
                sub_url += ', <a href="https://cmsweb.cern.ch/reqmgr2/data/request?inputdataset=%s&mask=RequestName&mask=RequestStatus">input</a>'%(ds_name)
            if 'unlock' in reasons or 'output' in reasons:
                sub_url += ', <a href="https://cmsweb.cern.ch/reqmgr2/data/request?outputdataset=%s&mask=RequestName&mask=RequestStatus">output</a>'%(ds_name)
            if 'pilup' in reasons:
                sub_url += ', <a href="https://cmsweb.cern.ch/reqmgr2/data/request?mc_pileup=%s&mask=RequestName&mask=RequestStatus">secondary</a>'%(ds_name)                
            table+="<tr><td>%s</td><td>%d</td><td><ul>%s</ul></td></tr>\n"%( sub_url, item[1]['size'], "<li>".join([""]+reasons))
            if reasons==['unlock']:
                only_unlock.add(item[0])
        table+="</table></html>"
        eosFile('%s/remaining_%s.html'%(monitor_dir,site),'w').write( table ).close()

        print "checking on unlock only datasets"
        to_ddm = UC.get('tiers_to_DDM')
        for item in only_unlock:
            tier = item.split('/')[-1]
            ds_status = getDatasetStatus(item)
            print item,ds_status
            if ds_status == 'PRODUCTION':
                print item,"is found",ds_status,"and unklocked on",site
                if options.invalidate_anything_left_production_once_unlocked:
                    print "Setting status to invalid for",item
                    setDatasetStatus(item, 'INVALID')
            if tier in to_ddm:
                print item,"looks like analysis and still dataops on",site
                if options.change_dataops_subs_to_anaops_once_unlocked:
                    print "Sending",item,"to anaops"
                    allCompleteToAnaOps(url, item)

def parse( options ):
    RDI = remainingDatasetInfo()
    UC = unifiedConfiguration()

    spec_site = filter(None,options.site.split(','))

    ## fetching global information
    locks = [l.item.split('#')[0] for l in session.query(Lock).filter(Lock.lock == True).all()]
    waiting = {}
    stuck = {}
    missing = {} 
    si = siteInfo()
    sis = si.disk.keys()
    random.shuffle( sis )
    n_site = options.nsites
    i_site = 0
    threads = []
    for site in sis:
        if spec_site and not site in spec_site:
            continue
        space = si.disk[site]
        if space and not spec_site: 
            continue
        if n_site and i_site>n_site:
            break
        i_site += 1
        
        print site,"has",space,"[TB] left out of",si.quota[site]
        threads.append( SiteBuster( site = site,
                                    UC = UC,
                                    RDI = RDI,
                                    SI = si,
                                    locks = copy.deepcopy(locks),
                                    waiting = copy.deepcopy(waiting),
                                    stuck = copy.deepcopy(stuck),
                                    missing = copy.deepcopy(missing),
                                    options = copy.deepcopy(options)
                                ))
    run_threads = ThreadHandler( threads = threads, 
                                 label = 'Site Threads',
                                 n_threads = 5 , 
                                 start_wait = 0,
                                 timeout = None,
                                 verbose=True)
    run_threads.run()
    #run_threads.start()
    #while run_threads.is_alive():
    #    time.sleep(10)
    

                    
def summary():
    ## not used anymore IMO
    RDI = remainingDatasetInfo()
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


if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    
    mlock = moduleLock(component='remainor',locking=False)
    ml=mlock()


    parser = optparse.OptionParser()
    parser.add_option('-s','--site', help="coma separated list of site to parse", default="")
    parser.add_option('-n','--nsites',help="number of site to parse", default=0, type=int)
    parser.add_option('-d','--ndatasets',help="number of top datasets to parse", default=0, type=int)
    parser.add_option('--subs-to-anaops', dest='change_dataops_subs_to_anaops_once_unlocked', default=False, action='store_true')
    parser.add_option('--invalidate', dest='invalidate_anything_left_production_once_unlocked', default=False, action='store_true')
    (options,args) = parser.parse_args()

    parse( options )
    

