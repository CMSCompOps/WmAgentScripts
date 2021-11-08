#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, getWorkflows, global_SI, sendEmail, componentInfo, getDatasetPresence, monitor_dir, monitor_pub_dir, reqmgr_url, campaignInfo, unifiedConfiguration, sendLog, do_html_in_each_module, base_eos_dir, eosRead, eosFile, agent_speed_draining, cacheInfo
import reqMgrClient
import json
import os, sys
import time
import random
import optparse
from collections import defaultdict
import random
import copy 
import itertools
from htmlor import htmlor
import math

def mappor(url , options=None):

    up = componentInfo(soft=['mcm','wtc','jira']) 

    ## define regionality site => fallback allowed. feed on an ssb metric ??
    mapping = defaultdict(list)
    reversed_mapping = defaultdict(list)
    regions = defaultdict(list)

    UC = unifiedConfiguration()
    over_rides = []
    use_T0 = ('T0_CH_CERN' in UC.get("site_for_overflow"))
    if options.t0: use_T0 = True
    if use_T0: over_rides.append('T0_CH_CERN')

    use_HLT = ('T2_CH_CERN_HLT' in UC.get("site_for_overflow"))
    if options.hlt: use_HLT = True
    if use_HLT: over_rides.append('T2_CH_CERN_HLT')
    
    use_CSCS = ('T0_CH_CSCS_HPC' in UC.get("site_for_overflow"))
    if options.cscs: use_CSCS = True
    if use_CSCS: over_rides.append('T0_CH_CSCS_HPC')
    

    SI = global_SI( over_rides )
    #print sorted(SI.all_sites)
    #print sorted(SI.sites_T0s)

    CI = campaignInfo()
    
    #sites_to_consider = SI.all_sites
    sites_to_consider = SI.sites_ready
    for site in sites_to_consider:
        region = site.split('_')[1]
        if not region in ['US'
                          ,'DE','IT','FR','CH',
                          'ES',
                          'UK',
                          'RU'### latest addition
                          ]: continue
        regions[region] = [region] 

    def site_in_depletion(s):
        return True
        if s in SI.sites_pressure:
            (m, r, pressure) = SI.sites_pressure[s]
            if float(m) < float(r):
                print s,m,r,"lacking pressure"
                return True
            else:
                print s,m,r,"pressure"
                pass
                
        return False

    for site in sites_to_consider:
        region = site.split('_')[1]
        ## fallback to the region, to site with on-going low pressure
        within_region = [fb for fb in sites_to_consider if any([('_%s_'%(reg) in fb and fb!=site and site_in_depletion(fb))for reg in regions[region]]) ]
        #print site,region, within_region
        mapping[site] = within_region
    

    for site in sites_to_consider:
        if site.split('_')[1] == 'US': ## to all site in the US
            ## add NERSC 
            mapping[site].append('T3_US_NERSC')
            mapping[site].append('T3_US_SDSC')
            mapping[site].append('T3_US_TACC')
            mapping[site].append('T3_US_PSC')
            ## add OSG            
            mapping[site].append('T3_US_OSG')
            #mapping[site].append('T3_US_Colorado')
            pass

    if use_HLT:
        mapping['T2_CH_CERN'].append('T2_CH_CERN_HLT')

    if use_T0:
        ## who can read from T0
        mapping['T2_CH_CERN'].append('T0_CH_CERN')
        mapping['T1_IT_CNAF'].append('T0_CH_CERN')
        mapping['T1_FR_CCIN2P3'].append('T0_CH_CERN')
        mapping['T1_DE_KIT'].append('T0_CH_CERN')

    if use_CSCS: 
	## analog config to T0: 
	mapping['T2_CH_CERN'].append('T0_CH_CSCS_HPC')
        mapping['T1_IT_CNAF'].append('T0_CH_CSCS_HPC')
        mapping['T1_FR_CCIN2P3'].append('T0_CH_CSCS_HPC')
        mapping['T1_DE_KIT'].append('T0_CH_CSCS_HPC')

    ## temptatively
    mapping['T0_CH_CERN'].append( 'T2_CH_CERN' )

    ## all europ can read from CERN
    for reg in ['IT','DE','UK','FR','BE','ES']:
        mapping['T2_CH_CERN'].extend([fb for fb in sites_to_consider if '_%s_'%reg in fb])
        pass

    ## all europ T1 among each others
    europ_t1 = [site for site in sites_to_consider if site.startswith('T1') and any([reg in site for reg in ['IT','DE','UK','FR','ES','RU']])]
    #print europ_t1
    for one in europ_t1:
        for two in europ_t1:
            if one==two: continue
            mapping[one].append(two)
            pass
        ## all EU T1 can read from T0
        mapping['T0_CH_CERN'].append( one )
        
    mapping['T0_CH_CERN'].append( 'T1_US_FNAL' )
    #mapping['T1_IT_CNAF'].append( 'T1_US_FNAL' )
    #mapping['T1_IT_CNAF'].extend( [site for site in SI.sites_ready if '_US_' in site] ) ## all US can read from CNAF
    mapping['T1_IT_CNAF'].append( 'T2_CH_CERN' )
    mapping['T1_DE_KIT'].append( 'T2_CH_CERN' )
    mapping['T2_CH_CERN'].append( 'T1_IT_CNAF' )
    mapping['T2_CH_CERN'].append( 'T1_US_FNAL' )
    mapping['T2_CH_CERN'].append('T3_CH_CERN_HelixNebula')
    mapping['T2_CH_CERN'].append('T3_CH_CERN_HelixNebula_REHA')
    

    for site in sites_to_consider:
        if '_US_' in site:
            mapping[site].append('T2_CH_CERN')
    ## make them appear as OK to use
    force_sites = []

    ## overflow CERN to underutilized T1s
    upcoming = json.loads( eosRead('%s/GQ.json'%monitor_dir) )
    for possible in SI.sites_T1s:
        if not possible in upcoming:
            mapping['T2_CH_CERN'].append(possible)
            pass

    take_site_out = UC.get('site_out_of_overflow')

    for site,fallbacks in mapping.items():
        mapping[site] = list(set(fallbacks))
    

    ### mapping is a dictionnary where
    # key can read from site in values. 
    ### reverserd mapping is a dictionnary where
    # key can be read by site in values.
    ## create the reverse mapping for the condor module
    for site,fallbacks in mapping.items():
        if site in take_site_out:
            print "taking",site,"out of overflow source by unified configuration"
            mapping.pop(site)
            continue
        for fb in fallbacks:
            if fb == site: 
                ## remove self
                mapping[site].remove(fb)
                continue
            if fb in take_site_out:
                ## remove those to be removed
                print "taking",fb,"out of overflow destination by unified configuration"
                mapping[site].remove(fb)
                continue
            if not site in reversed_mapping[fb]:
                reversed_mapping[fb].append(site)


    ## write it out and bail
    cache = cacheInfo()
    cache.store('overflow_mapping', mapping)
    cache.store('overflow_reverse_mapping', reversed_mapping)
    return

if __name__ == "__main__":
    url = reqmgr_url
    parser = optparse.OptionParser()
    parser.add_option('--t0',help="Allow to use T0", default=False, action='store_true')
    parser.add_option('--hlt',help="Allow to use HLT", default=False, action='store_true')
    parser.add_option('--cscs',help="Allow to use CSCS", default=False, action='store_true')
    (options,args) = parser.parse_args()
    mappor(url, options=options)

