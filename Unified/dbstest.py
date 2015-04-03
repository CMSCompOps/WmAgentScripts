import reqMgrClient
from dbs.apis.dbsClient import DbsApi    
from utils import getDatasetChops, distributeToSites, getDatasetPresence, listSubscriptions, checkTransferStatus, approveSubscription, checkTransferApproval
from utils import findCustodialLocation, siteInfo
import pprint
import json
import httplib 
import os 
import sys
from collections import defaultdict
import random

dataset = '/TTJets_MSDecays_dileptonic_scaleup_7TeV-madgraph-tauola/Summer11Leg-START53_LV4-v1/GEN-SIM'
#dataset='/ggHH_HHTobbgg_M-125_14TeV_madgraph-pythia6-tauola/TP2023SHCALGS-DES23_62_V1-v1/GEN-SIM'

url = 'cmsweb.cern.ch'

#listSubscriptions(url ,dataset)
#approveSubscription(url, 439985)
#print checkTransferApproval(url, 439985)
print checkTransferStatus(url, 440195)
"""
get_those_to='T1_US_FNAL_Disk'
get_those=[]
for dataset in filter(None, open('stageout.txt').read().split('\n')):
    # check if there is a custodial
    # check = findCustodialLocation(url, dataset)
    check = getDatasetPresence(url, dataset,complete=None)
    if len(check):
        print "OK for dataset at",check
    else:
        print "need to pick a site and transfer"
        get_those.append( dataset )
print get_those
#res= makeReplicaRequest(url, get_those_to, get_those, "restaging because of Redigi Move custodial screw up")
print res

"""
sys.exit(1)

SI = siteInfo()

#items = getDatasetChops(dataset)
items = [['block'] for i in range(100)]
siteblacklist = ['T2_TH_CUNSTDA','T1_TW_ASGC','T2_TW_Taiwan']
sites = [s for s in json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/whitelist.json').read()) if s not in siteblacklist]
random.shuffle(sites)
sites = sites[:10]
#weights = { }
#for (i,site) in enumerate(sites):
    #weights[site]= random.random()
#    weights[site] = i

SI.cpu_pledges
spreading = distributeToSites( items, sites, n_per_site = 2 , weights=SI.cpu_pledges)

#pprint.pprint(dict(spreading))

for site in spreading:
    print site,SI.cpu_pledges[site],len(spreading[site])
