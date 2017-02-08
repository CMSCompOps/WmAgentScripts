#!/usr/bin/env python -u
from dbs.apis.dbsClient import DbsApi
import urllib2,urllib, httplib, sys, re, os
import json
import optparse

#fname="delete_this_1000.txt" #relval files
fname="delete_this_999.txt" #other files

inputfile = open(fname,'r')

dbsApi = DbsApi(url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader')

dsets_fraction_lost = {}

access_type = "VALID"

for line in inputfile:
    lfn= line.rstrip('\n')
    #print lfn

    if dbsApi.listDatasets(logical_file_name = lfn, dataset_access_type = access_type) ==  []:
        print lfn
        continue

    #print dbsApi.listDatasets(logical_file_name = "/store/mc/RunIIWinter15GS/WToENu_M-500_TuneCUETP8M1_13TeV-pythia8/GEN-SIM/MCRUN2_71_V1-v3/00000/4AF070C6-DFE5-E411-A0D6-0025905B8572.root")

    dset_name = dbsApi.listDatasets(logical_file_name = lfn, dataset_access_type = access_type)[0]['dataset']

    status= dbsApi.listDatasets(dataset = dset_name, detail=1, dataset_access_type = access_type)[0]['dataset_access_type']

    if status != "VALID" and status != "PRODUCTION":
        print "found dataset that does not have VALID or PRODUCTION status, exiting"
        sys.exit(1)

    if dset_name not in dsets_fraction_lost.keys():
        dsets_fraction_lost[dset_name] =  {'n_total_events' : dbsApi.listFileSummaries(dataset = dset_name)[0]['num_event'], 'n_events_lost' : dbsApi.listFiles(logical_file_name = lfn, detail =1)[0]['event_count']}
    else:
        dsets_fraction_lost[dset_name]['n_events_lost'] = dsets_fraction_lost[dset_name]['n_events_lost'] + dbsApi.listFiles(logical_file_name = lfn, detail =1)[0]['event_count']


    #print dbsApi.listFiles(logical_file_name = lfn, detail =1)[0]['event_count']
    

for key in dsets_fraction_lost.keys():
    if 'DQMIO' not in key:
        print key + " " +str(float(dsets_fraction_lost[key]['n_events_lost'])/ float(dsets_fraction_lost[key]['n_total_events']))
    else:
        print key + " unknown"
