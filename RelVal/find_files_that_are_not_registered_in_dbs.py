#!/usr/bin/env python -u
from dbs.apis.dbsClient import DbsApi
import urllib2,urllib, httplib, sys, re, os
import json
import optparse

#fname="delete_this_1000.txt" #relval files
fname="relval_eos_file_names.txt" #other files
#fname="delete_this.txt" #other filse

inputfile = open(fname,'r')

dbsApi = DbsApi(url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
#dbsApi = DbsApi(url = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/')

dsets_fraction_lost = {}

access_type = "VALID"

for line in inputfile:
    lfn= line.rstrip('\n')

    if dbsApi.listFiles(logical_file_name = lfn) == []:
        print lfn
        continue

    continue

    #print "lfn = "+lfn
    if dbsApi.listDatasets(logical_file_name = lfn, dataset_access_type = "VALID") ==  [] and dbsApi.listDatasets(logical_file_name = lfn, dataset_access_type = "PRODUCTION") ==  [] and dbsApi.listDatasets(logical_file_name = lfn, dataset_access_type = "INVALID") ==  [] and dbsApi.listDatasets(logical_file_name = lfn, dataset_access_type = "DELETED") ==  [] and dbsApi.listDatasets(logical_file_name = lfn, dataset_access_type = "DEPRECATED") ==  []:
        print lfn
        continue

