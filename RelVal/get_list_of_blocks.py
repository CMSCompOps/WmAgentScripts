#!/usr/bin/env python -u
from dbs.apis.dbsClient import DbsApi
import urllib2,urllib, httplib, sys, re, os
import json
import optparse

parser = optparse.OptionParser()
parser.add_option('--dataset', dest='dataset')
parser.add_option('--runwhitelist', dest='runwhitelist')
parser.add_option('--output_fname', dest='output_fname')

(options,args) = parser.parse_args()

if options.dataset == None or options.runwhitelist == None or options.output_fname == None:
    print "Usage: python2.6 get_list_of_blocks.py --dataset DATASETNAME --runwhitelist RUNWHITELIST --output_fname OUTPUTFILENAME"
    sys.exit(0)


dataset=options.dataset
runwhitelist = options.runwhitelist
output_fname = options.output_fname

dbsApi = DbsApi(url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader')

blocks=dbsApi.listBlocks(dataset = dataset, run_num = runwhitelist)

output_file = open(output_fname,'w')

for block in blocks:
    print >> output_file, block['block_name']

if True:
    sys.exit(1)
else:
    sys.exit(0)
