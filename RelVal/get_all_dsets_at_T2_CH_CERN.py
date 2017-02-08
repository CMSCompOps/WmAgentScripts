#!/usr/bin/env python -u
#from dbs.apis.dbsClient import DbsApi
import urllib2,urllib, httplib, sys, re, os
import json
import optparse
from xml.dom import minidom

parser = optparse.OptionParser()
#parser.add_option('--output_fname', dest='output_fname')

(options,args) = parser.parse_args()

#if options.output_fname == None:
#    print "Usage: python2.6 get_all_dsets_at_T2_CH_CERN.py"
#    sys.exit(0)

url1 = "https://cmsweb.cern.ch/phedex/datasvc/xml/prod/blockreplicas?dataset=/*RelVal*/*/*&create_since=0&node=T2_CH_CERN"
webpage1 = urllib.urlopen(url1)
xmldoc1 = minidom.parse(webpage1)

url2 = "https://cmsweb.cern.ch/phedex/datasvc/xml/prod/blockreplicas?dataset=/*/*RelVal*/*&create_since=0&node=T2_CH_CERN"
webpage2 = urllib.urlopen(url2)
xmldoc2 = minidom.parse(webpage2)

selected_group = "DataOps"
#selected_group = "IB RelVal"

for phedex in  xmldoc1.childNodes:
    for block in phedex.childNodes:
        for replica in block.childNodes:
            group = replica.attributes['group'].value
            if group == selected_group:
            #if group not in ["RelVal","IB RelVal","DataOps",""]:
                print block.attributes['name'].value

for phedex in  xmldoc2.childNodes:
    for block in phedex.childNodes:
        for replica in block.childNodes:
            group = replica.attributes['group'].value
            if group == selected_group:
            #if group not in ["RelVal","IB RelVal","DataOps",""]:
                print block.attributes['name'].value

        #print block.attributes['name'].value

#        for subscription in dataset.childNodes:

#input = urllib.urlopen(url)

#output_fname = options.output_fname

#dbsApi = DbsApi(url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader')

#blocks=dbsApi.listBlocks(dataset = dataset, run_num = runwhitelist)

#output_file = open(output_fname,'w')


