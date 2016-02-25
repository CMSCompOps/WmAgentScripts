#!/usr/bin/env python
import optparse
import json
import urllib2, urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
sys.path.append("..")
import time


parser = optparse.OptionParser()
parser.add_option('--correct_env',action="store_true",dest='correct_env')
(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "
    
if not options.correct_env:
    os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; python2.6 "+command + "--correct_env")
    sys.exit(0)

url='cmsweb.cern.ch'

inputFile=args[0]

f = open(inputFile, 'r')

for line in f:
    workflow = line.rstrip('\n')
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request('GET','/couchdb/wmstats/_all_docs?keys=["'+workflow+'"]&include_docs=true')
    r2=conn.getresponse()
    data = r2.read()
    s = json.loads(data)
    print str(s['rows'][0]['doc']['request_status'][4])
    print str(s['rows'][0]['doc']['request_status'][6]['update_time'] - s['rows'][0]['doc']['request_status'][4]['update_time']) +" "+ workflow

sys.exit(0)


