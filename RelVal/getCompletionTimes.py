#!/usr/bin/env python
import optparse
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription
from xml.dom.minidom import getDOMImplementation
sys.path.append("..")
import dbsTest
import time


parser = optparse.OptionParser()
parser.add_option('--correct_env',action="store_true",dest='correct_env')
(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "
    
if not options.correct_env:
    os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
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


