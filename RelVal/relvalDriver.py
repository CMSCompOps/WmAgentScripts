#!/usr/bin/env python
import optparse
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription
from xml.dom.minidom import getDOMImplementation
sys.path.append("..")
import dbsTest
import time, datetime

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


while True:
    batches=os.listdir("batches")
    if len(batches) == 0:
        print "no batches, exiting"
        sys.exit(0)
    for file in batches:

        print "checking file "+str(file)
    
        f = open("batches/" +file, 'r')
        
        n_completed=0
        n_workflows=0
        
        for line in f:
            n_workflows=n_workflows+1
            workflow = line.rstrip('\n')
            conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            r1=conn.request('GET','/couchdb/wmstats/_all_docs?keys=["'+workflow+'"]&include_docs=true')
            r2=conn.getresponse()
            data = r2.read()
            s = json.loads(data)
            
            if len(s['rows'][0]['doc']['request_status']) >= 7:
                if s['rows'][0]['doc']['request_status'][6]['status'] == "completed":
                    n_completed=n_completed+1
                    
        print "datetime.datetime.now() = " + str(datetime.datetime.now())           
        print "n_workflows = " + str(n_workflows)
        print "n_completed = " + str(n_completed)
                    

        if n_workflows == n_completed:
            print file
            os.system("bash announce_relvals.sh batches/"+file+" "+file + "| tee relvalDriverLogs/annouce_relvals_log.txt")
            os.popen("cat relvalDriverLogs/annouce_relvals_log.txt | mail -s \""+ file +"\" andrew.m.levin@vanderbilt.edu -- -f amlevin@mit.edu");
            print "moving "+file+" out of batches folder"
            os.system("mv batches/"+file+" .")
            
            #sys.exit(0)

    time.sleep(3600)     

print "no batches, exiting"
sys.exit(0)


