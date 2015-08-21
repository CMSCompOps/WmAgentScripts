#!/usr/bin/env python
import optparse
import json
import urllib2, urllib, httplib, sys, re, os
from deprecated import phedexSubscription
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
    os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
    sys.exit(0)

url='cmsweb.cern.ch'

workflow="franzoni_RVCMSSW_7_0_0_pre11QQH1352T_Tauola_13_PU25ns__OldTrk_131221_073717_2277"
taskname="/franzoni_RVCMSSW_7_0_0_pre11QQH1352T_Tauola_13_PU25ns__OldTrk_131221_073717_2277/DIGIUP15_PU25/DIGIUP15_PU25MergeFEVTDEBUGHLToutput/RECOUP15_PU25"
job_number=2

conn2  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
r1=conn2.request('GET','/couchdb/wmstats/_design/WMStats/_view/jobsByStatusWorkflow?startkey=["'+workflow+'","'+taskname+'","jobfailed"]&endkey=["'+workflow+'%22,%22'+taskname+'","jobfailed",{}]&stale=ok&include_docs=true&reduce=false')
r2=conn2.getresponse()
data2 = r2.read()
s2 = json.loads(data2)

print "workflow "+workflow
print "task " +taskname
print "job "+str(job_number)
#print s2['rows'][job_number]['doc']['logArchiveLFN'].keys()[0]
#print s2['rows'][job_number]['doc']['output'][1]['lfn']

logarchive_srm=s2['rows'][0]['doc']['logArchiveLFN'].keys()[0]
print logarchive_srm

logarchive=logarchive_srm.split('/castor/cern.ch/cms')[1]

print logarchive

logarchivefile=logarchive.split('/')[len(logarchive.split('/'))-1]

print logarchivefile

unmerged_lfn=s2['rows'][job_number]['doc']['output'][len(s2['rows'][job_number]['doc']['output'])-1]['lfn']

print unmerged_lfn

unmerged_filename=unmerged_lfn.split('/')[len(unmerged_lfn.split('/'))-1]

print unmerged_filename

os.system("cmsStage "+logarchive+ " .")
os.system("tar xpf "+logarchivefile)
os.system("cp WMTaskSpace/logCollect1/"+unmerged_filename+" .")
os.system("rm -r WMTaskSpace")
os.system("rm "+logarchivefile)
