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

error_string1="RSS"
error_string2="StageOutFailure"
error_string3="Job has been running for more than"
error_string4="Job killed due to timeout"
error_string5="Return code: 134"

url='cmsweb.cern.ch'

inputFile=args[0]

f = open(inputFile, 'r')

#wf_names = []
#n_failures_tot = []
#n_failures_1 = []

for line in f:
    workflow = line.rstrip('\n')
    #wf_names.append(workflow)
    print "checking workflow " + workflow
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r11=conn.request('GET','/couchdb/wmstats/_design/WMStats/_view/jobsByStatusWorkflow?startkey=["'+workflow+'"]&endkey=["'+workflow+'",{}]&stale=ok&reduce=true&group_level=2')
    r12=conn.getresponse()
    data = r12.read()
    s = json.loads(data)
#print s['rows']
    #print "str(len(s['rows']))="+str(len(s['rows']))
    for i in range(0,len(s['rows'])):
        taskname=s['rows'][i]['key'][1]
        print "    checking task " + taskname
        #taskname='/franzoni_RVCMSSW_7_0_0_pre11RunMu2012C__OldTrk_RelVal_mu2012C_131221_072924_4122/HLTD/HLTDMergeRAWoutput/RECODreHLT'
    
        conn2  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r21=conn2.request('GET','/couchdb/wmstats/_design/WMStats/_view/jobsByStatusWorkflow?startkey=["'+workflow+'","'+taskname+'","jobfailed"]&endkey=["'+workflow+'%22,%22'+taskname+'","jobfailed",{}]&stale=ok&include_docs=true&reduce=false')
        r22=conn2.getresponse()
        data2 = r22.read()
        s2 = json.loads(data2)
        #print data2
        #print s2['rows']
        nfailures1 = 0
        nfailures2 = 0
        nfailures3 = 0
        nfailures4 = 0
        nfailures5 = 0
        nfailurestot = 0
        for j in range(0,len(s2['rows'])):
            nfailurestot = nfailurestot+1
            print "        job "+str(j)+":"
            for k in s2['rows'][j]['doc']['errors']:
                print "            "+k+" errors: "+str(len(s2['rows'][j]['doc']['errors'][k]))
                #print s2['rows'][j]['doc']['errors'][k][0]
                if len(s2['rows'][j]['doc']['errors'][k]) > 0:
                    if error_string1 in s2['rows'][j]['doc']['errors'][k][0]['details']:
                        #print "                found error_string1"
                        nfailures1=nfailures1+1
                        break
                    if error_string2 in s2['rows'][j]['doc']['errors'][k][0]['details']:
                        #print "                found error_string2"
                        nfailures2=nfailures2+1
                        break
                    if error_string3 in s2['rows'][j]['doc']['errors'][k][0]['details']:
                        #print "                found error_string3"
                        nfailures3=nfailures3+1
                        break
                    if error_string4 in s2['rows'][j]['doc']['errors'][k][0]['details']:
                        #print "                found error_string4"
                        nfailures4=nfailures4+1
                        break                                        
                    if error_string5 in s2['rows'][j]['doc']['errors'][k][0]['details']:
                        #print "                found error_string5"
                        nfailures5=nfailures5+1
                        break                                        
                        
            conn3  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))   
            r31=conn3.request('GET','/couchdb/wmstats/_design/WMStats/_view/latestRequest?reduce=true&group=true&keys=[["'+workflow+'","cmssrv113.fnal.gov:9999"],["'+workflow+'","cmssrv113.fnal.gov:9999"]]&stale=ok')
            r32=conn3.getresponse()
            data3 = r32.read()
            s3 = json.loads(data3)
            wf_id=s3['rows'][0]['value']['id']

            conn4  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            r41=conn4.request('GET','/couchdb/wmstats/_all_docs?keys=["'+wf_id+'",%20"'+wf_id+'"]&include_docs=true')
            r42=conn4.getresponse()
            data4 = r42.read()
            s4 = json.loads(data4)
            #print len(s4['rows'])
            #print s4['rows'][1]['doc']['tasks'][taskname]['status']
            #print len(s4['rows'][1]['doc']['tasks'][taskname]['status'])
            #print 'success' in s4['rows'][1]['doc']['tasks'][taskname]['status']
            #print 'failure' in s4['rows'][1]['doc']['tasks'][taskname]['status']
            #print 'exception' in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']
            #print len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'])

            totaljobs=0

            if len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 2:
                if 'success' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1:
                    print "problem with job status information"
                    sys.exit(0)
                else:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']+s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
            elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 1:
                if 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1:
                    print "problem with job status information"
                    sys.exit(0)
                else:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']
            else:
                print "problem with job status information"
                sys.exit(0)

        if nfailurestot > 0:
            print "        total jobs: "+str(totaljobs) 
            print "        total failures: "+str(nfailurestot)
            if nfailures1 > 0:
                print "        failures due to "+error_string1+": "+str(nfailures1)
            if nfailures2 > 0:    
                print "        failures due to "+error_string2+": "+str(nfailures2)
            if nfailures3 > 0:
                print "        failures due to "+error_string3+": "+str(nfailures3)
            if nfailures4 > 0:    
                print "        failures due to "+error_string4+": "+str(nfailures4)
            if nfailures5 > 0:    
                print "        failures due to "+error_string5+": "+str(nfailures5)            

            if nfailurestot != nfailures1+nfailures2+nfailures3+nfailures4+nfailures5:
                print "        missing some failures"



        #print "         PerformanceError errors: "+ str(len(s2['rows'][j]['doc']['errors']['PerformanceError']))    
        #print "         stageOut1 errors: "+ str(len(s2['rows'][j]['doc']['errors']['stageOut1']))
        #print "         cmsRun1 errors: "  + str(len(s2['rows'][j]['doc']['errors']['cmsRun1']))
        #print "         logArch1 errors: " + str(len(s2['rows'][j]['doc']['errors']['logArch1']))

        #print s2['rows'][j]['doc']['errors']['stageOut1'][0]['details']
