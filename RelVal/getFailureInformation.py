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
parser.add_option('--debug',action="store_true",dest='debug')
parser.add_option('--verbose',action="store_true",dest='verbose')
(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "
    
if not options.correct_env:
    os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
    sys.exit(0)

error_strings=[]

error_strings.append("RSS")
error_strings.append("StageOutFailure")
error_strings.append("Job has been running for more than")
error_strings.append("Job killed due to timeout")
error_strings.append("Return code: 134")
error_strings.append("FileReadError")
error_strings.append("Return code: 40")
error_strings.append("Return code: 137")
error_strings.append("No space left on device")
error_strings.append("SYSTEM_PERIODIC_REMOVE")
error_strings.append("FallbackFileOpenError")
error_strings.append("Job has exceeded maxVSize")
error_strings.append("The job has probably exhausted the virtual memory available to the process.")
error_strings.append("FileOpenError")

url='cmsweb.cern.ch'

inputFile=args[0]

f = open(inputFile, 'r')

wf_dicts = []

#loop over workflows
for line in f:
    workflow = line.rstrip('\n')
    if options.verbose or options.debug:
        print "checking workflow " + workflow
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r11=conn.request('GET','/couchdb/wmstats/_design/WMStats/_view/jobsByStatusWorkflow?startkey=["'+workflow+'"]&endkey=["'+workflow+'",{}]&stale=ok&reduce=true&group_level=2')
    r12=conn.getresponse()
    data = r12.read()
    s = json.loads(data)

    #loop over tasks
    for i in range(0,len(s['rows'])):
        task_dicts = []
        taskname=s['rows'][i]['key'][1]
        if options.debug:
            print "    checking task " + taskname
    
        conn2  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r21=conn2.request('GET','/couchdb/wmstats/_design/WMStats/_view/jobsByStatusWorkflow?startkey=["'+workflow+'","'+taskname+'","jobfailed"]&endkey=["'+workflow+'%22,%22'+taskname+'","jobfailed",{}]&stale=ok&include_docs=true&reduce=false')
        r22=conn2.getresponse()
        data2 = r22.read()
        s2 = json.loads(data2)

        nfailures={}

        for error_string in error_strings:
            nfailures[error_string]=0
        
        
        nfailurestot = 0

        #loop over failed jobs
        for j in range(0,len(s2['rows'])):
            nfailurestot = nfailurestot+1
            if options.debug:
                print "        job "+str(j)+":"
            #make sure that we count each job once    
            found_error_string=False     
            for k in s2['rows'][j]['doc']['errors']:
                if found_error_string == True:
                    continue
                if options.debug:
                    print "            "+k+" errors: "+str(len(s2['rows'][j]['doc']['errors'][k]))
                if len(s2['rows'][j]['doc']['errors'][k]) > 0:
                    for index in range(0, len(s2['rows'][j]['doc']['errors'][k])):
                        if found_error_string == True:
                            continue
                        for error_string in error_strings:
                            if error_string in s2['rows'][j]['doc']['errors'][k][index]['details']:
                                nfailures[error_string]=nfailures[error_string]+1
                                found_error_string=True
                                break

        conn3  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))   
        #r31=conn3.request('GET','/couchdb/wmstats/_design/WMStats/_view/latestRequest?reduce=true&group=true&keys=[["'+workflow+'","cmssrv113.fnal.gov:9999"],["'+workflow+'","vocms142.cern.ch:9999"]]&stale=ok')
        r31=conn3.request('GET','/couchdb/wmstats/_design/WMStats/_view/latestRequest?reduce=true&group=true&keys=[["'+workflow+'","cmsgwms-submit1.fnal.gov:9999"],["'+workflow+'","vocms053.cern.ch:9999"]]&stale=ok')
        
        r32=conn3.getresponse()
        data3 = r32.read()
        s3 = json.loads(data3)

        if len(s3['rows']) == 0:
            if options.verbose or options.debug:
                print "length of rows vector is 0  "
            continue
        
        wf_id=s3['rows'][0]['value']['id']
        
        conn4  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r41=conn4.request('GET','/couchdb/wmstats/_all_docs?keys=["'+wf_id+'",%20"'+wf_id+'"]&include_docs=true')
        r42=conn4.getresponse()
        data4 = r42.read()
        s4 = json.loads(data4)
        
        totaljobs=0


        if s4['rows'][1]['doc']==None:
            #print s4['rows'][1]
            if options.verbose or options.debug:
                print "s4['rows'][1]['doc'] equals None, setting total jobs to 999999999999"
            totaljobs=9999999999
        elif taskname not in s4['rows'][1]['doc']['tasks']:
            if options.verbose or options.debug:
                print "task "+taskname+" not found, setting total jobs to 999999999999"
            totaljobs=9999999999
        elif 'status' not in s4['rows'][1]['doc']['tasks'][taskname]:
            if options.verbose or options.debug:
                print "missing the number of failed and successful jobs in task, setting total jobs to 999999999999"
            totaljobs=9999999999    
        elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 2:
            if 'success' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1:
                print "problem with job status information"
                sys.exit(0)
            else:
                totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']+s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
        elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 1:
            if 'failure' in s4['rows'][1]['doc']['tasks'][taskname]['status'] and 'exception' in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] and len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) == 1:
                totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']
            elif 'success' in s4['rows'][1]['doc']['tasks'][taskname]['status']:
                totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
            else:     
                print "problem with job status information"
                print s4['rows'][1]['doc']['tasks'][taskname]['status']
                sys.exit(0)
        else:
            print "problem with job status information"
            sys.exit(0)
                
        if nfailurestot > 0 and options.debug:
            print "        total jobs: "+str(totaljobs) 
            print "        total failures: "+str(nfailurestot)


            for error_string in error_strings:
                if nfailures[error_string] > 0:
                    print "        failures due to "+error_string+": "+str(nfailures[error_string])


            sum=0        
            for error_string in error_strings:
                sum+=nfailures[error_string]

            if nfailurestot != sum:
                print "        missing some failures"

        task_dicts.append({'task_name':taskname.split('/')[len(taskname.split('/'))-1],'nfailures': nfailures,'nfailurestot':nfailurestot,'totaljobs':totaljobs})    

        wf_dicts.append({'wf_name':workflow,'task_dict':task_dicts})                      
        #print "         PerformanceError errors: "+ str(len(s2['rows'][j]['doc']['errors']['PerformanceError']))    
        #print "         stageOut1 errors: "+ str(len(s2['rows'][j]['doc']['errors']['stageOut1']))
        #print "         cmsRun1 errors: "  + str(len(s2['rows'][j]['doc']['errors']['cmsRun1']))
        #print "         logArch1 errors: " + str(len(s2['rows'][j]['doc']['errors']['logArch1']))

        #print s2['rows'][j]['doc']['errors']['stageOut1'][0]['details']
        
firsttime_all=True
for error_string in error_strings:
    firsttime_wf=True
    for wf in wf_dicts:
        firsttime=True
        for task in wf['task_dict']:
            if task['nfailures'][error_string]>0 and firsttime_wf and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                if firsttime_all and (options.verbose or options.debug):
                    print ""
                    firsttime_all=False
                print "there were the following failures due to "+error_string
                firsttime_wf=False
            if task['nfailures'][error_string]>0 and firsttime and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                print "    in the workflow "+wf['wf_name']
                firsttime=False
            if task['nfailures'][error_string] > 0 and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:    
                print "        "+str(task['nfailures'][error_string])+" out of "+str(task['totaljobs'])+" " +task['task_name']+" jobs"

firsttime_wf=True
for wf in wf_dicts:
    firsttime=True
    for task in wf['task_dict']:
        sum=0        
        for error_string in error_strings:
            sum+=task['nfailures'][error_string]
        if firsttime_wf and task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
            if firsttime_all and (options.verbose or options.debug):
                print ""
                firsttime_all=False
            print "there were the following failures due to other causes"    
            firsttime_wf=False
        if firsttime and task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
            print "    in the workflow "+wf['wf_name']
            firsttime=False
        if task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
            print "        "+  str(task['nfailurestot']-sum)+" out of "+str(task['totaljobs'])+" " +task['task_name']+" jobs"


if not options.verbose and not options.debug:
    sys.exit(0)

firsttime_wf=True
for wf in wf_dicts:
    firsttime=True
    for task in wf['task_dict']:
        sum=0        
        for error_string in error_strings:
            sum+=task['nfailures'][error_string]
        if firsttime_wf and task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
            if firsttime_all:
                print ""
                firsttime_all=False
            print "there were the following failures due to other causes (shown below out of the total number of failures)"
            firsttime_wf=False        
        if firsttime and task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
            print "    in the workflow "+wf['wf_name']
            firsttime=False
        if task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:            
            print "            "+ str(task['nfailurestot']-sum)+ " out of "+str(task['nfailurestot'])+" "+task['task_name']+" failures were missing"


            
firsttime_wf=True
for wf in wf_dicts:
    firsttime=True
    for task in wf['task_dict']:
        if task['nfailurestot'] > 0 and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
            if firsttime_wf :
                print ""
                print "there were the following failures"
                firsttime_wf=False
        if firsttime and task['nfailurestot'] > 0 and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:                
            print "    in the workflow "+wf['wf_name']
        if task['nfailurestot'] > 0 and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:    
            print "        "+ str(task['nfailurestot'])+" out of "+str(task['totaljobs'])+" " +task['task_name']+" jobs"
