#!/usr/bin/env python
import optparse
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import time

url='cmsweb.cern.ch'

def collect_job_failure_information(wf_list,verbose=False,debug=False):

    wf_dicts = []
    
    #loop over workflows
    for workflow in wf_list:

        if workflow == "fabozzi_RVCMSSW_8_0_11QQH1352T_13_PU25ns__reHLT_160615_130122_5498":
            continue

        print workflow

        #workflow = line.rstrip('\n')
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r11=conn.request('GET','/couchdb/wmstats/_design/WMStats/_view/jobsByStatusWorkflow?startkey=["'+workflow+'"]&endkey=["'+workflow+'",{}]&stale=ok&reduce=true&group_level=2')
        r12=conn.getresponse()
        data = r12.read()
        s = json.loads(data)

        #loop over tasks
        for i in range(0,len(s['rows'])):
            failures={}
            task_dicts = []
            taskname=s['rows'][i]['key'][1]

            if "CleanupUnmerged" in taskname.split('/')[len(taskname.split('/'))-1]:
                continue
    
            conn2  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            r21=conn2.request('GET','/couchdb/wmstats/_design/WMStats/_view/jobsByStatusWorkflow?startkey=["'+workflow+'","'+taskname+'","jobfailed"]&endkey=["'+workflow+'%22,%22'+taskname+'","jobfailed",{}]&stale=ok&include_docs=true&reduce=false')
            r22=conn2.getresponse()
            data2 = r22.read()
            s2 = json.loads(data2)

            nfailures={}

            nfailurestot = 0

            #loop over failed jobs
            for j in range(0,len(s2['rows'])):

                nfailurestot = nfailurestot+1

                found_job_killed_error=False

                #for logarchive jobs, for example, there should not be a logarchivelfn
                #another example is the case where the there is a failure in the logarchive step
                if 'logArchiveLFN' in s2['rows'][j]['doc']:
                    mergedfilename=s2['rows'][j]['doc']['logArchiveLFN'].keys()[0].split("=")[1]
                    unmergedfilename=None
                    for output in s2['rows'][j]['doc']['output']:
                        if '/store/unmerged/logs/' in output['lfn']:
                            unmergedfilename=output['lfn'].split('/')[len(output['lfn'].split('/')) - 1]
                else:
                    mergedfilename=None
                    unmergedfilename=None




                if 'JobKilled' in s2['rows'][j]['doc']['errors']:
                    if s2['rows'][j]['doc']['errors']['JobKilled'][0]['exitCode'] in failures.keys():

                        failures[s2['rows'][j]['doc']['errors']['JobKilled'][0]['exitCode']]['number']=failures[s2['rows'][j]['doc']['errors']['JobKilled'][0]['exitCode']]['number']+1
                    else:    

                        failures[s2['rows'][j]['doc']['errors']['JobKilled'][0]['exitCode']]={'number': 1, 'logarchivefiles': [], 'details': None}
                    found_job_killed_error=True    
                
                found_performance_killed_error=False
                if not found_job_killed_error and 'PerformanceError' in s2['rows'][j]['doc']['errors']:
                    if s2['rows'][j]['doc']['errors']['PerformanceError'][0]['exitCode'] in failures.keys():
                        failures[s2['rows'][j]['doc']['errors']['PerformanceError'][0]['exitCode']]['number']=failures[s2['rows'][j]['doc']['errors']['PerformanceError'][0]['exitCode']]['number']+1
                        failures[s2['rows'][j]['doc']['errors']['PerformanceError'][0]['exitCode']]['logarchivefiles'].append([mergedfilename,unmergedfilename])
                    else:    
                        failures[s2['rows'][j]['doc']['errors']['PerformanceError'][0]['exitCode']]={'number': 1, 'logarchivefiles': [[mergedfilename,unmergedfilename]], 'details': None}

                    found_performance_killed_error=True    

                found_fatal_exception=False                
                found_cmssw_step_failures=False        
                found_scram_script_failure=False
                found_no_output_failure=False
                if not found_performance_killed_error and not found_job_killed_error and 'cmsRun1' in s2['rows'][j]['doc']['errors']:


                    for k in s2['rows'][j]['doc']['errors']['cmsRun1']:
                        if k['type'] == "Fatal Exception":
                            if k['exitCode'] in failures.keys():
                                failures[k['exitCode']]['number']=failures[k['exitCode']]['number']+1
                                failures[k['exitCode']]['logarchivefiles'].append([mergedfilename,unmergedfilename])
                            else:
                                failures[k['exitCode']]={'number' : 1, 'logarchivefiles' : [[mergedfilename,unmergedfilename]], 'details' : k['details']}
                            found_fatal_exception=True
                            break #for multicore jobs, there can be a failure for each thread

                    if not found_fatal_exception:
                        for k in s2['rows'][j]['doc']['errors']['cmsRun1']:
                            if k['type'] == "CMSSWStepFailure":
                                if k['exitCode'] in failures.keys():
                                    failures[k['exitCode']]['number']=failures[k['exitCode']]['number']+1
                                    failures[k['exitCode']]['logarchivefiles'].append([mergedfilename,unmergedfilename])
                                else:
                                    failures[k['exitCode']]={'number' : 1, 'logarchivefiles' : [[mergedfilename,unmergedfilename]], 'details': k['details']}
                                found_cmssw_step_failures=True
                                break #for multicore jobs, there can be a failure for each thread

                    if not found_fatal_exception and not found_cmssw_step_failures:
                        for k in s2['rows'][j]['doc']['errors']['cmsRun1']:
                            if k['type'] == 'SCRAMScriptFailure':
                                if k['exitCode'] in failures.keys():
                                    failures[k['exitCode']]['number']=failures[k['exitCode']]['number']+1
                                else:
                                    failures[k['exitCode']]={'number' : 1, 'logarchivefiles' : [], 'details': k['details']}
                                found_scram_script_failure=True
                                break #for multicore jobs, there can be a failure for each thread

                    if not found_fatal_exception and not found_cmssw_step_failures and not found_scram_script_failure:
                        for k in s2['rows'][j]['doc']['errors']['cmsRun1']:
                            if k['type'] == 'NoOutput':
                                if k['exitCode'] in failures.keys():
                                    failures[k['exitCode']]['number']=failures[k['exitCode']]['number']+1
                                else:
                                    failures[k['exitCode']]={'number' : 1, 'logarchivefiles' : [], 'details': k['details']}
                                found_no_output_failure=True
                                break #for multicore jobs, there can be a failure for each thread
                
                found_upload_failure=False
                if not found_performance_killed_error and not found_job_killed_error and not found_fatal_exception and not found_cmssw_step_failures and not found_scram_script_failure and not found_no_output_failure and 'upload1' in s2['rows'][j]['doc']['errors']:

                    for k in s2['rows'][j]['doc']['errors']['upload1']:
                        if k['type'] == "DQMUploadFailure":
                            if k['exitCode'] in failures.keys():
                                failures[k['exitCode']]['number']=failures[k['exitCode']]['number']+1
                            else:
                                failures[k['exitCode']]={'number' : 1, 'logarchivefiles' : [], 'details': k['details']}
                            found_upload_failure=True
                            break #for multicore jobs, there can be a failure for each thread

            conn3  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))   
        #r31=conn3.request('GET','/couchdb/wmstats/_design/WMStats/_view/latestRequest?reduce=true&group=true&keys=[["'+workflow+'","cmssrv113.fnal.gov:9999"],["'+workflow+'","vocms142.cern.ch:9999"]]&stale=ok')
            r31=conn3.request('GET','/couchdb/wmstats/_design/WMStats/_view/latestRequest?reduce=true&group=true&keys=[["'+workflow+'","vocms026.cern.ch:9999"],["'+workflow+'","vocms053.cern.ch:9999"]]&stale=ok')
        
            r32=conn3.getresponse()
            data3 = r32.read()
            s3 = json.loads(data3)

            if len(s3['rows']) == 0:
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
                totaljobs=9999999999
            elif taskname not in s4['rows'][1]['doc']['tasks']:
                totaljobs=9999999999
            elif 'status' not in s4['rows'][1]['doc']['tasks'][taskname]:
                totaljobs=9999999999    
            elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 2:
                if 'transition' in s4['rows'][1]['doc']['tasks'][taskname]['status'] and 'success' in s4['rows'][1]['doc']['tasks'][taskname]['status']:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
                elif 'submitted' in s4['rows'][1]['doc']['tasks'][taskname]['status'] and 'success' in s4['rows'][1]['doc']['tasks'][taskname]['status']:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
                elif 'failure' in s4['rows'][1]['doc']['tasks'][taskname]['status'] and 'success' in s4['rows'][1]['doc']['tasks'][taskname]['status'] and len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) == 2 and 'exception' in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] and 'submit' in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']+s4['rows'][1]['doc']['tasks'][taskname]['status']['success']+ s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['submit']
                elif ('success' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1):
                    os.system('echo '+workflow+' | mail -s \"collect_job_failure_information.py error 1\" andrew.m.levin@vanderbilt.edu')
                    print "collect_job_failure_information.py error 1"
                    sys.exit(0)
                else:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']+s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
            elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 1:
                if 'failure' in s4['rows'][1]['doc']['tasks'][taskname]['status'] and 'exception' in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] and len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) == 1:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']
                elif 'success' in s4['rows'][1]['doc']['tasks'][taskname]['status']:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
                else:     
                    print "collect_job_failure_information.py error 2"
                    os.system('echo '+workflow+' | mail -s \"collect_job_failure_information.py error 2\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(0)
            #ignore the transition status        
            elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 3:
                #due to replication issues there can still be some submitted jobs in wmstats even after the workflow moves to completed
                if ('transition' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'success' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1) and ('submitted' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'success' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1): 
                    print "problem with job status information 4"
                    print s4['rows'][1]['doc']['tasks'][taskname]['status']
                    os.system('echo '+workflow+' | mail -s \"collect_job_failure_information.py error 4\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(0)
                else:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']+s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
            elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 5:
                if 'transition' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'success' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1 or 'cooloff' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'transition' not in s4['rows'][1]['doc']['tasks'][taskname]['status']: 
                    print "problem with job status information 6"

                    os.system('echo '+workflow+' | mail -s \"collect_job_failure_information.py error 6\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(0)
                else:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']+s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
            elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 4:
                if 'transition' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'success' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1 or 'transition' not in s4['rows'][1]['doc']['tasks'][taskname]['status']: 
                    print "problem with job status information 7"
                    print s4['rows'][1]['doc']['tasks'][taskname]['status']
                    os.system('echo '+workflow+' | mail -s \"collect_job_failure_information.py error 7\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(0)
                else:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']+s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
            else:
                print "problem with job status information 3"
                print s4['rows'][1]['doc']['tasks'][taskname]['status']
                os.system('echo '+workflow+' | mail -s \"collect_job_failure_information.py error 3\" andrew.m.levin@vanderbilt.edu')
                sys.exit(0)
                
            task_dicts.append({'task_name':taskname.split('/')[len(taskname.split('/'))-1], 'failures': failures, 'nfailures': nfailures,'nfailurestot':nfailurestot,'totaljobs':totaljobs})    

            wf_dicts.append({'wf_name':workflow,'task_dict':task_dicts})                      

    return wf_dicts        
