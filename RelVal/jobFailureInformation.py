#!/usr/bin/env python
import optparse
import json
import urllib2,urllib, httplib, sys, re, os, phedexSubscription
from xml.dom.minidom import getDOMImplementation
sys.path.append("..")
import dbsTest
import time


def explain_failure(exitcode,failure):
    if exitcode == 61300:
        return "due to the hard timeout"
    elif exitcode == 50660:
        return "due to RSS"
    elif exitcode == 50664:
        return "due to running too long"
    elif exitcode == 134:
        return "due to a segmentation fault"
    elif exitcode == '8021':
        return "due to FileReadErrors"
    elif exitcode == '8028':
        return "due to FallbackFileOpenErrors"
    elif failure['details'] != None and 'Adding last ten lines of CMSSW stdout:' not in failure['details']:
        return "due to \n\n"+failure['details']+"\n"
    else:    
        return "due to exit code "+str(exitcode)
    
def provide_log_files(exitcode):
    if exitcode == '8028':
        return False
    elif exitcode == '8021':
        return False
    else:
        return True

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

def getFailureInformation(inputfilename,outputfilename="",verbose=False,debug=False):

    if outputfilename!="":
        of = open(outputfilename,'w')

    inf = open(inputfilename, 'r')
    
    wf_dicts = []
    
    #loop over workflows
    for line in inf:

        workflow = line.rstrip('\n')
        if verbose or debug:
            print "checking workflow " + workflow
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
            if debug:
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
                if debug:
                    print "        job "+str(j)+":"
                #make sure that we count each job once    
                found_error_string=False     

                found_job_killed_error=False

                #for logarchive jobs, for example, there should not be a logarchivelfn
                if 'logArchiveLFN' in s2['rows'][j]['doc']:
                    mergedfilename=s2['rows'][j]['doc']['logArchiveLFN'].keys()[0].split("=")[1]
                else:
                    mergedfilename=None
                for output in s2['rows'][j]['doc']['output']:
                    if '/store/unmerged/logs/' in output['lfn']:
                        unmergedfilename=output['lfn'].split('/')[len(output['lfn'].split('/')) - 1]



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
                
                if not found_performance_killed_error and not found_job_killed_error and 'cmsRun1' in s2['rows'][j]['doc']['errors']:
                    found_fatal_exception=False
                    for k in s2['rows'][j]['doc']['errors']['cmsRun1']:
                        if k['type'] == "Fatal Exception":
                            if k['exitCode'] in failures.keys():
                                failures[k['exitCode']]['number']=failures[k['exitCode']]['number']+1
                                failures[k['exitCode']]['logarchivefiles'].append([mergedfilename,unmergedfilename])
                            else:
                                failures[k['exitCode']]={'number' : 1, 'logarchivefiles' : [[mergedfilename,unmergedfilename]], 'details' : k['details']}
                            found_fatal_exception=True

                    found_cmssw_step_failures=False        
                    if not found_fatal_exception:
                        for k in s2['rows'][j]['doc']['errors']['cmsRun1']:
                            if k['type'] == "CMSSWStepFailure":
                                if k['exitCode'] in failures.keys():
                                    failures[k['exitCode']]['number']=failures[k['exitCode']]['number']+1
                                    failures[k['exitCode']]['logarchivefiles'].append([mergedfilename,unmergedfilename])
                                else:
                                    failures[k['exitCode']]={'number' : 1, 'logarchivefiles' : [[mergedfilename,unmergedfilename]], 'details': k['details']}
                                found_cmssw_step_failures=True

                #sys.exit(0)
                for k in s2['rows'][j]['doc']['errors']:
                    if found_error_string == True:
                        continue
                    if debug:
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
                if verbose or debug:
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
                if verbose or debug:
                    print "s4['rows'][1]['doc'] equals None, setting total jobs to 999999999999"
                totaljobs=9999999999
            elif taskname not in s4['rows'][1]['doc']['tasks']:
                if verbose or debug:
                    print "task "+taskname+" not found, setting total jobs to 999999999999"
                totaljobs=9999999999
            elif 'status' not in s4['rows'][1]['doc']['tasks'][taskname]:
                if verbose or debug:
                    print "missing the number of failed and successful jobs in task, setting total jobs to 999999999999"
                totaljobs=9999999999    
            elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 2:
                if 'transition' in s4['rows'][1]['doc']['tasks'][taskname]['status'] and 'success' in s4['rows'][1]['doc']['tasks'][taskname]['status']:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['success']

                elif 'success' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1:
                    os.system('echo '+workflow+' | mail -s \"jobFailureInformation error 1\" andrew.m.levin@vanderbilt.edu')
                    print "problem with job status information 1"
                    sys.exit(0)
                else:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']+s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
            elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 1:
                if 'failure' in s4['rows'][1]['doc']['tasks'][taskname]['status'] and 'exception' in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] and len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) == 1:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']
                elif 'success' in s4['rows'][1]['doc']['tasks'][taskname]['status']:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
                else:     
                    print "problem with job status information 2"
                    os.system('echo '+workflow+' | mail -s \"jobFailureInformation error 2\" andrew.m.levin@vanderbilt.edu')
                    print s4['rows'][1]['doc']['tasks'][taskname]['status']
                    sys.exit(0)
            #ignore the transition status        
            elif len(s4['rows'][1]['doc']['tasks'][taskname]['status']) == 3:
                if 'transition' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'success' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'failure' not in s4['rows'][1]['doc']['tasks'][taskname]['status'] or 'exception' not in s4['rows'][1]['doc']['tasks'][taskname]['status']['failure'] or len(s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']) != 1:
                    print "problem with job status information 4"
                    os.system('echo '+workflow+' | mail -s \"jobFailureInformation error 4\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(0)
                else:
                    totaljobs=s4['rows'][1]['doc']['tasks'][taskname]['status']['failure']['exception']+s4['rows'][1]['doc']['tasks'][taskname]['status']['success']
            else:
                print "problem with job status information 3"
                os.system('echo '+workflow+' | mail -s \"jobFailureInformation error 3\" andrew.m.levin@vanderbilt.edu')
                sys.exit(0)
                
            if nfailurestot > 0 and debug:
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

            task_dicts.append({'task_name':taskname.split('/')[len(taskname.split('/'))-1], 'failures': failures, 'nfailures': nfailures,'nfailurestot':nfailurestot,'totaljobs':totaljobs})    

            wf_dicts.append({'wf_name':workflow,'task_dict':task_dicts})                      
        #print "         PerformanceError errors: "+ str(len(s2['rows'][j]['doc']['errors']['PerformanceError']))    
        #print "         stageOut1 errors: "+ str(len(s2['rows'][j]['doc']['errors']['stageOut1']))
        #print "         cmsRun1 errors: "  + str(len(s2['rows'][j]['doc']['errors']['cmsRun1']))
        #print "         logArch1 errors: " + str(len(s2['rows'][j]['doc']['errors']['logArch1']))

        #print s2['rows'][j]['doc']['errors']['stageOut1'][0]['details']

    istherefailureinformation=False        

    mergedexitcodes=[]

    for wf in wf_dicts:
        for task in wf['task_dict']:
            for key in task['failures'].keys():
                if key not in mergedexitcodes:
                    mergedexitcodes.append(key)

    return_string=""

    firsttime_all=True
    for exitcode in mergedexitcodes:
        example_log_files=None
        firsttime_wf=True
        for wf in wf_dicts:
            firsttime=True
            for task in wf['task_dict']:
                if exitcode not in task['failures']:
                    continue
                
                if firsttime_wf and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                    istherefailureinformation=True
                    if firsttime_all and (verbose or debug):
                        return_string=return_string+""
                        firsttime_all=False
                    return_string=return_string+"there were the following failures "+explain_failure(exitcode,task['failures'][exitcode])+ "\n"
                    if example_log_files == None and len(task['failures'][exitcode]['logarchivefiles']) > 0:
                        example_log_files=task['failures'][exitcode]['logarchivefiles'][0]
                    firsttime_wf=False
                if firsttime and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                    return_string=return_string+"    in the workflow "+wf['wf_name']+"\n"
                    firsttime=False
                if 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:    
                    return_string=return_string+"        "+str(task['failures'][exitcode]['number'])+" out of "+str(task['totaljobs'])+" " +task['task_name']+" jobs\n"

        if provide_log_files(exitcode) and not firsttime_wf and example_log_files != None:            
            return_string=return_string+"    here is an example:\n"
            return_string=return_string+"        xrdcp root://castorcms/"+example_log_files[0]+" .; tar xpf "+example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+" WMTaskSpace/logCollect1/"+example_log_files[1]+"; rm " +example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+ ";\n"
            return_string=return_string+"        eos cp "+example_log_files[0].replace('/castor/cern.ch/cms','/eos/cms')+" .; tar xpf "+example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+ " WMTaskSpace/logCollect1/"+example_log_files[1]+"; rm " +example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+ ";\n"

    firsttime_wf=True
    for wf in wf_dicts:
        firsttime=True
        for task in wf['task_dict']:
            sum=0        
            for exitcode in task['failures'].keys():
                sum+=task['failures'][exitcode]['number']
            if firsttime_wf and task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                istherefailureinformation=True
                if firsttime_all and (verbose or debug):
                    print ""
                    firsttime_all=False
                return_string=return_string+"there were the following other failures\n"    
                firsttime_wf=False
            if firsttime and task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                return_string=return_string+"    in the workflow "+wf['wf_name']+"\n"
                firsttime=False
            if task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                return_string=return_string+"        "+  str(task['nfailurestot']-sum)+" out of "+str(task['totaljobs'])+" " +task['task_name']+" jobs\n"
                failureinformation=True

    return [istherefailureinformation,return_string]
