#!/usr/bin/env python
import optparse
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
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
    elif exitcode == 71304:
        return "due to (running too long + soft kill failed) or (job eviction)"
    elif exitcode == 60318:
        return "due to a DQM server upload failure"
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

def include_in_other_category(exitcode):
    if exitcode == 60450:
        return True
    else:
        return False

url='cmsweb.cern.ch'

def print_job_failure_information(job_failure_information):

    istherefailureinformation=False

    mergedexitcodes=[]

    for wf in job_failure_information:
        for task in wf['task_dict']:
            for key in task['failures'].keys():
                if key not in mergedexitcodes:
                    mergedexitcodes.append(key)

    return_string=""

    firsttime_all=True
    for exitcode in mergedexitcodes:

        if include_in_other_category(exitcode):
            continue

        example_log_files=None
        firsttime_wf=True
        for wf in job_failure_information:
            firsttime=True
            for task in wf['task_dict']:
                if exitcode not in task['failures']:
                    continue
                
                if firsttime_wf and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                    istherefailureinformation=True
                    return_string=return_string+"there were the following failures "+explain_failure(exitcode,task['failures'][exitcode])+ "\n"
                    if example_log_files == None and len(task['failures'][exitcode]['logarchivefiles']) > 0:
                        example_log_files=task['failures'][exitcode]['logarchivefiles'][0]
                    firsttime_wf=False
                if firsttime and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                    return_string=return_string+"    in the workflow "+wf['wf_name']+"\n"
                    firsttime=False
                if 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:    
                    return_string=return_string+"        "+str(task['failures'][exitcode]['number'])+" out of "+str(task['totaljobs'])+" " +task['task_name']+" jobs\n"

        if provide_log_files(exitcode) and not firsttime_wf and example_log_files != None and example_log_files[0] != None:            
            return_string=return_string+"    here is an example:\n"
            return_string=return_string+"        method 1 to get the log file:\n"
            return_string=return_string+"            eos cp /eos/cms"+example_log_files[0].split('/castor/cern.ch/cms')[1]+ " .;\n            tar xpf "+example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+" WMTaskSpace/logCollect1/"+example_log_files[1]+";\n            rm " +example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+ ";\n"
            return_string=return_string+"        method 2 to get the log file:\n"
            return_string=return_string+"            stager_get -M "+example_log_files[0]+ ";\n            xrdcp -DIRequestTimeout 1000000000000000 root://castorcms/"+example_log_files[0]+" .;\n            tar xpf "+example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+" WMTaskSpace/logCollect1/"+example_log_files[1]+";\n            rm " +example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+ ";\n"
            #return_string=return_string+"        eos cp "+example_log_files[0].replace('/castor/cern.ch/cms','/eos/cms')+" .; tar xpf "+example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+ " WMTaskSpace/logCollect1/"+example_log_files[1]+"; rm " +example_log_files[0].split('/')[len(example_log_files[0].split('/'))-1]+ ";\n"

    firsttime_wf=True
    for wf in job_failure_information:
        firsttime=True
        for task in wf['task_dict']:
            sum=0        

            for exitcode in task['failures'].keys():
                if not include_in_other_category(exitcode):
                    sum+=task['failures'][exitcode]['number']

            if firsttime_wf and task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                istherefailureinformation=True
                return_string=return_string+"there were the following other failures\n"    
                firsttime_wf=False
            if firsttime and task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                return_string=return_string+"    in the workflow "+wf['wf_name']+"\n"
                firsttime=False
            if task['nfailurestot'] != sum and 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                return_string=return_string+"        "+  str(task['nfailurestot']-sum)+" out of "+str(task['totaljobs'])+" " +task['task_name']+" jobs\n"
                failureinformation=True

    return [istherefailureinformation,return_string]
