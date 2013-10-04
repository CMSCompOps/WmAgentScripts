#!/usr/bin/env python

import sys,os
import subprocess

def increaseCounterInDict(dict,site,type):
    # print 'site',site,'type',type
    if site in dict.keys():
        dict[site][type] += 1
    else :
        tmp = {
        'Processing': 0,
        'Production': 0,
        'Merge': 0,
        'Cleanup': 0,
        'LogCollect': 0,
        }
        dict[site] = tmp
        dict[site][type] += 1
    # print 'dict',dict

def fillIDinDict(dict,site,id):
    if site not in dict.keys(): dict[site] = []
    dict[site].append(id)

def fillIDWFinDict(dict,site,workflow,id):
    if site not in dict.keys(): dict[site] = {}
    if workflow not in dict[site].keys(): dict[site][workflow] = []
    dict[site][workflow].append(id)

def printDict(dict,description):
    sorted = dict.keys()
    sorted.sort()
    print '----------------------------------------------------------------------------------------------------'
    print '| %20s | Processing | Production | Merge      | Cleanup    | LogCollect | Total      |' % description
    print '----------------------------------------------------------------------------------------------------'
    total_processing = 0
    total_production = 0
    total_merge = 0
    total_cleanup = 0
    total_logcollect = 0
    total = 0
    for site in sorted:
        total_production += dict[site]['Production']
        total_processing += dict[site]['Processing']
        total_merge += dict[site]['Merge']
        total_cleanup += dict[site]['Cleanup']
        total_logcollect += dict[site]['LogCollect']
        total += dict[site]['Processing']
        total += dict[site]['Production']
        total += dict[site]['Merge']
        total += dict[site]['Cleanup']
        total += dict[site]['LogCollect']
        print '| %20s | %10d | %10d | %10d | %10d | %10d | %10d |' % (site,dict[site]['Processing'],dict[site]['Production'],dict[site]['Merge'],dict[site]['Cleanup'],dict[site]['LogCollect'],dict[site]['Processing']+dict[site]['Production']+dict[site]['Merge']+dict[site]['Cleanup']+dict[site]['LogCollect'])
    print '----------------------------------------------------------------------------------------------------'
    print '| %20s | %10d | %10d | %10d | %10d | %10d | %10d |' % ('Total',total_processing,total_production,total_merge,total_cleanup,total_logcollect,total)
    print '----------------------------------------------------------------------------------------------------'

overview_running = {}
overview_pending = {}
overview_other = {}
overview_running48 = {}
overview_numjobstart = {}
overview_removereason = {}
jobs_48 = {}
jobs_numjobstart = {}
jobs_removereason = {}

#command='condor_q -format "%i." ClusterID -format "%s " ProcId -format " %i " JobStatus  -format " %d " ServerTime-EnteredCurrentStatus -format "%s" UserLog -format " %s" DESIRED_Sites -format " %s" RemoveReason -format " %i\n" NumJobStarts'
#command="""condor_q -format "%i." ClusterID -format "%s " ProcId -format " %i " JobStatus  -format " %d " ServerTime-JobStartDate -format "%s" UserLog -format " %s" DESIRED_Sites -format " %s" RemoveReason -format " %i\n" NumJobStarts | awk '{if ($2!= 1) print $0}'"""
command="""condor_q -format "%i." ClusterID -format "%s " ProcId -format " %i " JobStatus  -format " %d " ServerTime -format " %d " JobStartDate -format " %d" EnteredCurrentStatus -format " %s" UserLog -format " %s" DESIRED_Sites -format " %s" RemoveReason -format " %i\n" NumJobStarts"""
proc = subprocess.Popen(command, stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
out, err = proc.communicate()
for line in out.split('\n') :
    if line == "" : continue
    array = line.split()
    id = array[0]
    status = int(array[1])
    ServerTime=int(array[2])
    JobStartDate=int(array[3])
    if 'sleep' in line:
        print line
        continue
    #if len(array)<8:
    if not array[4].isdigit():
        EnteredCurrentStatus=int(array[3])
        log =array[4]
        site = array[5]
    else:
        EnteredCurrentStatus=int(array[4])
        log = array[5]
        site = array[6]
    workflow = log.split("/JobCache/")[1].split('/')[0]
    numjobstart = int(array[-1])
    removereason = "UNDEFINED"
    if len(array) > 8: removereason = "DEFINED"    
    type = ''
    if log.count('Merge') > 0 :
        type = 'Merge'
    elif log.count('Cleanup') > 0 :
        type = 'Cleanup'
    elif log.count('LogCollect') > 0 :
        type = 'LogCollect'
    elif log.count('Production') > 0 :
        type = 'Production'
    elif log.count('MonteCarloFromGEN') > 0 :
                 type = 'Production'
    elif log.count('Processing') > 0 :
            type = 'Processing'
    else :
        type = 'Processing'
    if status == 2:
        increaseCounterInDict(overview_running,site,type)
        # print 'overview_running',overview_running
        time=ServerTime-JobStartDate
        if time > 172800 :
            increaseCounterInDict(overview_running48,site,type)
            fillIDWFinDict(jobs_48,site,workflow,id)
        if numjobstart > 3:
            increaseCounterInDict(overview_numjobstart,site,type)
            fillIDWFinDict(jobs_numjobstart,site,workflow,id)            
    elif status == 1:
        increaseCounterInDict(overview_pending,site,type)
    elif removereason == "DEFINED" :
        increaseCounterInDict(overview_removereason,site,type)
        fillIDWFinDict(jobs_removereason,site,workflow,id)
    else :
        increaseCounterInDict(overview_other,site,type)
        
        
printDict(overview_running,'Running')
print ""
printDict(overview_pending,'Pending')
print ""
if len(overview_running48.keys()) > 0:
    printDict(overview_running48,'Running > 48h')
    print ""
    sorted = jobs_48.keys()
    sorted.sort()
    print 'Jobs that run for > 48 hours by workflow:'
    print ""
    for site in sorted:
        print site + ':'
        print ''
        for wf in jobs_48[site].keys():
            print wf,':',' '.join(jobs_48[site][wf])
        print ""
        
print ""
if len(overview_removereason.keys()) > 0:
    printDict(overview_removereason,'Removed')
    print ""
    sorted = jobs_removereason.keys()
    sorted.sort()
    print 'Jobs with RemoveReason!=UNDEFINED'
    print ""
    for site in sorted:
        print site + ':'
        print ''
        for wf in jobs_removereason[site].keys():
            print wf,':',' '.join(jobs_removereason[site][wf])
        print ""

print ""
if len(overview_numjobstart.keys()) > 0:
    printDict(overview_numjobstart,'Restarted')
    print ""
    sorted = jobs_numjobstart.keys()
    sorted.sort()
    print 'Jobs with NumJobStart > 3'
    print ""
    for site in sorted:
        print site + ':'
        print ''
        for wf in jobs_numjobstart[site].keys():
            print wf,':',' '.join(jobs_numjobstart[site][wf])
        print ""
