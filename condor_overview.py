#!/usr/bin/env python
"""
    Condor Overview:
    Script to summarize output from condorq,
    Shows:
    - a table of running jobs: type of job-task vs. site.
    - a table of pending jobs: type of job-task vs. site
    - jobs that have run for more than 24 hours
    - jobs that have restarted more than 3 times
    
"""
import sys,os
import subprocess
from random import choice
def increaseCounterInDict(dict,site,type):
    """
    increases the job count for the given site
    creates the site if not in dict
    """
    # print 'site',site,'type',type
    if site in dict:
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
    """
    creates one site on dictionary (a row)
    """
    if site not in dict:
        dict[site] = []
    dict[site].append(id)

def fillIDWFinDict(dict,site,workflow,id):
    """
    Adds one wf to dictionary
    creates row and column if not already
    """
    if site not in dict:
        dict[site] = {}
    if workflow not in dict[site]:
        dict[site][workflow] = []
    dict[site][workflow].append(id)

def printDict(dict,description):
    """
    format-prints dict contents
    """
    sortedKeys = sorted(dict.keys())
    print '----------------------------------------------------------------------------------------------------'
    print '| %20s | Processing | Production | Merge      | Cleanup    | LogCollect | Total      |' % description
    print '----------------------------------------------------------------------------------------------------'
    total_processing = 0
    total_production = 0
    total_merge = 0
    total_cleanup = 0
    total_logcollect = 0
    total = 0
    for site in sortedKeys:
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

def main():
    #Data dictionaries
    overview_running = {}
    overview_pending = {}
    overview_other = {}
    overview_running48 = {}
    overview_numjobstart = {}
    overview_removereason = {}
    jobs_48 = {}
    jobs_numjobstart = {}
    jobs_removereason = {}

    #previous commands
    #command='condor_q -format "%i." ClusterID -format "%s " ProcId -format " %i " JobStatus  -format " %d " ServerTime-EnteredCurrentStatus -format "%s" UserLog -format " %s" DESIRED_Sites -format " %s" RemoveReason -format " %i\n" NumJobStarts'
    #command="""condor_q -format "%i." ClusterID -format "%s " ProcId -format " %i " JobStatus  -format " %d " ServerTime-JobStartDate -format "%s" UserLog -format " %s" DESIRED_Sites -format " %s" RemoveReason -format " %i\n" NumJobStarts | awk '{if ($2!= 1) print $0}'"""

    #condor_q command
    command="""condor_q -format "%i." ClusterID -format "%s " ProcId -format "%i " JobStatus  -format "%d " ServerTime -format "[%d] " JobStartDate -format "%s " UserLog -format "[%s] " MATCH_EXP_JOBGLIDEIN_CMSSite -format "%s " DESIRED_Sites -format "%i\n" NumJobStarts"""
    proc = subprocess.Popen(command, stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
    out, err = proc.communicate()
    #split lines
    for line in out.split('\n') :
        if line == "" : continue
        array = line.split()
        i = 0
        #clusterID.ProcId (composed ID)
        id = array[i]; i+=1
        #JobStatus
        status = int(array[i]); i+=1
        #ServerTime
        ServerTime=int(array[i]); i+=1

        #if it has the JobStartDate
        if array[i].startswith('['):
            #JobStartDate (delete the [])
            JobStartDate=int(array[i][1:-1]); i+=1
        
        if 'sleep' in line:
            print line
            continue

        #get log name
        log = array[i]; i+=1
        #get Workflow name from the LOG
        workflow = log.split("/JobCache/")[1].split('/')[0]
        
        #if it has a MATCH_EXP_JOBGLIDEIN_CMSSite
        if array[i].startswith('['):
            site = array[i][1:-1]; i+=1
        else:
            site = 'UNKNOWN'
        
        #site list from i to previous to -1
        #this is to 1. Join site list, 2. remove [], then split by comma
        sitelist = ''.join(array[i:-1]).split(',')
        
        if site == 'UNKNOWN' and len(sitelist) == 1:
            site = sitelist[0]
        elif site == 'UNKNOWN' and len(sitelist) > 1:
            site = 'MULTIPLE'
            #pick random from whitelist
            site = choice(sitelist)
            
        #get number of job restarts
        numjobstart = int(array[-1])
        removereason = "UNDEFINED"

        if len(sitelist) > 1: removereason = "DEFINED"    
        type = ''
        #get type of WF from Log Name
        if 'Merge' in log:
            type = 'Merge'
        elif 'Cleanup' in log:
            type = 'Cleanup'
        elif 'LogCollect' in log:
            type = 'LogCollect'
        elif 'Production' in log:
            type = 'Production'
        elif 'MonteCarloFromGEN' in log:
            type = 'Production'
        elif 'Processing' in log:
            type = 'Processing'
        else :
            type = 'Processing'

        # IF Running
        if status == 2:
            increaseCounterInDict(overview_running,site,type)
            # How much time has run
            time=ServerTime-JobStartDate

            #if larger tan 48 hours
            if time > 172800 :
                increaseCounterInDict(overview_running48,site,type)
                fillIDWFinDict(jobs_48,site,workflow,id)
            #if restarted more than 3 times
            if numjobstart > 3:
                increaseCounterInDict(overview_numjobstart,site,type)
                fillIDWFinDict(jobs_numjobstart,site,workflow,id)
        #if Pending
        elif status == 1:
            increaseCounterInDict(overview_pending,site,type)
        # if not running or pending, and reason is DEFINED
        elif removereason == "DEFINED" :
            increaseCounterInDict(overview_removereason,site,type)
            fillIDWFinDict(jobs_removereason,site,workflow,id)
        # if reason UNDEFINED
        else :
            increaseCounterInDict(overview_other,site,type)
            
    #print results
    printDict(overview_running,'Running')
    print ""
    printDict(overview_pending,'Pending')
    print ""

    if overview_running48:
        printDict(overview_running48,'Running > 48h')
        print ""
        sortKeys = sorted(jobs_48)
        print 'Jobs that run for > 48 hours by workflow:'
        print ""
        for site in sortKeys:
            print site + ':'
            print ""
            for wf in jobs_48[site].keys():
                print wf,':',' '.join(jobs_48[site][wf])
            print ""
            
    print ""
    if overview_removereason:
        printDict(overview_removereason,'Removed')
        print ""
        sortKeys = sorted(jobs_removereason.keys())
        print 'Jobs with RemoveReason!=UNDEFINED'
        print ""
        for site in sortKeys:
            print site + ':'
            print ''
            for wf in jobs_removereason[site].keys():
                print wf,':',' '.join(jobs_removereason[site][wf])
            print ""

    print ""
    if overview_numjobstart:
        printDict(overview_numjobstart,'Restarted')
        print ""
        sortKeys = sorted(jobs_numjobstart.keys())
        print 'Jobs with NumJobStart > 3'
        print ""
        for site in sortKeys:
            print site + ':'
            print ''
            for wf in jobs_numjobstart[site].keys():
                print wf,':',' '.join(jobs_numjobstart[site][wf])
            print ""


if __name__ == '__main__':
    main()
