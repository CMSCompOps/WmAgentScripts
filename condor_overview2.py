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
import sys
import optparse
from random import choice
try:
    import htcondor
except ImportError:
    print "You do not have a proper environment, please source the following:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(1)

global_pool = 'cmsgwms-collector-global.cern.ch'

# production schedd's
schedds = ["vocms0304.cern.ch",
           "vocms0308.cern.ch",
           "vocms0309.cern.ch",
           "vocms0310.cern.ch",
           "vocms0311cern.ch",
           "cmssrv217.fnal.gov",
           "cmssrv218.fnal.gov",
           "cmssrv219.fnal.gov",
           "cmsgwms-submit1.fnal.gov",
           "cmsgwms-submit2.fnal.gov",
           ]


def increaseCounterInDict(jobDict, site, jobType):
    """
    increases the job count for the given site
    creates the site if not in jobDict
    """
    # print 'site',site,'jobType',jobType
    if site in jobDict:
        jobDict[site][jobType] += 1
    else:
        tmp = {
            'Processing': 0,
            'Production': 0,
            'Merge': 0,
            'Cleanup': 0,
            'LogCollect': 0,
        }
        jobDict[site] = tmp
        jobDict[site][jobType] += 1
    # print 'dict',dict


def fillIDinDict(jobDict, site, jobId):
    """
    creates one site on dictionary (a row)
    """
    if site not in jobDict:
        jobDict[site] = []
    jobDict[site].append(jobId)


def fillIDWFinDict(jobDict, site, workflow, jobId):
    """
    Adds one wf to dictionary
    creates row and column if not already
    """
    if site not in jobDict:
        jobDict[site] = {}
    if workflow not in jobDict[site]:
        jobDict[site][workflow] = []
    jobDict[site][workflow].append(jobId)


def printDict(jobDict, description):
    """
    format-prints dict contents
    """
    sortedKeys = sorted(jobDict)
    print '-' * 100
    print '| %-20s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s |' % (description, 'Processing', 'Production', 'Merge', 'Cleanup', 'LogCollect', 'Total')
    print '-' * 100
    total_processing = 0
    total_production = 0
    total_merge = 0
    total_cleanup = 0
    total_logcollect = 0
    total = 0
    for site in sortedKeys:
        siteDict = jobDict[site]
        total_production += siteDict['Production']
        total_processing += siteDict['Processing']
        total_merge += siteDict['Merge']
        total_cleanup += siteDict['Cleanup']
        total_logcollect += siteDict['LogCollect']
        total += siteDict['Processing']
        total += siteDict['Production']
        total += siteDict['Merge']
        total += siteDict['Cleanup']
        total += siteDict['LogCollect']
        total_site = siteDict['Processing'] + siteDict['Production'] + siteDict['Merge'] + siteDict['Cleanup']+siteDict['LogCollect']
        print '| %-20s | %10d | %10d | %10d | %10d | %10d | %10d |' % (site,
                                                                       siteDict['Processing'],
                                                                       siteDict['Production'],
                                                                       siteDict['Merge'],
                                                                       siteDict['Cleanup'],
                                                                       siteDict['LogCollect'],
                                                                       total_site)
    print '-' * 100
    print '| %-20s | %10d | %10d | %10d | %10d | %10d | %10d |' % ('Total', total_processing, total_production, total_merge, total_cleanup, total_logcollect, total)
    print '-' * 100


def get_overview(overview_running,
                 overview_pending,
                 overview_other,
                 overview_running48,
                 overview_numjobstart,
                 overview_removereason,
                 jobs_48,
                 jobs_maxwall,
                 jobs_numjobstart,
                 jobs_removereason,
                 schedd=None):
    """
    Gets a summary for one schedd
    """
    if not schedd:
        schedd = htcondor.Schedd()
    else:
        schedd = htcondor.Schedd(schedd)

    jobs = schedd.xquery("true", ['ClusterID',
                                  'ProcId',
                                  'JobStatus',
                                  'ServerTime',
                                  'JobStartDate',
                                  'WMAgent_SubTaskName',
                                  'MATCH_EXP_JOBGLIDEIN_CMSSite',
                                  'DESIRED_Sites',
                                  'NumJobStarts',
                                  'MaxWallTimeMins'])
    # split lines
    for job in jobs:
        # clusterID.ProcId (composed ID)
        jobId = "%s.%s" % (job["clusterID"], job["ProcId"])
        # other features
        status = job["JobStatus"]

        # ServerTime-JobStartDate
        if "JobStartDate" in job:
            RunTime = job["JobStatus"] - job["JobStartDate"]

        # get task name
        taskname = job["WMAgent_SubTaskName"]

        # get Workflow from the taskName
        workflow = taskname.split("/")[1]

        # DesiredSite list
        sitelist = job["DESIRED_Sites"].split(",")
        # if it has a MATCH_EXP_JOBGLIDEIN_CMSSite
        if "MATCH_EXP_JOBGLIDEIN_CMSSite" in job:
            site = job["MATCH_EXP_JOBGLIDEIN_CMSSite"]
        else:
            site = choice(sitelist)

        # get number of job restarts
        numjobstart = job["NumJobStarts"]

        removereason = "UNDEFINED"
        if len(sitelist) > 1:
            removereason = "DEFINED"

        jobType = ''
        # the last name
        name = taskname.split("/")[-1]
        # get jobType of job from TaskName Name
        if 'LogCollect' in name:
            jobType = 'LogCollect'
        elif 'Merge' in name:
            jobType = 'Merge'
        elif 'Cleanup' in name:
            jobType = 'Cleanup'
        elif 'Production' in name:
            jobType = 'Production'
        elif 'MonteCarloFromGEN' in name:
            jobType = 'Production'
        elif 'Processing' in name or 'Proc' in name:
            jobType = 'Processing'
        else:
            jobType = 'Processing'
        
        if 'MaxWallTimeMins' in job:
            maxWallTimeMins = job['MaxWallTimeMins']
        else:
            maxWallTimeMins = False
        # IF Running
        if status == 2:
            increaseCounterInDict(overview_running, site, jobType)
            # if larger tan 48 hours
            if RunTime > 48 * 3600:
                increaseCounterInDict(overview_running48, site, jobType)
                fillIDWFinDict(jobs_48, site, workflow, jobId)
            # if restarted more than 3 times
            if numjobstart > 3:
                increaseCounterInDict(overview_numjobstart, site, jobType)
                fillIDWFinDict(jobs_numjobstart, site, workflow, jobId)
        # if Pending
        elif status == 1:
            increaseCounterInDict(overview_pending, site, jobType)
            #check maxWallTime greater than 24 hours
            if (maxWallTimeMins and maxWallTimeMins > 46*60) or not maxWallTimeMins:
                fillIDWFinDict(jobs_maxwall, site, workflow, jobId)
                
        # if not running or pending, and reason is DEFINED
        elif removereason == "DEFINED":
            increaseCounterInDict(overview_removereason, site, jobType)
            fillIDWFinDict(jobs_removereason, site, workflow, jobId)
        # if reason UNDEFINED
        else:
            increaseCounterInDict(overview_other, site, jobType)


def print_results(overview_running,
                  overview_pending,
                  overview_running48,
                  overview_numjobstart,
                  overview_removereason,
                  jobs_48,
                  jobs_maxwall,
                  jobs_numjobstart,
                  jobs_removereason):
    """
    Shows results in nice console tables
    """
    printDict(overview_running, 'Running')
    print ""
    printDict(overview_pending, 'Pending')
    print ""
    if overview_running48:
        printDict(overview_running48, 'Running > 48h')
        print ""
        sortKeys = sorted(jobs_48)
        print 'Jobs that run for > 48 hours by workflow:'
        print ""
        for site in sortKeys:
            print site + ':'
            print ""
            for wf, jobs in jobs_48[site].items():
                print wf, ':', ' '.join(jobs)
            print ""

    print ""
    if jobs_maxwall:
        sortKeys = sorted(jobs_maxwall)
        print 'Jobs that have MaxWall > 46 hours (or empty) by workflow:'
        print ""
        for site in sortKeys:
            print site + ':'
            print ""
            for wf, jobs in jobs_maxwall[site].items():
                print wf, ':', ' '.join(jobs)
            print ""

    print ""
    if overview_removereason:
        printDict(overview_removereason, 'Removed')
        print ""
        sortKeys = sorted(jobs_removereason)
        print 'Jobs with RemoveReason!=UNDEFINED'
        print ""
        for site in sortKeys:
            print site + ':'
            print ''
            for wf, jobs in jobs_removereason[site].items():
                print wf, ':', ' '.join(jobs)
            print ""

    print ""
    if overview_numjobstart:
        printDict(overview_numjobstart, 'Restarted')
        print ""
        sortKeys = sorted(jobs_numjobstart)
        print 'Jobs with NumJobStart > 3'
        print ""
        for site in sortKeys:
            print site + ':'
            print ''
            for wf, jobs in jobs_numjobstart[site].items():
                print wf, ':', ' '.join(jobs)
            print ""
    

def main():
    parser = optparse.OptionParser()
    parser.add_option('-g', '--global', action='store_true',  dest='printall', default=False, help='Print overview from all production schedds')
    (options, args) = parser.parse_args()

    # Data dictionaries
    overview_running = {}
    overview_pending = {}
    overview_other = {}
    overview_running48 = {}
    overview_numjobstart = {}
    overview_removereason = {}
    jobs_48 = {}
    jobs_maxwall = {}
    jobs_numjobstart = {}
    jobs_removereason = {}

    if(options.printall):
        # global pool collector
        coll = htcondor.Collector(global_pool)
        schedd_ads = coll.query(htcondor.AdTypes.Schedd, 'CMSGWMS_Type=?="prodschedd"', ['Name', 'MyAddress', 'ScheddIpAddr'])
  
        # all schedds
        for ad in schedd_ads:
            if ad["Name"] not in schedds:
                continue
            print "getting jobs from %s"%ad["Name"]
            #fill the overview
            get_overview(overview_running,
                         overview_pending,
                         overview_other,
                         overview_running48,
                         overview_numjobstart,
                         overview_removereason,
                         jobs_48,
                         jobs_maxwall,
                         jobs_numjobstart,
                         jobs_removereason,
                         ad)

    else:
        # fill the overview
        get_overview(overview_running,
                     overview_pending,
                     overview_other,
                     overview_running48,
                     overview_numjobstart,
                     overview_removereason,
                     jobs_48,
                     jobs_maxwall,
                     jobs_numjobstart,
                     jobs_removereason,
                     schedd=None)

    print_results(overview_running,
                  overview_pending,
                  overview_running48,
                  overview_numjobstart,
                  overview_removereason,
                  jobs_48,
                  jobs_maxwall,
                  jobs_numjobstart,
                  jobs_removereason)

if __name__ == '__main__':
    main()
