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
from random import choice

try:
    import htcondor
except ImportError:
    print "You do not have a proper environment, please source the following:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(1)


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
            'Harvesting': 0,
            'Skim': 0,
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
    print '-' * 128
    print '| %-20s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s |' % (
        description, 'Processing', 'Production', 'Merge', 'Cleanup', 'LogCollect', 'Harvesting', 'Skim', 'Total')
    print '-' * 128
    total_processing = 0
    total_production = 0
    total_merge = 0
    total_cleanup = 0
    total_logcollect = 0
    total_harvest = 0
    total_skim = 0
    total = 0
    for site in sortedKeys:
        siteDict = jobDict[site]
        total_production += siteDict['Production']
        total_processing += siteDict['Processing']
        total_merge += siteDict['Merge']
        total_cleanup += siteDict['Cleanup']
        total_logcollect += siteDict['LogCollect']
        total_harvest += siteDict['Harvesting']
        total_skim += siteDict['Skim']
        total += siteDict['Processing']
        total += siteDict['Production']
        total += siteDict['Merge']
        total += siteDict['Cleanup']
        total += siteDict['LogCollect']
        total += siteDict['Harvesting']
        total += siteDict['Skim']
        total_site = siteDict['Processing'] + siteDict['Production'] + siteDict['Merge'] + siteDict['Cleanup'] + \
                     siteDict['LogCollect'] + siteDict['Harvesting'] + siteDict['Skim']
        print '| %-20s | %10d | %10d | %10d | %10d | %10d | %10d | %10d | %10d |' % (site,
                                                                                     siteDict['Processing'],
                                                                                     siteDict['Production'],
                                                                                     siteDict['Merge'],
                                                                                     siteDict['Cleanup'],
                                                                                     siteDict['LogCollect'],
                                                                                     siteDict['Harvesting'],
                                                                                     siteDict['Skim'],
                                                                                     total_site)
    print '-' * 128
    print '| %-20s | %10d | %10d | %10d | %10d | %10d | %10d | %10d | %10d |' % (
        'Total', total_processing, total_production, total_merge, total_cleanup, total_logcollect, total_harvest,
        total_skim, total)
    print '-' * 128


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
                                  'MachineAttrGLIDEIN_CMSSite0',
                                  'DESIRED_Sites',
                                  'CMS_JobType',
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
            runTime = job["ServerTime"] - job["JobStartDate"]
        else:
            runTime = 0

        # get task name
        taskname = job["WMAgent_SubTaskName"]

        # get Workflow from the taskName
        workflow = taskname.split("/")[1]

        # DesiredSite list
        sitelist = job["DESIRED_Sites"].split(",")
        if "MachineAttrGLIDEIN_CMSSite0" in job:
            site = job["MachineAttrGLIDEIN_CMSSite0"]
        else:
            site = choice(sitelist)

        # get number of job restarts
        numjobstart = job["NumJobStarts"]

        removereason = "UNDEFINED"
        if len(sitelist) > 1:
            removereason = "DEFINED"

        jobType = job["CMS_JobType"]

        maxWallTimeMins = job['MaxWallTimeMins']

        # IF Running
        if status == 2:
            increaseCounterInDict(overview_running, site, jobType)
            # if larger tan 48 hours
            if runTime > 48 * 3600:
                increaseCounterInDict(overview_running48, site, jobType)
                fillIDWFinDict(jobs_48, site, workflow, jobId)
            # if restarted more than 3 times
            if numjobstart > 3:
                increaseCounterInDict(overview_numjobstart, site, jobType)
                fillIDWFinDict(jobs_numjobstart, site, workflow, jobId)
        # if Pending
        elif status == 1:
            increaseCounterInDict(overview_pending, site, jobType)
            if maxWallTimeMins > 46 * 60:
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
