#!/usr/bin/env python
"""
This is for counting jobs from each condor schedd
Creates the following files: CondorMonitoring.json, CondorJobs_Workflows.json, Running*.txt and Pending*.txt ( * in types )
"""

import sys,os,re,urllib,urllib2,subprocess,time,smtplib,os
import htcondor as condor
from datetime import datetime
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
try:
    import json
except ImportError:
    import simplejson as json

# Mailing list for notifications
mailingSender = 'noreply@cern.ch'
mailingList = ['luis89@fnal.gov','dmason@fnal.gov','alan.malta@cern.ch']

## Job Collectors (Condor pools)
#global_pool = ['vocms097.cern.ch']
global_pool = ['vocms099.cern.ch']
tier0_pool = ['vocms007.cern.ch']

##The following groups should be updated according to https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsWorkflowTeamWmAgentRealeases
relvalAgents = ['vocms053.cern.ch']
testAgents = ['cmssrv113.fnal.gov', 'vocms040.cern.ch', 'vocms009.cern.ch', 'vocms0224.cern.ch', 'vocms0230.cern.ch']

##Job expected types
jobTypes = ['Processing', 'Production', 'Skim', 'Harvest', 'Merge', 'LogCollect', 'Cleanup', 'RelVal', 'Express', 'Repack', 'Reco']

## Job counting / Condor monitoring
baseSiteList = {} # Site list
baseSitePledges = {} # Site pledges list
jobCounting = {} # Actual job counting
pendingCache = [] # pending jobs cache
totalRunningSite = {} # Total running per site
jobs_failedTypeLogic = {} # Jobs that failed the type logic assignment
output_json = "CondorMonitoring.json" # Output json file name
##Counting jobs for Workflows
overview_workflows = {}
json_name_workflows = "CondorJobs_Workflows.json" # Output json file name

##SSB links
site_link = "http://dashb-ssb.cern.ch/dashboard/templates/sitePendingRunningJobs.html?site="
overalls_link = "http://dashb-ssb-dev.cern.ch/dashboard/templates/sitePendingRunningJobs.html?site=All%20"
url_site_status = 'http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=158&batch=1&lastdata=1'
url_site_pledges = 'http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=159&batch=1&lastdata=1'

def createSiteList():
    """
    Creates a initial site list with the data from site status in Dashboard
    """
    sites = urllib2.urlopen(url_site_status).read()
    try:
        site_status = json.read(sites)['csvdata']
    except:
        site_status = json.loads(sites)['csvdata']
    
    for site in site_status:
        name = site['VOName']
        status = site['Status']
        if siteName(name):
            baseSiteList[name] = status

def getSitePledge():
    """
    Get the expected pledge to use from Dashboard
    """
    sites = urllib2.urlopen(url_site_pledges).read()
    try:
        site_pledges = json.read(sites)['csvdata']
    except:
        site_pledges = json.loads(sites)['csvdata']
    
    for site in site_pledges:
        name = site['VOName']
        if site['Value'] == None:
            value = 0
        else:
            value = int(site['Value'])
        if siteName(name):
            baseSitePledges[name] = value

def initJobDictonaries():
    """
    Init running/pending jobs for each site in the baseSiteList
    """
    for site in baseSiteList.keys():
        if '_Disk' not in site: # Avoid _Disk suffixes
            jobCounting[site] = {}

def siteName(candidate):
    """
    Check candidate as a site name. Should pass:
        T#_??_*
    Returns True if it is a site name
    """
    regexp = "^T[0-3%](_[A-Z]{2}(_[A-Za-z0-9]+)*)$"
    if re.compile(regexp).match(candidate) != None:
        return True
    else:
        return False

def addSite(site):
    """
    Add a site to all the dictionaries
    """
    print "DEBUG: Adding site %s to base lists" % site
    if site not in jobCounting.keys():
        jobCounting[site] = {}
    if site not in baseSiteList.keys():
        baseSiteList[site] = 'on'

def addSchedd(site,sched):
    """
    Add a schedd to all the dictionaries for a given site
    """
    if sched not in jobCounting[site].keys():
        jobCounting[site][sched] = {}
        for type in jobTypes:
            jobCounting[site][sched][type] = {}
    
def addCore(site,sched,type,cores):
    """
    Add a Number of cores for a given site, schedd and type to all the dictionaries
    """
    if cores not in jobCounting[site][sched][type].keys():
        jobCounting[site][sched][type][cores] = {}
        for status in ['Running', 'Pending']:
            jobCounting[site][sched][type][cores][status] = 0.0
    
def increaseRunning(site,sched,type,cores):
    """
    Increase the number of running jobs for the given site, schedd, type and cores
    This always increase job count by 1
    """
    if site not in jobCounting.keys():
        addSite(site)
    if sched not in jobCounting[site].keys():
        addSchedd(site,sched)
    if cores not in jobCounting[site][sched][type].keys():
        addCore(site,sched,type,cores)
    #Now do the actual counting
    jobCounting[site][sched][type][cores]['Running'] += 1
        
def increasePending(site,sched,type,cores,num):
    """
    Increase the number of pending jobs for the given site and type
    This handles smart counting: sum the relative pending 'num'
    """
    if site not in jobCounting.keys():
        addSite(site)
    if sched not in jobCounting[site].keys():
        addSchedd(site,sched)
    if cores not in jobCounting[site][sched][type].keys():
        addCore(site,sched,type,cores)
    #Now do the actual counting
    jobCounting[site][sched][type][cores]['Pending'] += num

def increaseRunningWorkflow(workflow,siteToExtract,cores):
    """
    Increases the number of running jobs per workflow
    """
    if workflow not in overview_workflows.keys():
        addWorkflow(workflow)
        if siteToExtract in overview_workflows[workflow]['runningJobs'].keys():
            overview_workflows[workflow]['runningJobs'][siteToExtract] += cores
            overview_workflows[workflow]['condorJobs'] += cores
        else:
            overview_workflows[workflow]['runningJobs'][siteToExtract] = cores
            overview_workflows[workflow]['condorJobs'] += cores
    else:
        if siteToExtract in overview_workflows[workflow]['runningJobs'].keys():
            overview_workflows[workflow]['runningJobs'][siteToExtract] += cores
            overview_workflows[workflow]['condorJobs'] += cores
        else:
            overview_workflows[workflow]['runningJobs'][siteToExtract] = cores
            overview_workflows[workflow]['condorJobs'] += cores
    
def increasePendingWorkflow(workflow,siteToExtract,cores):
    """
    Increases the number of pending jobs per workflow
    """
    if workflow not in overview_workflows.keys():
        addWorkflow(workflow)
        overview_workflows[workflow]['condorJobs'] += cores
        overview_workflows[workflow]['pendingJobs'] += cores
        overview_workflows[workflow]['desiredSites'] = overview_workflows[workflow]['desiredSites'].union(set(siteToExtract))
    else:
        overview_workflows[workflow]['condorJobs'] += cores
        overview_workflows[workflow]['pendingJobs'] += cores
        overview_workflows[workflow]['desiredSites'] = overview_workflows[workflow]['desiredSites'].union(set(siteToExtract))

def addWorkflow(workflow):
    """
    Add a new workflow to overview_workflows
    """
    overview_workflows[workflow] = {
                                    'condorJobs' : 0,
                                    'runningJobs' : {},
                                    'pendingJobs' : 0,
                                    'desiredSites' : set()
                                    }

def jobType(id,schedd,typeToExtract):
    """
    This deduces job type from given info about scheduler and taskName
    Only intended as a backup in case job type from the classAds is not standard
    """
    type = ''
    if schedd in relvalAgents:
        type = 'RelVal'
    elif 'Cleanup' in typeToExtract:
        type = 'Cleanup'
    elif 'LogCollect' in typeToExtract:
        type = 'LogCollect'
    elif 'harvest' in typeToExtract.lower():
        type = 'Harvest'
    elif 'Merge' in typeToExtract:
        type = 'Merge'
    elif 'skim' in typeToExtract.lower():
        type = 'Skim'
    elif 'Express' in typeToExtract:
        type = 'Express'
    elif 'Repack' in typeToExtract:
        type = 'Repack'
    elif 'Reco' in typeToExtract:
        type = 'Reco'
    elif 'Production' in typeToExtract or 'MonteCarloFromGEN' in typeToExtract:
        type = 'Production'
    elif any([x in typeToExtract for x in ['Processing','StepOneProc','StepTwoProc','StepThreeProc']]):
        type = 'Processing'
    elif 'StoreResults' in typeToExtract:
        type = 'Merge'
    elif schedd in testAgents:
        type = 'Processing'
    else:
        type = 'Processing'
        jobs_failedTypeLogic[id]=dict(scheduler = schedd, BaseType = typeToExtract)
    return type

def relativePending(siteToExtract):
    """
    Return the remaining slots available (in principle) to run jobs for the given sites
    If there is no slots available, ruturn the same value for all the given (same chance to run)
    """
    relative = {}
    total = 0.0
    for site in siteToExtract:
        if site in totalRunningSite.keys():
            running = totalRunningSite[site]
        else:
            running = 0.0
        if site in baseSitePledges.keys():
            pledge = baseSitePledges[site]
        else:
            pledge = 0.0
        
        relative[site] = pledge - running
        if relative[site] < 0.0:
            relative[site] = 0.0
        total += relative[site]
    
    # if total = 0, it means that there is not available slots for any site, set the same for all sites
    if total == 0.0:
        total = len(siteToExtract)
        for site in relative.keys():
            relative[site] = 1.0
    
    return relative, total
    
def getJobsOveralls():
    """
    This creates the overall job counting by site and server
    """
    totalBySite = {}
    totalByServer = {}
    totalByTask = {}
    totalJobs = {}
    
    totalJobs = {
                 'Running' : 0.0,
                 'Pending' : 0.0
                 }
    for task in jobTypes:
        totalByTask[task] = {
                             'Running' : 0.0,
                             'Pending' : 0.0
                             }
    for site in jobCounting.keys():
        #Add site to the overalls by site, then add each tasks
        totalBySite[site] = {
                             'Running' : 0.0,
                             'Pending' : 0.0
                             }
        for task in jobTypes:
            totalBySite[site][task] = {
                                       'Running' : 0.0,
                                       'Pending' : 0.0
                                       }
            
        for schedd in jobCounting[site].keys():
            #If schedd is not in totalByServer, then add it
            if not schedd in totalByServer.keys():
                totalByServer[schedd] = {
                                         'Running' : 0.0,
                                         'Pending' : 0.0
                                         }
                for task in jobTypes:
                    totalByServer[schedd][task] = {
                                                   'Running' : 0.0,
                                                   'Pending' : 0.0
                                                   }
            for type in jobCounting[site][schedd].keys():
                for ncore in jobCounting[site][schedd][type].keys():
                    
                    run_jobs = jobCounting[site][schedd][type][ncore]['Running']
                    pen_jobs = jobCounting[site][schedd][type][ncore]['Pending']
                    #Add to total by sites
                    totalBySite[site][type]['Running'] += run_jobs
                    totalBySite[site][type]['Pending'] += pen_jobs
                    totalBySite[site]['Running'] += run_jobs
                    totalBySite[site]['Pending'] += pen_jobs
                    #Add to total by servers
                    totalByServer[schedd][type]['Running'] += run_jobs
                    totalByServer[schedd][type]['Pending'] += pen_jobs
                    totalByServer[schedd]['Running'] += run_jobs
                    totalByServer[schedd]['Pending'] += pen_jobs
                    #Add to total by Task
                    totalByTask[type]['Running'] += run_jobs
                    totalByTask[type]['Pending'] += pen_jobs
                    #Add to total jobs
                    totalJobs['Running'] += run_jobs
                    totalJobs['Pending'] += pen_jobs
    
    return totalBySite, totalByServer, totalByTask, totalJobs
    
def createReports(currTime):
    """
    1. Prints a report for the given dictionary
    2. Creates a text file to feed each column in SSB Running/Pending view
    3. Creates the output json file to feed SSB historical view
    4. Creates workflow overview json
    """
    date = currTime.split('h')[0]
    hour = currTime.split('h')[1]
    
    totalBySite, totalByServer, totalByTask, totalJobs = getJobsOveralls()
    
    sites = totalBySite.keys()
    servers = totalByServer.keys()
    sites.sort()
    servers.sort()
    
    #Init output files (txt)
    for status in ['Running', 'Pending']:
        for type in jobTypes:
            file = open('./'+status+type+'.txt', 'w+')
            file.close()
        file = open('./'+status+"Total"+'.txt', 'w+')
        file.close()
    
    for status in ['Running', 'Pending']:
        
        #Print header report to stdout
        title_line = "| %25s |" % status
        aux_line = "| %25s |" % ('-'*25)
        for type in jobTypes:
            title_line += " %10s |" % type
            aux_line += " %10s |" % ('-'*10)
        title_line += " %10s |" % 'Total'
        aux_line += " %10s |" % ('-'*10)
        print aux_line, '\n', title_line, '\n', aux_line
        
        #Fill output files with all the SITES info. Also print out reports to stdout
        for site in sites: 
            site_line = "| %25s |" % site
            for type in jobTypes:
                typeJobs = int(totalBySite[site][type][status])
                site_line += " %10s |" % typeJobs
                
                file = open('./'+status+type+'.txt', 'a')
                file.write( "%s %s\t%s\t%s\t%s\t%s%s\n" % (date, hour, site, str(typeJobs), 'green', site_link, site ))
                
            siteJobs = int(totalBySite[site][status])
            site_line += " %10s |" % siteJobs
            
            file = open('./'+status+"Total"+'.txt', 'a')
            file.write( "%s %s\t%s\t%s\t%s\t%s%s\n" % (date, hour, site, str(siteJobs), 'green', site_link, site ))
            
            print site_line
        overalls_line = "| %25s |" % 'Overalls'
        for type in jobTypes:
            totalTypeJobs = int(totalByTask[type][status])
            overalls_line += " %10s |" % totalTypeJobs
            
            file = open('./'+status+type+'.txt', 'a')
            file.write( "%s %s\t%s%s\t%s\t%s\t%s%s\n" % (date, hour, 'Overall', 'Sites', str(totalTypeJobs), 'green' , overalls_link, 'T3210' ))
            
        totalJobsTable = int(totalJobs[status])
        overalls_line += " %10s |" % totalJobsTable
        file = open('./'+status+"Total"+'.txt', 'a')
        file.write( "%s %s\t%s%s\t%s\t%s\t%s%s\n" % (date, hour, 'Overall', 'Sites', str(totalJobsTable), 'green', overalls_link, 'T3210' ))
        print aux_line, '\n', overalls_line, '\n', aux_line, '\n'
        
        #Fill output files with all the SERVERS info. Also print out reports to stdout
        print aux_line, '\n', title_line, '\n', aux_line
        for server in servers: 
            site_line = "| %25s |" % server
            for type in jobTypes:
                typeJobs = int(totalByServer[server][type][status])
                site_line += " %10s |" % typeJobs
                
                file = open('./'+status+type+'.txt', 'a')
                file.write( "%s %s\t%s\t%s\t%s\t%s%s%s\n" % (date, hour, server, str(typeJobs), 'green', site_link, server, '&server' ))
                
            siteJobs = int(totalByServer[server][status])
            site_line += " %10s |" % siteJobs
            
            file = open('./'+status+"Total"+'.txt', 'a')
            file.write( "%s %s\t%s\t%s\t%s\t%s%s%s\n" % (date, hour, server, str(siteJobs), 'green', site_link, server, '&server' ))
            
            print site_line
        
        overalls_line = "| %25s |" % 'Overalls'
        for type in jobTypes:
            totalTypeJobs = int(totalByTask[type][status])
            overalls_line += " %10s |" % totalTypeJobs
            
            file = open('./'+status+type+'.txt', 'a')
            file.write( "%s %s\t%s%s\t%s\t%s\t%s%s\n" % (date, hour, 'Overall', 'Servers', str(totalTypeJobs), 'green' , overalls_link, 'Servers' ))
            
        totalJobsTable = int(totalJobs[status])
        overalls_line += " %10s |" % totalJobsTable
        file = open('./'+status+"Total"+'.txt', 'a')
        file.write( "%s %s\t%s%s\t%s\t%s\t%s%s\n" % (date, hour, 'Overall', 'Servers', str(totalJobsTable), 'green', overalls_link, 'Servers' ))
        print aux_line, '\n', overalls_line, '\n', aux_line, '\n'
    
    #Create output json file
    jsonCounting = {"UPDATE" : {"TimeDate" : currTime}, "Sites" : []}
    for site in jobCounting.keys():
        siteInfo = {}
        siteInfo["Site"] = site
        siteInfo["Running"] = 0.0
        siteInfo["Pending"] = 0.0
        siteInfo["Servers"] = []
        for server in jobCounting[site].keys():
            serverInfo = {}
            serverInfo["Server"] = server
            serverInfo["Running"] = 0.0
            serverInfo["Pending"] = 0.0
            serverInfo["Types"] = []
            for type in jobCounting[site][server].keys():
                typeInfo = {}
                typeInfo["Type"] = type
                typeInfo["Running"] = 0.0
                typeInfo["Pending"] = 0.0
                typeInfo["NCores"] = []
                for core in jobCounting[site][server][type].keys():
                    coreInfo = {}
                    coreInfo["Cores"] = core
                    coreInfo["Running"] = jobCounting[site][server][type][core]["Running"]
                    coreInfo["Pending"] = jobCounting[site][server][type][core]["Pending"]
                    
                    typeInfo["NCores"].append(coreInfo)
                    typeInfo["Running"] += coreInfo["Running"]
                    typeInfo["Pending"] += coreInfo["Pending"]
                
                serverInfo["Types"].append(typeInfo)
                serverInfo["Running"] += typeInfo["Running"]
                serverInfo["Pending"] += typeInfo["Pending"]
            
            siteInfo["Servers"].append(serverInfo)
            siteInfo["Running"] += serverInfo["Running"]
            siteInfo["Pending"] += serverInfo["Pending"]
        
        jsonCounting["Sites"].append(siteInfo)
    
    #Write the output json
    jsonfileJobs = open(output_json,'w+')
    jsonfileJobs.write(json.dumps(jsonCounting,sort_keys=True, indent=9))
    jsonfileJobs.close()
    
    # Creates json file for jobs per workflow
    jsonfileWorkflows = open(json_name_workflows,'w+')
    jsonfileWorkflows.write(json.dumps(overview_workflows, default=set_default, sort_keys=True, indent=4))
    jsonfileWorkflows.close()

def send_mail(send_from, send_to, subject, text, files=[], server="localhost"):
    """
    Method to send emails
    """
    assert isinstance(send_to, list)
    assert isinstance(files, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

def set_default(obj):
    """
    JSON enconder doesn't support sets, parse them to lists
    """
    if isinstance(obj, set):
        return list(obj)
    raise TypeError

def main():
    """
    Main algorithm
    """
    starttime=datetime.now()
    print 'INFO: Script started on: ', starttime
    
    #get time (date and hour)
    currTime = time.strftime("%Y-%m-%dh%H:%M:%S")
    
    #Create base dictionaries for running/pending jobs per site
    createSiteList() # Sites from SSB
    getSitePledge() # Get pledges by site from SSB
    initJobDictonaries() # Init running/pending dictionaries
    
    #Going through each collector and process a job list for each scheduler
    all_collectors = global_pool + tier0_pool
    for collector_name in all_collectors:
        
        print "INFO: Querying collector %s" % collector_name
        
        schedds={}
        
        collector = condor.Collector( collector_name )
        scheddAds = collector.locateAll( condor.DaemonTypes.Schedd )
        for ad in scheddAds:
            schedds[ad['Name']] = dict(schedd_type=ad.get('CMSGWMS_Type', ''),
                                       schedd_ad=ad)
        
        print "DEBUG: Schedulers ", schedds.keys()
        
        for schedd_name in schedds:
            
            if schedds[schedd_name]['schedd_type'] != 'prodschedd': #Only care about production Schedds
                continue
            
            print "INFO: Getting jobs from collector: %s scheduler: %s" % (collector_name, schedd_name)
            
            schedd_ad = schedds[schedd_name]['schedd_ad']
            schedd = condor.Schedd( schedd_ad )
            
            jobs = schedd.xquery( 'true', ['ClusterID', 'ProcId', 'JobStatus', 
                                           'CMS_JobType', 'WMAgent_SubTaskName',
                                           'RequestCpus', 'DESIRED_Sites', 
                                           'MATCH_EXP_JOBGLIDEIN_CMSSite'] )
            
            for job in jobs:
                
                id = str(job['ClusterID']) + '.' + str(job['ProcId'])
                status = int(job['JobStatus'])
                
                if 'WMAgent_SubTaskName' not in job:
                    print 'I found a job not coming from WMAgent: %s' % id
                    continue
                    
                workflow = job['WMAgent_SubTaskName'].split('/')[1]
                task = job['WMAgent_SubTaskName'].split('/')[-1]
                type = job['CMS_JobType']
                cpus = int(job['RequestCpus'])
                siteToExtract = str(job['DESIRED_Sites']).replace(' ', '').split(",")
                
                
                if schedd_name in relvalAgents: #If RelVal job
                    type = 'RelVal'
                elif task == 'Reco': #If PromptReco job (Otherwise type is Processing)
                    type = 'Reco'
                elif type not in jobTypes: #If job type is not standard
                    type = jobType(id,schedd_name,task)
                
                siteRunning = job.get('MATCH_EXP_JOBGLIDEIN_CMSSite', '')
                if siteName(siteRunning): # If job is currently running
                    siteToExtract = [siteRunning]
                
                # Ignore jobs to the T1s from the Tier-0 pool
                # Avoid double accounting with the global pool
                if collector_name == tier0_pool[0]: 
                    if not 'T2_CH_CERN_T0' in siteToExtract:
                        continue
                
                # Ignore jobs to T2_CH_CERN_T0 from the global pool
                # Avoid double accounting with the Tier-0 pool
                if collector_name == global_pool[0]: 
                    if 'T2_CH_CERN_T0' in siteToExtract:
                        continue
                
                if status == 2: #Running
                    increaseRunning(siteToExtract[0],schedd_name,type,cpus)
                    increaseRunningWorkflow(workflow,siteToExtract[0],1)
                elif status == 1: #Pending
                    pendingCache.append([schedd_name,type,cpus,siteToExtract])
                    increasePendingWorkflow(workflow,siteToExtract,1)
                else: #Ignore jobs in another state
                    continue
    print "INFO: Querying Schedds for this collector is done"
    
    # Get total running
    for site in jobCounting.keys():
        totalRunningSite[site] = 0.0
        for schedd in jobCounting[site].keys():
            for type in jobCounting[site][schedd].keys():
                for ncore in jobCounting[site][schedd][type].keys():
                    totalRunningSite[site] += jobCounting[site][schedd][type][ncore]['Running']
    
    # Now process pending jobs
    for job in pendingCache:
        job_schedd = job[0]
        type = job[1]
        cpus = job[2]
        siteToExtract = []
        for site in job[3]:
            siteToExtract.append(site.replace('_Disk',''))
        siteToExtract = list(set(siteToExtract)) #remove duplicates
        
        relative, total = relativePending(siteToExtract) # total != 0 always
        for penSite in siteToExtract:
            relative_pending = relative[penSite]/total # calculate relative pending weight
            increasePending(penSite, job_schedd, type, cpus, relative_pending)
    print "INFO: Smart pending job counting is done \n"
    
    # Handling jobs that failed task extraction logic
    if jobs_failedTypeLogic != {}:      
        body_text = 'A job type is unknown for the JobCounter script\n'
        body_text += 'Please have a look to the following jobs:\n\n %s'% str(jobs_failedTypeLogic)
        send_mail(mailingSender,
                  mailingList,
                  '[Condor Monitoring] Failed task type logic problem',
                  body_text)
        print 'ERROR: I find jobs that failed the type assignment logic, I will send an email to: %s' % str(mailingList)
    
    print 'INFO: Creating reports...'
    createReports(currTime)
    
    print 'INFO: The script has finished after: ', datetime.now()-starttime
    
if __name__ == "__main__":
    main()
