#!/usr/bin/env python
"""
This is for counting jobs from each condor schedd
Creates the following files: CondorMonitoring.json, CondorJobs_Workflows.json, Running*.txt and Pending*.txt ( * in types )
"""

import sys,os,re,urllib,urllib2,subprocess,time,smtplib,os
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
mailingList = ['luis89@fnal.gov','dmason@fnal.gov']

## Job Collectors (Condor pools)
global_pool = ['vocms097.cern.ch']
tier0_pool = ['vocms007.cern.ch']

## The following machines should be ignored (Crab Schedulers)
crab_scheds = ['vocms83.cern.ch','stefanovm.cern.ch']

##The following groups should be updated according to https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsWorkflowTeamWmAgentRealeases
relvalAgents = ['vocms053.cern.ch']
testAgents = ['cmssrv95.fnal.gov', 'cmssrv113.fnal.gov', 'vocms040.cern.ch', 'vocms0224.cern.ch', 'vocms0230.cern.ch']

##Job expected types
jobTypes = ['Processing', 'Production', 'Skim', 'Harvest', 'Merge', 'LogCollect', 'Cleanup', 'RelVal', 'Express', 'Repack', 'Reco']
backfillTypes = ['TOP', 'SMP', 'RECO', 'DIGI', 'Prod', 'MinBias', 'MINIAOD', 'HLT', 'LHE', 'ZMM']

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

def jobType(id,sched,typeToExtract):
    """
    This deduces job type from given info about scheduler and taskName
    """
    type = ''
    if sched in relvalAgents:
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
    elif sched in testAgents or any(x in typeToExtract for x in backfillTypes):
        type = 'Processing'
    else:
        type = 'Processing'
        jobs_failedTypeLogic[id]=dict(scheduler = sched, BaseType = typeToExtract)
    return type

def fixArray(array):
    """
    Sometimes condor return different formats. Parse all to string
    """
    strings_array = []
    for entry in array:
        strings_array.append(str(entry))
    return strings_array

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
    jsonfile = open(output_json,'w+')
    jsonfile.write(json.dumps(jsonCounting,sort_keys=True, indent=9))
    jsonfile.close()

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
    for col in all_collectors:
        # Get the list of scheduler for the given collector
        schedds={}
        listcommand="condor_status -pool "+col+""" -schedd -format "%s||" Name -format "%s||" CMSGWMS_Type -format "\n" Owner"""
        proc = subprocess.Popen(listcommand, stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
        out, err = proc.communicate()
        for line in err.split('\n') :
            if 'Error' in line:
                body_text = 'There is a problem with one of the collectors! The monitoring script may give false information. These are the logs:\n\n'
                body_text += err
                body_text += '\nSee the log file in this directory for more output logs:\n\n'
                body_text += '    /afs/cern.ch/user/c/cmst1/CondorMonitoring\n'
                send_mail(mailingSender,
                          mailingList,
                          '[Condor Monitoring] Condor Collector %s Error' % col,
                          body_text)
                print 'ERROR: I find a problem while getting schedulers for collector %s, I will send an email to: %s' % (col, str(mailingList))
                break
        for line in out.split('\n'):
            if not line: continue # remove empty lines from split('\n')
            schedd_info = line.split("||")
            # schedd_info[0] is the Schedd Name
            # schedd_info[1] is the Schedd type if available, if not it is''
            schedds[schedd_info[0].strip()] = schedd_info[1].strip()
                
        print "INFO: Condor status on collector %s has been started" % col
        print "DEBUG: Schedulers ", schedds.keys()
        
        # Get the running/pending jobs from condor for the given scheduler
        for sched in schedds.keys():
            
            # Ignore Analysis schedulers 
            if 'crabschedd' == schedds[sched] or sched in crab_scheds or 'crab' in sched:
                print "DEBUG: Ignoring crab scheduler ", sched
                continue
            
            # Get all the jobs for the given scheduler
            command='condor_q -pool '+col+' -name ' + sched
            command=command+"""  -format "%i." ClusterID -format "%s||" ProcId  -format "%i||" JobStatus  -format "%s||" WMAgent_SubTaskName -format "%s||" RequestCpus -format "%s||" DESIRED_Sites -format "%s||" MATCH_EXP_JOBGLIDEIN_CMSSite -format "\n" Owner"""
            proc = subprocess.Popen(command, stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
            out, err = proc.communicate()
            print "INFO: Handling condor_q on collector: %s scheduler: %s" % (col, sched)
            
            for line in out.split('\n'):
                if not line: continue # remove empty lines from split('\n')
                
                array = line.split("||")
                if len(array) < 6: 
                    continue # ignore bad lines (incomplete info lines)
                array = fixArray(array)
                
                # array[0] ClusterID.ProcId
                # array[1] JobStatus
                # array[2] WMAgent_SubTaskName
                # array[3] RequestCpus
                # array[4] DESIRED_Sites
                    # only when job is already running: array[5] MATCH_EXP_JOBGLIDEIN_CMSSite
                # array[6] ''    --> nothing
                # --> standard len(array) {6,7} depending if the job is already running in a site
                id = array[0]
                status = array[1]
                workflow = array[2].split('/')[1]
                task = array[2].split('/')[-1]
                cpus = int(array[3])
                siteToExtract = array[4].replace(' ', '').split(",")
                
                type = jobType(id,sched,task) # Deducing job type
                
                if siteName(array[5]): # If job is currently running
                    siteToExtract = [array[5]]
                
                if col == tier0_pool[0]:
                    if siteToExtract[0] != 'T2_CH_CERN_T0':
                        continue
                
                if status == "2":
                    increaseRunning(siteToExtract[0],sched,type,cpus)
                    increaseRunningWorkflow(workflow,siteToExtract[0],1)
                elif status == "1":
                    pendingCache.append([sched,type,cpus,siteToExtract])
                    increasePendingWorkflow(workflow,siteToExtract,1)
                else: #We don't care about jobs in another status
                    continue
    print "INFO: Full condor status pooling is done"
    
    # Get total running
    for site in jobCounting.keys():
        totalRunningSite[site] = 0.0
        for schedd in jobCounting[site].keys():
            for type in jobCounting[site][schedd].keys():
                for ncore in jobCounting[site][schedd][type].keys():
                    totalRunningSite[site] += jobCounting[site][schedd][type][ncore]['Running']
    
    # Now process pending jobs
    for job in pendingCache:
        schedd = job[0]
        type = job[1]
        cpus = job[2]
        siteToExtract = []
        for site in job[3]:
            siteToExtract.append(site.replace('_Disk',''))
        siteToExtract = list(set(siteToExtract)) #remove duplicates
        
        relative, total = relativePending(siteToExtract) # total != 0 always
        for penSite in siteToExtract:
            relative_pending = relative[penSite]/total # calculate relative pending weight
            increasePending(penSite, schedd, type, cpus, relative_pending)
    print "INFO: Smart pending site counting done \n"
    
    # Handling jobs that failed task extraction logic
    if jobs_failedTypeLogic != {}:      
        body_text = 'There is a problem with the logic to deduce job type from the condor data.\n'
        body_text += 'Please have a look to the following jobs:\n\n %s'% str(jobs_failedTypeLogic)
        send_mail(mailingSender,
                  mailingList,
                  '[Condor Monitoring] Failed task type logic problem',
                  body_text)
        print 'ERROR: I find jobs that failed the type assignment logic, I will send an email to: %s' % str(mailingList)
    
    print 'INFO: Creating reports...'
    createReports(currTime)
    
    # Creates json file for jobs per workflow
    jsonfile = open(json_name_workflows,'w+')
    jsonfile.write(json.dumps(overview_workflows, default=set_default, sort_keys=True, indent=4))
    jsonfile.close()
    print 'INFO: The script has finished after: ', datetime.now()-starttime
    
if __name__ == "__main__":
    main()