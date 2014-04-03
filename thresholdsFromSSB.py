#!/usr/bin/env python -w
"""
thresholdsFromSSB.py
Set the thresholds for each site in the WMAgent
Pull the inforamtion from SSB.
"""

import sys,urllib,urllib2,re,time,os,traceback
import socket,httplib
try:
    import json
    print "json imported"
except ImportError:
    import simplejson as json
    print "simplejson imported"
#import optparse
#import httplib
#import datetime
#import shutil
import subprocess

wmapath = "/data/srv/wmagent/current"

# urls from site status board
url_total_run = 'http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=160&batch=1&lastdata=1'
url_max_merge = 'http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=161&batch=1&lastdata=1'
url_site_status = 'http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=158&batch=1&lastdata=1'

# pending slots variables
pending_site = 0.4 # 40%
pending_task = 0.3 # 30%

#regex to identify Tiers
#sites are only the ones that with T0, T1, T2 or T3
tierpat = r'T\d_[A-Z]{2}_\w+'


def setSiteThresholds(max_merge,max_proc,site,factor):
    """
    Set thresholds for site
    pending_jobs policy:
        max_pending for a site is pending_site*max_proc
        max_pending for a task is pending_task*max_task
    This allows to keep the right preasure in the queue, and keep the agent safe.
    The site threshold is higger than each task threshold. This allow to keep in the queue different task jobs.
    """
    def cmd(pen):
        return "%s/config/wmagent/manage execute-agent wmagent-resource-control --site-name=%s --running-slots=%s --pending-slots=%s" % (wmapath,site,max_proc,pen)
    def cmd_task(run,pen,task):
        return "%s/config/wmagent/manage execute-agent wmagent-resource-control --site-name=%s --running-slots=%s --pending-slots=%s --task-type=%s" % (wmapath,site,run,pen,task)
    
    # Set general site threshold
    pending = str(int(max_proc*pending_site*factor))
    proc = subprocess.Popen(cmd(pending),stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
    out, err = proc.communicate()
    
    # Set threshold for Processing, Production and Analysis jobs
    group_1 = ['Processing', 'Production', 'Analysis']
    for task in group_1:
        pending = str(int(max_proc*pending_task*factor))
        if int(pending) < 10 and int(pending) > 0: pending = '10'
        proc = subprocess.Popen(cmd_task(max_proc,pending,task),stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
        out, err = proc.communicate()
    
    # Set thresholds for Merge, Cleanup, Harvesting, LogCollect, Skim
    group_2 = ['Merge','Cleanup','Harvesting','LogCollect','Skim']
    for task in group_2:
        pending = str(int(max_merge*pending_task*factor))
        if int(pending) < 10 and int(pending) > 0: pending = '10'
        proc = subprocess.Popen(cmd_task(max_merge,pending,task),stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
        out, err = proc.communicate()

def thresholdsByVOName(sites):
    """
    Creates a dictionary with keys->VOName and values->threshold: 
    """
    thresholdbyVOName = {}
    for site in sites:
        voname = site['VOName']
        value = site['Value']
        if voname not in thresholdbyVOName:
            if value is None: 
                print 'Site %s does not have threholds in SSB' %voname
                continue
            thresholdbyVOName[voname]=int(value)
        else:
            print 'I have a duplicated threshold entry for %s' %voname
    return thresholdbyVOName

def main():
    #Get info about other agents
    url = "cmsweb.cern.ch"
    conn  =  httplib.HTTPSConnection(url, cert_file = '/data/certs/servicecert.pem', key_file = '/data/certs/servicekey.pem')
    conn.request("GET",'/couchdb/wmstats/_design/WMStats/_view/agentInfo?stale=update_after')
    response = conn.getresponse()
    data = response.read()
    conn.close()
    
    host = socket.gethostname()
    
    teamByHost = dict()
    agents = json.loads(data)['rows']
    for agentInfo in agents:
        agent_url = agentInfo['value']['agent_url']
        info = dict(team = agentInfo['value']['agent_team'], 
                    isDrain = agentInfo['value']['drain_mode'])
        teamByHost[agent_url.split(':')[0]] = info
    
    factor = 1.0   
    if host in teamByHost.keys():
        team = teamByHost[host]['team']
        agentsTeam = [teamByHost[key]['isDrain'] for key in teamByHost.keys() if teamByHost[key]['team'] == team]
        if agentsTeam.count(False) != 0:
            factor = 1.0/agentsTeam.count(False)
    
    #global url, tierpat
    try:
        #get text from URLs
        sites = urllib2.urlopen(url_site_status).read()
        total_tun = urllib2.urlopen(url_total_run).read()
        total_merge = urllib2.urlopen(url_max_merge).read()
        #compile pattern
        patt = re.compile(tierpat)
        #parse from json format to dictionary, get only 'csvdata'
        try:
            site_status = json.read(sites)['csvdata']
            running_site = json.read(total_tun)['csvdata']
            runnning_merge = json.read(total_merge)['csvdata']
        except:
            site_status = json.loads(sites)['csvdata']
            running_site = json.loads(total_tun)['csvdata']
            runnning_merge = json.loads(total_merge)['csvdata']
        
        # dictionaries with thresholds info by VOName
        slotsBySite = thresholdsByVOName(running_site)
        slotsForMerge = thresholdsByVOName(runnning_merge)
        
        for site in site_status:
            sitename = site['VOName']
            sitestatus = site['Status']
            if patt.match(sitename):
                #update according to site status
                if sitestatus in ['down','on','drain']: 
                    try:
                        setSiteThresholds(slotsForMerge[sitename], slotsBySite[sitename], sitename, factor)
                        print 'Setting thresholds for site %s: CPUBound = %s, IOBound = %s' % (sitename,slotsBySite[sitename],slotsForMerge[sitename])
                        continue
                    except:
                        print 'Error: Site %s does not have information about thresholds' % sitename
                        continue
                elif sitestatus == 'skip':
                    print "Skipping site %s" % sitename
                    continue
                else:
                    print "Error: Unkwown status '%s' for site %s" % (sitestatus,sitename)
            else:
                print "Site '%s' not a Tier" % sitename

    except Exception, e:
        print( traceback.format_exc() )
if __name__ == "__main__":
        main()