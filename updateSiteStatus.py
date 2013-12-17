#!/usr/bin/env python -w
"""
updateSiteStatus.py
Retrieves Sites status from SSB and updates WMAgent, so
it doesn't assign jobs to sites that are down or draining,
and assigns to sites that are on.
This replaces previous script named 'thres.py'
"""

import sys,urllib,urllib2,re,time,os, traceback
try:
    import json
    print "json imported"
    #print help(json)
except ImportError:
    import simplejson as json
    print "simplejson imported"

import optparse
import httplib
import datetime
import shutil
import subprocess

wmapath = "/data/srv/wmagent/current"
#TODO test if a shorter URL works (i.e. without empty fields)
url=('http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=158'
    '&time=24&dateFrom=&dateTo=&site=&sites=all&clouds=undefined&batch=1&'
    'lastdata=1')
#regex to identify Tiers
#sites are only the ones that with T0, T1, T2 or T3
tierpat = r'T\d_[A-Z]{2}_\w+'

#connection threshold
minthres = 100

upper = {}
sitecmd = {}

def setnormal(site):
    """
    Calls manage to set site status to normal
    """
    cmd = "%s/config/wmagent/manage execute-agent wmagent-resource-control --site-name=%s --normal" % (wmapath,site)
    proc = subprocess.Popen(cmd,stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
    out, err = proc.communicate()
    return

def setdown(site):
    """
    Calls manage to set site status to down
    """
    cmd = "%s/config/wmagent/manage execute-agent wmagent-resource-control --site-name=%s --down" % (wmapath,site)
    proc = subprocess.Popen(cmd,stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
    out, err = proc.communicate()
    return

def setdrain(site):
    """
    Calls manage to set site status to drain
    """
    cmd = "%s/config/wmagent/manage execute-agent wmagent-resource-control --site-name=%s --drain" % (wmapath,site)
    proc = subprocess.Popen(cmd,stderr = subprocess.PIPE,stdout = subprocess.PIPE, shell = True)
    out, err = proc.communicate()
    return

def main():
    #global url, tierpat
    try:
        #get text from URL
        rawtext = urllib2.urlopen(url).read()
        #compile pattern
        patt = re.compile(tierpat)
        #parse from json format to dictionary
        try:
            fullmap = json.read(rawtext)
        except:
            fullmap = json.loads(rawtext)
        #get only sites info, from value on csvdata (a list)
        cvsdata = fullmap['csvdata']
        
        
        #iterate over the list of sites
        for siteinfo in cvsdata:
            #get site name
            sitename = siteinfo['VOName']
            sitestatus = siteinfo['Status']
            #only tier names
            if patt.match(sitename):
                #update according to site status
                if sitestatus == 'down': #TODO validate down status?
                    print "%s set to down" % sitename
                    setdown(sitename)
                    continue
                elif sitestatus == 'on':
                    print "%s set to normal" % sitename
                    setnormal(sitename)
                    continue
                elif sitestatus == 'drain':
                    print "%s set to drain" % sitename
                    setdrain(sitename)
                    continue
                elif sitestatus == 'skip':
                    print "%s skipped" % sitename
                    continue
                else:
                    print "unkwown command '%s' for site %s" % (sitestatus,sitename)
            else:
                print "Site '%s' not a Tier" % sitename

    except Exception, e:
        print( traceback.format_exc() )
if __name__ == "__main__":
        main()
