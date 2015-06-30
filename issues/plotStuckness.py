#!/usr/bin/env python
import sys,urllib,urllib2,re,time,os
try:
    import json
except ImportError:
    import simplejson as json
import optparse
import httplib
import datetime
import time
import zlib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from pprint import pprint 

afs_base ='/afs/cern.ch/user/j/jbadillo/www/' 
live_status = ['assigned','acquired','running-open','running-closed','completed']
ignoreCampaign = set(['CMSSW_7_1_0','CMSSW_7_2_0_pre4','TEST'])

def drawPlot(s, t, r, filename):
    """
    draw the plot
    """
    avg = t/r
    threshold = 1.5*avg
    #f.write('avg = %s<br>'%avg)
    #f.write('threshold = %s<br>'%(threshold))
    h = ['request','status','priority','relativeprio','dayssame','assignedon','stuckness']
    c = 'rgbcmgyko'

    i = 0
    plt.ylabel('Relative Priority')
    plt.xlabel('Days old')
    handles = []
    for rtype, reqs in s.items():
        X = np.array([r['requestdays'] for r in reqs])
        Y = np.array([r['relativeprio'] for r in reqs])
        L = np.array([r['dayssame'] for r in reqs])

        #add some random noise to see quantity
        X += 0.3*(30)*np.random.random(X.shape)
        Y += 0.3*np.random.random(Y.shape)        
        han = plt.scatter(X,Y, c=c[i], marker='o', s=10, lw = 0)
        #add lines to represent dimension
        for j in range(X.size):
            plt.plot( [ X[j]-L[j], X[j]], [Y[j], Y[j]], '%s-'%c[i] )
        handles.append(han)
        i += 1
    

    plt.legend(handles, s.keys(), loc='lower right', prop={'size':8}, scatterpoints = 1)

    plt.savefig(filename)
    #f.write(foot%time.strftime("%c"))

def getpriorities(s,campaign,zone,status):
    """
    Get the different priority values, in decreasing order
    filtered by campaign, zone and status
    """
    p = set()
    for r in s:
        if r['priority'] not in p:
            if (not campaign or campaign == getcampaign(r) ) and (not zone or zone == r['zone']) and r['status'] in status:
                p.add(r['priority'])
    p = sorted(p)
    p.reverse()
    return p

def main():

    #read json files
    now = datetime.datetime.now()
    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat('%s/data.json' % afs_base)
    datajsonmtime = "%s" % time.ctime(mtime)
    d=open('%s/data.json' % afs_base).read()
    s = json.loads(d)


    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat('%s/pledged.json' % afs_base)
    pledgedjsonmtime = "%s" % time.ctime(mtime)
    d=open('%s/pledged.json' % afs_base).read()
    pledged = json.loads(d)
    totalpledged = 0
    #calculate total pledge
    for i in pledged.keys():
        totalpledged = totalpledged + pledged[i]


    s2 = []
    #filter out test stuff
    for r in s:
        if r['status'] not in live_status:
            continue
        name = r['requestname']
        status = r['status']
        campaign = r['campaign']
        rtype = r['type']
        #ignore anything that says "Test"
        if 'test' in name.lower() or 'test' in campaign.lower():
            continue
        #ignore anything that says "backfill"
        if 'backfill' in name.lower() or 'backfill' in campaign.lower():
            continue
        #ignore resubmissions:
        if rtype == 'Resubmission':
            continue
        #ignore dave mason's tests and stefan's tests
        if 'dmason' in name or 'piperov' in name:
            continue
        #ignore relvals and test
        if 'dmason' in name or 'piperov' in name:
            continue
        #ignore relvals
        if 'RVCMSSW' in name:
            continue
        s2.append(r)
    s = s2

    urgent_requests = []
    stuck_days = 5
    old_days = 15
    oldest = {}
    
    highest_prio = max( getpriorities(s,None,None,live_status))
    #for status in ['assigned','acquired','running-open','running-closed','completed']:
    #   for priority in getpriorities(s,'','',[status]):
    totaldays = 0
    reqs = 0
    s2 = {}

    #arrange them by type
    for r in s:
        name = r['requestname']
        status = r['status']
        campaign = r['campaign']
        rtype = r['type']
        days = []
        for ods in r['outputdatasetinfo']:
            if 'lastmodts' not in ods or not ods['lastmodts']:
                days.append(0)
            else:
                lastmodts = datetime.datetime.fromtimestamp(ods['lastmodts'])
                delta = now - lastmodts
                days.append(delta.days + delta.seconds/3600.0/24.0)
        dayssame = min(days) if days else 0
        #TODO calculate stuckness
        relativeprio = float(r['priority'])/highest_prio
        stuckness = relativeprio*dayssame
        r['relativeprio'] = relativeprio
        r['stuckness'] = stuckness
        r['dayssame'] = dayssame
        if dayssame:
            totaldays += dayssame
            reqs += 1
        if rtype not in s2:
            s2[rtype] = []
        s2[rtype].append(r)
        

    s = s2
    drawPlot(s, totaldays, reqs, './www/stuckness.png')
    sys.exit(0)

if __name__ == "__main__":
        main()
