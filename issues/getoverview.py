#!/usr/bin/env python -w
"""
retrieves overview information from WMStats and creates
a single json file
"""

import sys,time,os,urllib,urllib2,re
try:
    import json
except ImportError:
    import simplejson as json
import httplib
import shutil
import time
import datetime
import zlib

overview = ''
cachedoverview = '/afs/cern.ch/user/j/jbadillo/www/overview.cache'
cachedoverviewzipped = '/afs/cern.ch/user/j/jbadillo/www/overview.cache.zipped'

def downloadoverview(retries=15, sleeptime = 30):
    """
    Downloads overview from wmstats and parses it
    """
    #TODO use WMStats client instead
    c = 1
    #get information from wmstats
    while c < retries:
        print "Connecting(%s)" % c
        try:
            conn = httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'),
                    key_file = os.getenv('X509_USER_PROXY'),timeout=30)
            conn.request("GET",'/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype?stale=update_after')
            r2 = conn.getresponse()
            
            print "Loading JSON"
            s = json.loads(r2.read())
            conn.close()
            if type(s) is list:
                if 'error_url' in s[0].keys():
                    print "ERROR: %s\n" % s
                    c += 1
                    time.sleep(st)
            return s
        except:
            print "Cannot connect"
            c += 1
            time.sleep(sleeptime)
    return None

def formatoverview(s):
    """
    Cleans data and prepares it
    """
    #formatting data
    print "Creating data" 
    now = time.time()
    reqs = []
    reqnames = set()
    statuses = []
    for r in s['rows']:
        ret = {}
        ret['request_name'],ret['status'],ret['type'] = r['key']
        #ignore taskchains
        #if ret['type'] == 'TaskChain':
        #    continue
        #replace status
        if ret['status'] == 'aborted-archived':
            ret['status'] = 'aborted'
        elif ret['status'] == 'normal-archived':
            ret['status'] = 'announced'
        elif ret['status'] == 'rejected-archived':
            ret['status'] = 'rejected'
        #ignore tests
        elif ret['status'] == 'testing-failed':
            continue
        elif ret['status'] == 'testing-approved':
            continue
        #avoid duplicates
        if ret['request_name'] not in reqnames:
            reqs.append(ret)
            reqnames.add(ret['request_name'])

    print " > %s" % (time.time()-now)
    return reqs

def savetofile(reqs):
    """
    Saves the summary to file an zipped file
    """
    print "Writing"
    now = time.time()
    output = open(cachedoverview, 'w')
    output.write("%s" % reqs)
    output.close()

    print "Uncompressed > %s" % (time.time()-now)
    now = time.time()
    comp = zlib.compress("%s" % reqs)
    output = open(cachedoverviewzipped, 'w')
    output.write("%s" % comp)
    output.close()
    print "Compressed > %s" % (time.time()-now)

def main():
    print datetime.datetime.utcnow()
    overview = downloadoverview()
    if not overview:
        print "Cannot read json"
        sys.exit(2)
    reqs = formatoverview(overview)
    savetofile(reqs)
    print datetime.datetime.utcnow()

if __name__ == "__main__":
        main()
