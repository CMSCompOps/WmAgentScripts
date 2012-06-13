#!/usr/bin/env python

import sys
import pickle

from WMCore.Database.CMSCouch import CouchDBRequests

requestName = sys.argv[1]

requestor = CouchDBRequests(url = "https://cmsweb.cern.ch")
wlpkl = requestor.makeRequest(uri = "/couchdb/reqmgr_workload_cache/%s/spec" % requestName, decode = False)
wldoc = requestor.makeRequest(uri = "/couchdb/reqmgr_workload_cache/%s" % requestName)

wl = pickle.loads(wlpkl)

if len(wl.tasks.tasklist) == 1:
    print "Request only has one top level task, nothing to fix."
    sys.exit(1)

for taskName in wl.tasks.tasklist:
    if taskName.find("Cleanup") != -1:
        break

wl.tasks.tasklist.remove(taskName)
newwlpkl = pickle.dumps(wl)

requestor.makeRequest(uri = "/couchdb/reqmgr_workload_cache/%s/spec?rev=%s" % (requestName, wldoc["_rev"]), type="PUT", data = newwlpkl, encode = False)
