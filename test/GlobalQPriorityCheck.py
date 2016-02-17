#!/usr/bin/env python
from __future__ import print_function
import os
from pprint import pprint
from WMCore.Wrappers import JsonWrapper
from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WMBSHelper import freeSlots
from WMCore.WorkQueue.WorkQueueUtils import cmsSiteNames
from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.WorkQueue.DataStructs.WorkQueueElementsSummary import WorkQueueElementsSummary
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement

HOST = "https://cmsweb-testbed.cern.ch" 
COUCH = "%s/couchdb" % HOST
wmstatDBName = "wmstats"
WEBURL = "%s/workqueue" % COUCH
REQMGR2 = "%s/reqmgr2" % HOST
LOG_DB_URL = "%s/wmstats_logdb" % COUCH
LOG_REPORTER = "global_workqueue"
reqmgrCouchDB = "reqmgr_workload_cache"

queueParams = {'WMStatsCouchUrl': "%s/%s" % (COUCH, wmstatDBName)}
queueParams['QueueURL'] = WEBURL
queueParams['CouchUrl'] = COUCH
queueParams['ReqMgrServiceURL'] = REQMGR2
queueParams['RequestDBURL'] = "%s/%s" % (COUCH, reqmgrCouchDB)
queueParams['central_logdb_url'] = LOG_DB_URL
queueParams['log_reporter'] = LOG_REPORTER

globalQ = globalQueue(**queueParams)

siteSummary = {}

gqElements = globalQ.status("Available")

print(type(gqElements))
# fileName = "wq_testdata.json"
# f = open(fileName, "w")
# JsonWrapper.dump(gqElements, f)
# f.close()
# 
# f = open(fileName, "r")
# gqDict = JsonWrapper.load(f)
# f.close()
# 
# gqElements = []
# for ele in gqDict:
#     gqElements.append(WorkQueueElement(**ele))
    
gqSummary = WorkQueueElementsSummary(gqElements)
testReq = "riahi_TEST_HELIX_0911-T1_UK_RALBackfill_151119_190209_6101"
filteredElements = gqSummary.elementsWithHigherPriorityInSameSites(testReq)

wqSummary = WorkQueueElementsSummary(filteredElements)
wqElements = wqSummary.getWQElementResultsByReauest()

print("Sites for %s: priority %s, %s" % (testReq, gqSummary.getWQElementResultsByReauest(testReq)['Priority'], gqSummary.getPossibleSitesByRequest(testReq)))
for req in wqElements:
    print("%s: priority: %s job: %s sites: %s" % (req, wqElements[req]['Priority'], wqElements[req]['Jobs'], wqSummary.getPossibleSitesByRequest(req)))