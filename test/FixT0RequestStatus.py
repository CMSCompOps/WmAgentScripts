#!/usr/bin/env python
import os
import sys
from optparse import OptionParser
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from pprint import pprint
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter


baseUrl = "https://cmsweb-testbed.cern.ch/couchdb"
reqUrl = "%s/t0_request" % baseUrl
reqDB = RequestDBWriter(reqUrl, couchapp = "T0Request")
report = reqDB.getRequestByStatus(["AlcaSkim", "Merge", "Harvesting", "Processing Done"], detail = True)

problemRequests = []
for request, info in report.items():
    length = 0
    for stateInfo in info['RequestTransition']:
        length += 1
        if stateInfo['Status'] == "completed" and len(info['RequestTransition']) != length:
            problemRequests.append(request)
            break
        
pprint(problemRequests)
print len(problemRequests)
# for request in problemRequests:
#     report = reqDB.updateRequestStatus(request, "completed")
#     print report
