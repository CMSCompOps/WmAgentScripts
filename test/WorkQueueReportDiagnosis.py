#!/usr/bin/env python
"""Helper class for RequestManager interaction
"""

from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError
from WMCore.Database.CMSCouch import CouchError
from WMCore.Database.CouchUtils import CouchConnectionError
from WMCore import Lexicon

from WMCore.WorkQueue.WorkQueue import globalQueue

import os
import time
import socket
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
queueParams['ReqMgrServiceURL'] = REQMGR2
queueParams['RequestDBURL'] = "%s/%s" % (COUCH, reqmgrCouchDB)
queueParams['central_logdb_url'] = LOG_DB_URL
queueParams['log_reporter'] = LOG_REPORTER

import pdb
gq = globalQueue(**queueParams)
elements = gq.statusInbox(dictKey = "RequestName")

print elements


class WorkQueueReqMgrInterface():
    """Helper class for ReqMgr interaction"""
    def __init__(self, **kwargs):
        if not kwargs.get('logger'):
            import logging
            kwargs['logger'] = logging
        self.logger = kwargs['logger']
        self.reqMgr = RequestManager(kwargs)
        self.previous_state = {}

    def report(self, queue):
        """Report queue status to ReqMgr."""
        new_state = {}
        uptodate_elements = []
        now = time.time()

        elements = queue.statusInbox(dictKey = "RequestName")
        if not elements:
            return new_state

        for ele in elements:
            ele = elements[ele][0] # 1 element tuple
            try:
                request = self.reqMgr.getRequest(ele['RequestName'])
                if request['RequestStatus'] in ('failed', 'completed', 'announced',
                                                'epic-FAILED', 'closed-out', 'rejected'):
                    # requests can be done in reqmgr but running in workqueue
                    # if request has been closed but agent cleanup actions
                    # haven't been run (or agent has been retired)
                    # Prune out obviously too old ones to avoid build up
                    if queue.params.get('reqmgrCompleteGraceTime', -1) > 0:
                        if (now - float(ele.updatetime)) > queue.params['reqmgrCompleteGraceTime']:
                            # have to check all elements are at least running and are old enough
                            request_elements = queue.statusInbox(WorkflowName = request['RequestName'])
                            if not any([x for x in request_elements if x['Status'] != 'Running' and not x.inEndState()]):
                                last_update = max([float(x.updatetime) for x in request_elements])
                                if (now - last_update) > queue.params['reqmgrCompleteGraceTime']:
                                    self.logger.info("Finishing request %s as it is done in reqmgr" % request['RequestName'])
                                    queue.doneWork(WorkflowName=request['RequestName'])
                                    continue
                    else:
                        pass # assume workqueue status will catch up later
                elif request['RequestStatus'] == 'aborted':
                    queue.cancelWork(WorkflowName=request['RequestName'])
                # Check consistency of running-open/closed and the element closure status
                elif request['RequestStatus'] == 'running-open' and not ele.get('OpenForNewData', False):
                    print "change request status to running-closed %s" % request
                elif request['RequestStatus'] == 'running-closed' and ele.get('OpenForNewData', False):
                    print "closing the work for %s" % request
                # update request status if necessary
                elif ele['Status'] not in self._reqMgrToWorkQueueStatus(request['RequestStatus']):
                    print "element status: %s" % ele['Status']
                # check if we need to update progress, only update if we have progress
                elif ele['PercentComplete'] > request['percent_complete'] + 1 or \
                     ele['PercentSuccess'] > request['percent_success'] + 1:
                    print "done elements %s" % ele
            except Exception, ex:
                msg = 'Error talking to ReqMgr about request "%s": %s'
                self.logger.error(msg % (ele['RequestName'], str(ex)))

        return uptodate_elements

    def _reqMgrToWorkQueueStatus(self, status):
        """Map ReqMgr status to that in a WorkQueue element, it is not a 1-1 relation"""
        statusMapping = {'acquired': ['Acquired'],
                         'running' : ['Running'],
                         'running-open': ['Running'],
                         'running-closed': ['Running'],
                         'failed': ['Failed'],
                         'aborted': ['Canceled', 'CancelRequested'],
                         'completed': ['Done']}
        if status in statusMapping:
            return statusMapping[status]
        else:
            return []