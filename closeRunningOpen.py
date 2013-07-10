"""c
_closeRunningOpen_

Use it when many requests are stuck in running-open probably because of problems
in the GQ.

Created on Jul 9, 2013

@author: dballest
"""
import sys

from WMCore.Database.CMSCouch import Database
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend

def getProblematicRequests():
    """
    _getProblematicRequests_
    """
    badWorkflows = []
    backend = WorkQueueBackend('https://cmsweb.cern.ch/couchdb')
    workflowsToCheck = backend.getInboxElements(OpenForNewData = True)
    for element in workflowsToCheck:
        childrenElements = backend.getElementsForParent(element)
        if not len(childrenElements):
            badWorkflows.append(element)
    return badWorkflows

def main():
    print "Looking for problematic inbox elements..."
    problemRequests = getProblematicRequests()
    print "Found %d bad elements:" % len(problemRequests)
    if not problemRequests:
        print "Nothing to fix, contact a developer if the problem persists..."
        return 0
    for request in problemRequests:
        print request["RequestName"]
    var = raw_input("Can we delete these inbox elements: Y/N\n")
    if var == "Y":
        print "Deleting them from the global inbox, you need a WMAgent proxy for this."
        inboxDB = Database('workqueue_inbox', 'https://cmsweb.cern.ch/couchdb')
        for request in problemRequests:
            inboxDB.delete_doc(request._id, request.rev)
        print "Done with the deletions, this should fix the problem."
        return 0
    else:
        print "Doing nothing as you commanded..."
        return 0

if __name__ == "__main__":
    sys.exit(main())