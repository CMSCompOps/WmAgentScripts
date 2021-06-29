#!/usr/bin/env python
import reqMgrClient as reqmgr
import optparse


def setStatus(url, workflowname, newstatus, cascade):
    print "Setting %s to %s" % (workflowname, newstatus)
    if newstatus == 'closed-out':
        return reqmgr.closeOutWorkflow(url, workflowname, cascade)
    elif newstatus == 'announced':
        return reqmgr.announceWorkflow(url, workflowname, cascade)
    elif newstatus == "staged":
        return reqmgr.setStatusToStaged(url, workflowname, cascade)
    else:
        print "ERROR: Cannot set status to ", newstatus


def main():
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', help='Which server to communicate with', default='cmsweb.cern.ch', choices=['cmsweb.cern.ch', 'cmsweb-testbed.cern.ch'])
    parser.add_option('-w', '--workflow', help='Workflow name')
    parser.add_option('-f', '--file', help='A file name which contains the workflows (One workflow in each line)')
    parser.add_option('-c', '--cascade', help='Set the workflow state in cascade mode', default=False)
    parser.add_option('-s', '--status', help='The new status', choices=['staged', 'closed-out', 'announced'])
    (options, args) = parser.parse_args()

    if not options.status:
        parser.error('Status is not given')

    if options.workflow:
        setStatus(options.url, options.workflow, options.status, options.cascade)
    elif options.file:
        for workflow in filter(None, open(options.file).read().split('\n')):
            setStatus(options.url, workflow, options.status, options.cascade)
    else:
        parser.error("You should provide either workflow or file options")


if __name__ == "__main__":
    main()