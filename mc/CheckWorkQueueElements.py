import sys
from WMCore.Database.CMSCouch import Database

def main():
    if len(sys.argv) != 2:
        print "Usage:"
        print "python CheckWorkQueueElements.py <workflowName>"
        sys.exit(0)

    workflow = sys.argv[1]
    x = Database('workqueue', 'https://cmsweb.cern.ch/couchdb')
    y = x.loadView('WorkQueue', 'elementsByParent', {'include_docs' : True}, [workflow])
    for entry in y['rows']:
        doc = entry['doc']
        element = doc['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']
        if element['Status'] != 'Done':
            print 'Element: %s is %s in %s' % (doc['_id'], element['Status'], element['ChildQueueUrl'])

if __name__ == '__main__':
    sys.exit(main())
