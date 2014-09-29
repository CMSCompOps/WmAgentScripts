from WMCore.Database.CMSCouch import CouchServer
#couchUrl = "https://cmsweb.cern.ch/couchdb"
couchUrl = "https://cmsweb-testbed.cern.ch/couchdb"
qUrl = "%s/workqueue" % couchUrl 
queueParams = {'QueueURL': qUrl, "CouchUrl": couchUrl}
dbname = "workqueue"
couchdb = CouchServer(couchUrl).connectDatabase(dbname, False)
ids = []
for item in couchdb.changes(60211506)['results']:
    if item.has_key('deleted') and item["deleted"]:
        revisions = couchdb.getRevision(item["id"])
        ids.append(item["id"])
        for rev in revisions:
            #print rev
            revNum = rev["ok"]['_revisions']['start'] - 1
            revID = rev["ok"]['_revisions']['ids'][1]
            preRev = "%s-%s" % (revNum, revID)
            name = rev["ok"]['_id']
            preDoc = couchdb.document(name, preRev)
            eleStr = 'WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'
            if preDoc.has_key(eleStr):
                lastStatus = preDoc[eleStr]['Status']
                if lastStatus not in ['Canceled', 'Done']:
                    print lastStatus
                    print "xxx"
                    print preDoc
                    print "ooo"


print "*********"
print "All Done"
#print couchdb.allDocs({}, ids)
#print couchdb.getRevision("6130293acd7029436f32a87dac9bf9fa")