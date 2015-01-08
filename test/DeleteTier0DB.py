from WMCore.Database.CMSCouch import CouchServer

class Tier0Delete(object):
    
    def __init__(self, couchURL):
        if isinstance(couchURL, CouchServer):
            self.couchServer = couchURL
        else:
            self.couchServer = CouchServer(couchURL)
            
        self.t0Summary = self.couchServer.connectDatabase('t0_workloadsummary', False)
        self.t0Request = self.couchServer.connectDatabase('t0_request', False)
        self.t0WMStats = self.couchServer.connectDatabase('tier0_wmstats', False)
    
    def deleteReplicatorDocs(self, db, docs = None):
        if docs == None:
            docs = db.allDocs()['rows']
        
        filteredDocs = self._filterReplicationDocs(docs)
        if len(filteredDocs) == 0:
            return 
        for doc in filteredDocs:
            db.queueDelete(doc)
        return db.commit()
    
    def _filterReplicationDocs(self, docs):
        filteredDocs = []
        for j in docs:
            if not j['id'].startswith('_'):
                doc = {}
                doc["_id"]  = j['id']
                doc["_rev"] = j['value']['rev']
                filteredDocs.append(doc)
        return filteredDocs
    
    def deleteAllT0(self):
        dbList = [self.t0Summary, self.t0Request]
        dbList = [self.t0Summary]
        for db in dbList:
            print self.deleteReplicatorDocs(db)
        
if __name__ == "__main__":
        
    url = "https://cmsweb-testbed.cern.ch/couchdb"
    t0 = Tier0Delete(url)
    t0.deleteAllT0()
        