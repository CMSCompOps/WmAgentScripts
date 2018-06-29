import os
import json

class TrelloClient:
    def __init__(self):
        for line in open('Unified/secret_trello.txt'):
            line = line.replace('\n','')
            try:
                k,v = line.split(':')
                ###print k,v
                setattr(self,k,v)
            except:
                print line,"does not follow convention key:value"

        ## created by hand
        self.lists = {
            'draining' : '58da314ad8064a3772a8b2b7',
            'running' : '58da313230415813a0f3c31c',
            'standby' : '58da314194946756ba09b66e',
            'drained' : '58da315628847a392b663927',
            'offline' : '58da315628847a392b663927'
            }
        ## a map of agents
        self.getAgents()
        #print json.dumps( self.agents  , indent=2)

    def getAgents(self):
        self.agents = {}
        
        for c in self.getBoard():
            n = c.get('name',None)
            fn = n.split(' - ')[0].strip()
            dom = '.cern.ch' if 'vocms' in fn else '.fnal.gov'
            fn+=dom
            #print n,fn,c.get('id')
            self.agents[fn] = c.get('id')

    def _auth(self):
        return 'key=%s&token=%s'%(self.key, self.token)

    def _put(self, o, oid, pars):
        url = 'https://api.trello.com/1/%s/%s?%s&%s'%( o, oid, '&'.join(['%s=%s'%(k,v) for (k,v) in pars.items()]), self._auth())
        print url
        r = os.popen('curl -s --request PUT --url "%s"'% url)
        #print r
        d = {}
        try:
            d = json.loads(r.read())
        except:
            print "Failed to put %s to %s %s"%( json.dumps( pars),
                                                o, oid)
        return d

    def _get(self, o, oid,supp='?'):
        url = 'https://api.trello.com/1/%s/%s%s&%s'% ( o,oid,supp, self._auth())
        #print url
        r = os.popen('curl -s "%s"'%url)
        d = {}
        try:
            d = json.loads(r.read())
        except:
            print "Failed to get %s %s with info %s"%( o, oid, supp)
        return d

    def changeList(self, cn, ln):
        ln = self.lists.get(ln, ln)
        new_list_id = self.getList(ln).get('id',None)
        c = self.getCard(cn)
        old_list_id =c.get('idList',None)
        if new_list_id and old_list_id and old_list_id!=new_list_id:
            self._put('cards',c.get('id'), {'idList' : new_list_id})
        
    def getList(self, ln):
        return  self._get('lists',ln,)

    def getBoard(self, bn = '4np6TByB'):
        return self._get('boards', bn, '/cards?fields=name,url')

    def getCard(self, cn):
        cn = self.agents.get(cn,cn)
        return self._get('cards', cn)
