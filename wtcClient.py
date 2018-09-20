import os
import httplib
import ssl
import json

class wtcClient(object):
    def __init__(self):
        self.conn = None
        if not os.path.exists('Unified/secret_act.txt'):
            print 'Needs to be called from same directory as key.json'
            return

        with open('Unified/secret_act.txt', 'r') as key_file:
            self.key_info = json.load(key_file)
        self._make_conn()

    def _make_conn(self):
        self.conn = httplib.HTTPSConnection(self.key_info['url'], self.key_info['port'],
                                            context=ssl._create_unverified_context())
        
    def check(self):
        ## get something from the machine
        pass

    def get_actions(self):
        try:
            return self._get_actions()
        except:
            try:
                self._make_conn()
                return self._get_actions()
            except Exception as e:
                print str(e)
                return None
            
    def _get_actions(self):
        self.conn.request(
            'GET', 
            '/getaction?days=15',
            #'/getaction?days=15&key=%s'%self.key_info['key'],
            json.dumps({'key': self.key_info['key']}),
            headers = {'Content-type': 'application/json'})
        r= self.conn.getresponse().read()
        action_list = json.loads( r )
        return action_list
        
    def remove_action(self, *args):
        try:
            return self._remove_action(*args)
        except:
            try:
                self._make_conn()
                return self._remove_action(*args)
            except Exception as e:
                print str(e)
                return None
            
    def _remove_action(self, *args):

        self.conn.request(
            'POST', '/reportaction',
            json.dumps({'key': self.key_info['key'], 'workflows': args}),
            {'Content-type': 'application/json'})
        
        r= self.conn.getresponse().read()
        jr = json.loads( r )
        print jr 
        #conn.close()
        #return (r == 'Done')
        return all([w in jr['success'] for w in args])

        
