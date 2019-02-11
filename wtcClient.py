import os
import httplib
import ssl
import json
from utils import mongo_client

class wtcClient(object):
    def __init__(self):
        self.conn = None
        if not os.path.exists('Unified/secret_act.txt'):
            print 'Needs to be called from same directory as key.json'
            return

        with open('Unified/secret_act.txt', 'r') as key_file:
            self.key_info = json.load(key_file)
        self._make_conn()

        ## the Mongodb part
        self.client = mongo_client()
        self.db = self.client['wtc-console'].actions
        

    def _make_conn(self):
        self.conn = httplib.HTTPSConnection(self.key_info['url'], self.key_info['port'],
                                            context=ssl._create_unverified_context())
        
    def check(self):
        ## get something from the machine
        pass

    
    def add(self, wfn, action, params):
        spec_acdc = { 'Action' : 'acdc',
                      'Parameters' : { 'TaskName1' : {
                          'sites' : [], ## [],'auto'
                          'memory' : 12000,
                          'multicore' : 5,
                          'split' : 'x3', #Same, max,'','x5',
                          'xrootd' : 'enabled', #'disabled'
                      },
                                       'TaskName2' : {}
                                   }
                  }
        spec_clone = { 'Action' : 'clone',
                       'Parameters' : {
                           'comment' : '', ## optional
                           'memory' : 12000,
                           'split' : 'x3', #Same, max,'','x5',
                       }
                   }
        spec_hold = { 'Action' : 'special',
                      'Parameters' : {
                          'action' : 'onhold' #on-hold
                          }
                  }
        spec_force = { 'Action' : 'special',
                       'Parameters' : {
                          'action' : 'bypass' # by-pass 
                          }
                  }

    def _add(self, wfn, action, params):
        already = self.db.find_one({'workflow' : wfn})
        if already:
            if already.get('Action', None) != action:
                print "inconsistent action on"wfn
                return
            already['Parameters'].update( params )
            # if it exists, updat the Parameters dictionnary
            self.db.update_one(
                {'_id' : already.pop('_id')},
                {'$set' : already}
                )
        else:
            doc = { 'workflow' : wfn,
                    'Action' : action,
                    'Parameters' : params }
            self.db.insert_one( doc )

                    
        
        
    def get_actions(self):
        try:
            actions_1 = self._get_actions()
        except:
            try:
                self._make_conn()
                actions_1 = self._get_actions()
            except Exception as e:
                print str(e)

        try:
            ## some massaging of the format has to be done, maybe, depends on the format in mongodb from the new console
            actions_2 = self.db.find({'acted' : 0})
        except Exception as e:
            print str(e)

        duplicates = set(actions_1.keys()) and set(actions_2.keys())
        if len(duplicates)!=0:
            print "There are duplicated action being dropped"
            for a in duplicates:
                if a in actions_1: actions_1.pop(a)
                if a in actions_2: actions_2.pop(a)
                
        actions = actions_1
        actions.update( actions_2 )
        return actions

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
            r_1 = self._remove_action(*args)
        except:
            try:
                self._make_conn()
                r_1 = self._remove_action(*args)
            except Exception as e:
                print str(e)
                r_1 = []
        r_2 = []
        for w in args:
            try:
                self.db.update({'workflow' : w},
                               {'$set' : {'acted' :1}})
                r_2.append(True)
            except Exception as e:
                r_2.append(False)
        return r_1+r_2

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

        
