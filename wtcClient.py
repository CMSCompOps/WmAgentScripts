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
        collection = 'wtc-console'#'wtc-console-dev'
        self.adb = self.client[collection].action
        self.tdb = self.client[collection].task_action
        

    def _make_conn(self):
        self.conn = httplib.HTTPSConnection(self.key_info['url'], self.key_info['port'],
                                            context=ssl._create_unverified_context())
        
    def _old_schema(self, task_a):
        return {
            'Action' : task_a['parameters']['action'],
            'Parameters' : task_a['parameters']
        }
        
    def _transform(self):
        ## old expected spec in actor
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

        actions_1 = self._get_actions()
        now = time.mktime(time.gmtime())
        for action in actions_1:
            do = action.get('Action',None)
            if do == 'acdc':
                for task,params in action.get('Parameters',{}).items():
                    ## create a doc per such item
                    doc = { 'name' : task,
                            'workflow' : task.split('/')[1],
                            'acted' : 0,
                            'timestamp' : now,
                            'parameters' : params
                    }
                    self.tdb.update_one({'name' : task,
                                         'acted' : 0},
                                        {'$set' : doc},
                                        upsert=True)
            elif do == 'clone':
                pass
            elif do == 'special':
                pass

    def clean(self):
        aa = self.tdb.find()
        goon = True
        while goon:
            goon = False
            for a in aa:
                f = [o for o in self.tdb.find({'name' : a['name']})]
                if len(f)>1:
                    print "yup",[o for o in f]
                    f = sorted(f, key= lambda o:o['timestamp'])
                    goon = True
                    #self.tdb.delete_one({'_id': f[1]['_id']})
                    break
                else:
                    print "unique entry for",a['name']
                    

    def check(self):
        pass
    
    def add(self, wfn, action, params):
        pass

    def _add(self, task, action, params):
        now = time.mktime(time.gmtime())
        wfn = task.split('/')[1]
        already = self.db.find_one({'name' : task, 'acted' : 0})
        if already:
            if already.get('action', None) != action:
                print "inconsistent action on",wfn
                return
            already['parameters'].update( params )
            # if it exists, updat the Parameters dictionnary
            self.db.update_one(
                {'_id' : already.pop('_id')},
                {'$set' : already}
                )
        else:
            doc = { 
                'name' : task,
                'workflow' : wfn,
                'acted' : 0,
                'timestamp' : now,
                'action' : action,
                'parameters' : params }
            self.db.insert_one( doc )

                    
        
        
    def get_actions(self):
        actions = self.tdb.find({'acted' : 0})
        ## this direct return is what we want eventually
        ## but it requires deep changes in actor
        #return actions #
        ## for now, transform into the expected action schema
        by_wf = {}
        for a in actions:
            t_a= self._old_schema(a)
            by_wf.setdefault(a['workflow'], t_a)['Parameters'].update( t_a.get('Parameters',{}))
            
        return by_wf


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

        
