import sys
import urllib
import logging
from dbs.apis.dbsClient import DbsApi
import httplib
import os
import socket
import json
import collections
from collections import defaultdict
import random
import copy
import pickle
import time
import math
import threading
import glob
import datetime
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email.utils import make_msgid

from RucioClient import RucioClient

## add local python paths
for p in ['/usr/lib64/python2.7/site-packages','/usr/lib/python2.7/site-packages']:
    if not p in sys.path: sys.path.append(p)


def mongo_client():
    import pymongo, ssl
    return pymongo.MongoClient('mongodb://%s/?ssl=true' % mongo_db_url,
                               ssl_cert_reqs=ssl.CERT_NONE)


class unifiedConfiguration:
    def __init__(self, configFile='unifiedConfiguration.json'):
        # Explicitly set configFile to 'None' once you want to read from MongoDB
        self.configFile = configFile
        if self.configFile is None:
            self.configs = self.configFile
        else:
            try:
                self.configs = json.loads(open(self.configFile).read())
            except Exception as ex:
                print("Could not read configuration file: %s\nException: %s" %
                      (self.configFile, str(ex)))
                sys.exit(124)

        if self.configs is None:
            try:
                self.client = mongo_client()
                self.db = self.client.unified.unifiedConfiguration
            except Exception as ex:
                print ("Could not reach pymongo.\n Exception: \n%s" % str(ex))
                # self.configs = json.loads(open(self.configFile).read())
                sys.exit(124)

    def get(self, parameter):
        if self.configs:
            if parameter in self.configs:
                return self.configs[parameter]['value']
            else:
                print parameter, 'is not defined in global configuration'
                print ','.join(self.configs.keys()), 'possible'
                sys.exit(124)
        else:
            found = self.db.find_one({"name": parameter})
            if found:
                found.pop("_id")
                found.pop("name")
                return found
            else:
                availables = [o['name'] for o in self.db.find_one()]
                print parameter, 'is not defined in mongo configuration'
                print ','.join(availables), 'possible'
                sys.exit(124)


SC = unifiedConfiguration('serviceConfiguration.json')

mongo_db_url = SC.get('mongo_db_url')
dbs_url = os.getenv('UNIFIED_DBS3_READER', SC.get('dbs_url'))
dbs_url_writer = os.getenv('UNIFIED_DBS3_WRITER', SC.get('dbs_url_writer'))
phedex_url = os.getenv('UNIFIED_PHEDEX', SC.get('phedex_url'))
reqmgr_url = os.getenv('UNIFIED_REQMGR', SC.get('reqmgr_url'))
monitor_dir = os.getenv('UNIFIED_MON', SC.get('monitor_dir'))
monitor_eos_dir = SC.get('monitor_eos_dir')
monitor_dir = monitor_eos_dir
monitor_pub_dir = os.getenv('UNIFIED_MON', SC.get('monitor_pub_eos_dir'))
monitor_pub_eos_dir = SC.get('monitor_pub_eos_dir')
monitor_pub_dir = monitor_pub_eos_dir
base_dir =  os.getenv('UNIFIED_DIR', SC.get('base_dir'))
base_eos_dir = SC.get('base_eos_dir')
unified_url = os.getenv('UNIFIED_URL', SC.get('unified_url'))
unified_url_eos = SC.get('unified_url_eos')
unified_url = unified_url_eos
url_eos = unified_url_eos
unified_pub_url = os.getenv('UNIFIED_URL', SC.get('unified_pub_url'))
cache_dir = SC.get('cache_dir')

FORMAT = "%(module)s.%(funcName)s(%(lineno)s) => %(message)s (%(asctime)s)"
DATEFMT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format = FORMAT, datefmt = DATEFMT, level=logging.DEBUG)


do_html_in_each_module = False

def deep_update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping) or isinstance(v, dict):
            default = v.copy()
            default.clear()
            r = deep_update(d.get(k, default), v)
            d[k] = r
        else:
            d[k] = v
    return d

def sendLog( subject, text , wfi = None, show=True ,level='info'):
    try:
        try_sendLog( subject, text , wfi, show, level)
    except Exception as e:
        print "failed to send log to elastic search"
        print str(e)
        sendEmail('failed logging',subject+text+str(e))

def new_searchLog( q, actor=None, limit=50 ):
    conn = httplib.HTTPSConnection( 'es-unified7.cern.ch' )
    return _searchLog(q, actor, limit,conn, prefix = '/es/unified-logs/_doc', h = es_header())

def _searchLog( q, actor, limit, conn, prefix, h = None):

    goodquery={"query": {"bool": {"must": [{"wildcard": {"meta": "*%s*"%q}}]}}, "sort": [{"timestamp": "desc"}], "_source": ["text", "subject", "date", "meta"]}

    if actor:
        goodquery['query']['bool']['filter'] = { "term" : { "subject" : actor}}

    turl = prefix+'/_search?size=%d'%limit
    print turl
    conn.request("GET" , turl, json.dumps(goodquery) ,headers = h if h else {})

    response = conn.getresponse()
    data = response.read()
    o = json.loads( data )

    hits =  o['hits']['hits']
    return hits

def es_header():
    entrypointname,password = open('Unified/secret_es.txt').readline().split(':')
    import base64
    auth = base64.encodestring(('%s:%s' % (entrypointname, password)).replace('\n', '')).replace('\n', '')
    header = { "Authorization":  "Basic %s"% auth, "Content-Type": "application/json"}
    return header

def new_sendLog( subject, text , wfi = None, show=True, level='info'):
    conn = httplib.HTTPSConnection( 'es-unified7.cern.ch' )

    conn.request("GET", "/es", headers=es_header())
    response = conn.getresponse()
    data = response.read()
    print data

    ## historical information on how the schema was created
    """
    schema= {
            "date": {
                "type": "string",
                "index": "not_analyzed"
            },
            "author": {
                "type": "string"
            },
            "subject": {
                "type": "string"
            },
            "text": {
                "type": "string",
                "index": "not_analyzed"
            },
            "meta": {
                "type": "string",
                "index": "not_analyzed"
            },
            "timestamp": {
                "type": "double"
            }
            }
    content = {}
    settings = {
        "settings" : {
            "index" : {
                "number_of_shards" : 3,
                "number_of_replicas" : 2
                }}}
    content.update( settings )

    content.update({            "mappings" : {"log" : { "properties" : schema}}})

    conn.request("PUT", "/es/unified-logs",  json.dumps( content ), headers = es_header())
    response = conn.getresponse()
    data = response.read()
    print data
    return
    """

    _try_sendLog( subject, text, wfi, show, level, conn = conn, prefix='/es/unified-logs', h = es_header())

def try_sendLog( subject, text , wfi = None, show=True, level='info'):

    re_conn = httplib.HTTPSConnection( 'es-unified7.cern.ch' )
    _try_sendLog( subject, text, wfi, show, level, conn = re_conn, prefix='/es/unified-logs', h = es_header())


def _try_sendLog( subject, text , wfi = None, show=True, level='info', conn= None, prefix= '/es/unified-logs', h =None):

    meta_text="level:%s\n"%level
    if wfi:
        ## add a few markers automatically
        meta_text += '\n\n'+'\n'.join(map(lambda i : 'id: %s'%i, wfi.getPrepIDs()))
        _,prim,_,sec = wfi.getIO()
        if prim:
            meta_text += '\n\n'+'\n'.join(map(lambda i : 'in:%s'%i, prim))
        if sec:
            meta_text += '\n\n'+'\n'.join(map(lambda i : 'pu:%s'%i, sec))
        out = filter(lambda d : not any([c in d for c in ['FAKE','None']]),wfi.request['OutputDatasets'])
        if out:
            meta_text += '\n\n'+'\n'.join(map(lambda i : 'out:%s'%i, out))
        meta_text += '\n\n'+wfi.request['RequestName']

    now_ = time.gmtime()
    now = time.mktime( now_ )
    now_d = time.asctime( now_ )
    doc = {"author" : os.getenv('USER'),
           "subject" : subject,
           "text" : text ,
           "meta" : meta_text,
           "timestamp" : now,
           "date" : now_d}

    if show:
        print text
    encodedParams = urllib.urlencode( doc )
    conn.request("POST" , prefix+'/_doc/', json.dumps(doc), headers = h if h else {})
    response = conn.getresponse()
    data = response.read()
    try:
        res = json.loads( data )
    except Exception as e:
        print "failed"
        print str(e)
        pass


def sendEmail( subject, text, sender=None, destination=None ):
    UC = unifiedConfiguration()

    email_destination = UC.get("email_destination")
    if not destination:
        destination = email_destination
    else:
        destination = list(set(destination))
    if not sender:
        map_who = {
                    'mcremone' : 'matteoc@fnal.gov',
                    'qnguyen' : 'thong.nguyen@cern.ch'
                    }
        user = os.getenv('USER')
        if user in map_who:
            sender = map_who[user]
        else:
            sender = 'cmsunified@cern.ch'

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = COMMASPACE.join( destination )
    msg['Date'] = formatdate(localtime=True)
    new_msg_ID = make_msgid()
    msg['Subject'] = '[Ops] '+subject
    msg.attach(MIMEText(text))
    smtpObj = smtplib.SMTP()
    smtpObj.connect()
    smtpObj.sendmail(sender, destination, msg.as_string())
    smtpObj.quit()

def condorLogger(agent, workflow, wmbs, errorcode_s):
    """
    mechanism to get the condor log out of the agent to a visible place
    """
    pass

def cmsswLogger(errorcode_s):
    """
    mechanism to get the cmsrun log (from node, or eos) to a visible place
    """
    pass

def url_encode_params(params = {}):
    """
    encodes given parameters dictionary. Dictionary values
    can contain list, in that case, encoded params will look
    like this: param=val1&param=val2...
    """
    params_list = []
    for key, value in params.items():
        if isinstance(value, list):
            params_list.extend([(key, x) for x in value])
        else:
            params_list.append((key, value))
    return urllib.urlencode(params_list)

def make_x509_conn(url=reqmgr_url,max_try=5):
    tries = 0
    while tries<max_try:
        try:
            conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            return conn
        except:
            tries+=1
            pass
    return None

def GET(url, there, l=True):
    conn = make_x509_conn(url)
    r1=conn.request("GET",there)
    r2=conn.getresponse()
    if l:
        return json.loads(r2.read())
    else:
        return r2

class UnifiedLock:
    def __init__(self, acquire=True):
        self.owner = "%s-%s"%(socket.gethostname(), os.getpid())
        if acquire: self.acquire()

    def acquire(self):
        from assignSession import session, LockOfLock
        ## insert a new object with the proper time stamp
        ll = LockOfLock( lock=True, 
                         time = time.mktime( time.gmtime()),
                         owner = self.owner)
        session.add( ll )
        session.commit()

    def deadlock(self):
        host = os.getenv('HOST',os.getenv('HOSTNAME',socket.gethostname()))
        from assignSession import session, LockOfLock
        to_remove = []
        for ll in session.query(LockOfLock).filter(LockOfLock.lock== True).filter(LockOfLock.owner.contains(host)).all():
            print ll.owner
            try:
                host,pid = ll.owner.split('-')
                process = os.popen('ps -e -f | grep %s | grep -v grep'%pid).read()
                if not process:
                    print "the lock",ll,"is a deadlock"
                    to_remove.append( ll )
                else:
                    print "the lock on",ll.owner,"is legitimate"
            except:
                print ll.owner,"is not good"

        if to_remove:
            for ll in to_remove:
                session.delete( ll )
            session.commit()

            
    def clean(self):
        ##TODO: remove deadlocks
        ##TODO: remove old entries to keep the db under control
        return

    def __del__(self):
        self.release()

    def release(self):
        from assignSession import session, LockOfLock
        for ll in session.query(LockOfLock).filter(LockOfLock.owner == self.owner).all():
            ll.lock = False
            ll.endtime = time.mktime( time.gmtime())
        session.commit()

class lockInfo:
    def __init__(self, andwrite=True):
        self.owner = "%s-%s"%(socket.gethostname(), os.getpid())
        self.unifiedlock = UnifiedLock()
        
    def release(self, item ):
        try:
            self._release(item)
        except Exception as e:
            print "failed to release"
            print str(e)

    def _release(self, item ):
        from assignSession import session, Lock
        l = session.query(Lock).filter(Lock.item == item).first()
        if not l:
            sendLog('lockInfo',"[Release] %s to be released is not locked"%item)
        else:
            sendLog('lockInfo',"[Release] releasing %s"%item)
            l.lock = False
            session.commit()

    def islocked( self, item):
        from assignSession import session, Lock
        l = session.query(Lock).filter(Lock.item == item).first()
        return (l and l.lock)

    def _lock(self, item, site, reason):
        if not item:
            sendEmail('lockInfo', "trying to lock item %s" % item)
            print "[ERROR] trying to lock item",item
            
        from assignSession import session, Lock
        l = session.query(Lock).filter(Lock.item == item).first()
        do_com = False
        if not l:
            print "in lock, making a new object for",item
            l = Lock(lock=False)
            l.item = item
            l.is_block = '#' in item
            session.add ( l )
            do_com = True
        else:
            print "lock for",item,"already existing",l.lock
        now = time.mktime(time.gmtime())
        ## overwrite the lock
        message = "[Lock] %s"%item
        if l.lock != True:

            l.lock = True
            do_com = True
            message+=" being locked"
        if reason and reason!=l.reason:
            l.reason = reason
            do_com =True
            message+=" because of %s"%reason
        if do_com:
            sendLog('lockInfo',message)
            l.time = now
            session.commit()

    def lock(self, item, site='', reason=None):
        try:
            self._lock( item, site, reason)
        except Exception as e:
            ## to be removed once we have a fully functional lock db
            print "could not lock",item,"at",site
            print str(e)


    def items(self, locked=True):
        from assignSession import session, Lock
        ret = sorted([ l.item for l in session.query(Lock).all() if l.lock==locked])
        return ret

    def tell(self, comment):
        from assignSession import session, Lock
        print "---",comment,"---"
        for l in session.query(Lock).all():
            print l.item,l.lock
        print "------"+"-"*len(comment)


class statusHistory:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.statusHistory

    def content(self):
        c = {}
        for doc in self.db.find():
            c[int(doc['time'])] = doc
        return c

    def add(self, now, info):
        info['time'] = time.mktime(now)
        info['date'] = time.asctime(now)
        self.db.insert_one( info )

    def trim(self, now, days):
        now = time.mktime(now)
        for doc in self.db.find():
            if (float(now)-float(doc['time'])) > days*24*60*60:
                print "trim history of",doc['_id']
                self.db.delete_one( {'_id' : doc['_id']})

class StartStopInfo:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.startStopTime
        
    def pushStartStopTime(self, component, start, stop):
        doc = { 'component' : component,
                'start' : int(start),
            }
        if stop is not None:
            doc.update({
                'stop' : int(stop),
                'lap' : int(stop)-int(start)
            })
        
        self.db.update_one( {'component': component, 'start' : int(start)},
                            {"$set": doc},
                            upsert = True)

    def get(self, component, metric='lap'):
        res = [oo[metric] for oo in sorted(self.db.find({'component' : component}), key = lambda o : o['start']) if metric in oo]
        return res
        
    def purge(self, now, since_in_days):
        then = now - (since_in_days*24*60*60)
        self.db.delete_many( { 'start' : {'$lt': then }})

def checkMemory():
    import resource
    rusage_denom = 1024.
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return mem


class componentInfo:
    def __init__(self, block=True, mcm=None, soft=None, keep_trying=False, check_timeout = 120):
        self.checks = componentCheck(block, mcm, soft, keep_trying)
        self.check_timeout = check_timeout
        # start the checking
        self.checks.start()

    def check(self):
        check_start = time.mktime(time.gmtime())
        # on timeout
        ping = 10
        while self.checks.is_alive():
            now = time.mktime(time.gmtime())
            if (now-check_start) > self.check_timeout:
                alarm =  "Timeout in checking the sanity of components %d > %d , while checking on %s"%(now-check_start,self.check_timeout, self.checks.checking)
                sendLog('componentInfo',alarm, level='critical')
                return False
            print "componentInfo, ping",now,check_start,now-check_start
            time.sleep(ping)
        
        self.status = self.checks.status
        print "componentInfo, going with"
        print self.checks.go
        return self.checks.go

class componentCheck(threading.Thread):
    def __init__(self, block=True, mcm=None, soft=None, keep_trying=False):
        threading.Thread.__init__(self)
        self.daemon = True
        if soft is None:
            self.soft = ['mcm','wtc', 'mongo' , 'jira'] ##components that are not mandatory
        else:
            self.soft = soft
        self.block = block
        self.status ={
            'reqmgr' : False,
            'mcm' : False,
            'dbs' : False,
            'cmsr' : False,
            'wtc' : False,
            'eos' : False,
            'mongo' : False,
            'jira' : False
            }
        self.code = 0
        self.keep_trying = keep_trying
        self.go = False
        self.checking=None

    def run(self):
        self.go = self.check()
        print "componentCheck finished"

    def check_cmsr(self):
        from assignSession import session, Workflow
        all_info = session.query(Workflow).filter(Workflow.name.contains('1')).all()

    def check_reqmgr(self):
        data = getReqmgrInfo(reqmgr_url)
        
    def check_mcm(self):
        from McMClient import McMClient
        mcmC = McMClient(dev=False)
        test = mcmC.getA('requests',page=0)
        time.sleep(1)
        if not test:
            raise Exception("mcm is corrupted")

    def check_dbs(self):
        dbsapi = DbsApi(url=dbs_url)
        if 'testbed' in dbs_url:
            blocks = dbsapi.listBlockSummaries( dataset = '/QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph/Summer12pLHE-DMWM_Validation_DONOTDELETE_Alan_TEST-v1/GEN', detail\
                                                =True)
        else:
            blocks = dbsapi.listBlockSummaries( dataset = '/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM', detail=True)
        if not blocks:
            raise Exception("dbs corrupted")

    def check_wtc(self):
        from wtcClient import wtcClient
        WC = wtcClient()
        a = WC.get_actions()
        if a is None:
            raise Exception("No action can be retrieved")

    def check_eos(self):
        eosfile = base_eos_dir+'/%s-testfile'%os.getpid()
        oo = eosFile(eosfile)
        oo.write("Testing I/O on eos")
        r = oo.close() ## commits to eos
        if r:
            r = os.system('env EOS_MGM_URL=root://eoscms.cern.ch eos rm %s'% eosfile)
            if not r == 0:
                raise Exception("failed to I/O on eos")

    def check_mongo(self):
        db = agentInfoDB()
        infos = [a['status'] for a in db.find()]
        
    def check_jira(self):
        from JIRAClient import JIRAClient
        JC = JIRAClient()
        opened = JC.find({'status': 'OPEN'})
        
        
    def check(self):
        ecode = 120
        for component in sorted(self.status):
            ecode+=1
            self.checking = component
            while True:
                try:
                    print "checking on",component
                    sys.stdout.flush()
                    getattr(self,'check_%s'%component)()
                    self.status[component] = True
                    break
                except Exception as e:
                    self.tell(component)
                    if self.keep_trying:
                        print "re-checking on",component
                        time.sleep(30)
                        continue
                    import traceback
                    print traceback.format_exc()
                    print component,"is unreachable"
                    print str(e)
                    if self.block and not (self.soft and component in self.soft):
                        self.code = ecode
                        return False
                    break

        print json.dumps( self.status, indent=2)
        sys.stdout.flush()
        return True

    def tell(self, c):
        host = socket.gethostname()
        sendLog('componentInfo',"The %s component is unreachable from %s"%(c, host), level='critical')

def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError, e:
        return False
    return True

def read_file(target):
    content = open(target).read()
    if target.endswith('json'):
        if is_json(content):
            return content
        else:
            print("Opening an invalid json file")
            sendLog("eosRead","Error reading json file {} at {}".format(target, datetime.datetime.now()), level='critical')
            return "{}"
    else:
        return content

def eosRead(filename,trials=5):
    filename = filename.replace('//','/')
    if not filename.startswith('/eos/'):
        print filename,"is not an eos path in eosRead"
    T=0
    while T<trials:
        T+=1
        try:
            return read_file(filename) 
        except Exception as e:
            print "failed to read",filename,"from eos"
            time.sleep(2)
            cache = (cache_dir+'/'+filename.replace('/','_')).replace('//','/')
            r = os.system('env EOS_MGM_URL=root://eoscms.cern.ch eos cp %s %s'%( filename, cache ))
            if r==0:
                return read_file(cache)
    print "unable to read from eos"
    return None
        
class eosFile(object):
    def __init__(self, filename, opt='w', trials=5):
        if not filename.startswith('/eos/'):
            print filename,"is not an eos path"
            sys.exit(2)
        self.opt = opt
        self.eos_filename = filename.replace('//','/')
        self.cache_filename = (cache_dir+'/'+filename.replace('/','_')).replace('//','/')
        self.cache = open(self.cache_filename, self.opt)
        self.trials = trials

    def write(self, something):
        self.cache.write( something )
        return self

    def close(self):
        self.cache.close()
        bail_and_email = False
        T = 0
        while T < self.trials:
            T += 1
            try:
                print "moving",self.cache_filename,"to",self.eos_filename
                print("Attempt {}".format(T))
                r = os.system("env EOS_MGM_URL=root://eoscms.cern.ch eos cp %s %s"%( self.cache_filename, self.eos_filename))
                if r==0 and os.path.getsize(self.eos_filename) > 0: return True
                print "not able to copy to eos",self.eos_filename,"with code",r
                time.sleep(30)
                
                if bail_and_email:
                    h = socket.gethostname()
                    print 'eos is acting up on %s on %s. not able to copy %s to eos code %s'%( h, time.asctime(), self.eos_filename, r)
                    break

            except Exception as e:
                print "Failed to copy",self.eos_filename,"with",str(e)
                if bail_and_email:
                    h = socket.gethostname()
                    print 'eos is acting up on %s on %s. not able to copy %s to eos \n%s'%( h, time.asctime(), self.eos_filename, str(e))
                    break
                else:
                    time.sleep(30)
        h = socket.gethostname()
        msg = 'eos is acting up on %s on %s. not able to copy %s to eos'%( h, time.asctime(), self.eos_filename)
        sendEmail('eosFile',msg)
        print(msg)
        return False

class batchInfo:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.batchInfo
    def update(self, name, a_list):
        ex = self.db.find_one({'name' : name})
        if ex:
            ex['ids'] = list(set(list(ex['ids'])+list(a_list)))
        else:
            ex = {'ids': list(a_list)}
        self.add( name, ex)

    def add(self, name, content):
        ## update if necessary
        self.db.update_one({'name' : name},
                           {"$set": content},
                           upsert = True
                       )
    def content(self):
        c = {}
        for o in self.db.find():
            c[o['name']] = o['ids']
        return c

    def all(self):
        return [o['name'] for o in self.db.find()]

    def pop(self, name):
        self.db.delete_one({'name': name})

class campaignInfo:
    def __init__(self):

        self.campaigns = {}
        self.client = mongo_client()
        self.db = self.client.unified.campaignsConfiguration
        self.campaigns = self.content()

        SI = global_SI()
        for c in self.campaigns:
            if 'parameters' in self.campaigns[c]:
                if 'SiteBlacklist' in self.campaigns[c]['parameters']:
                    for black in copy.deepcopy(self.campaigns[c]['parameters']['SiteBlacklist']):
                        if black.endswith('*'):
                            self.campaigns[c]['parameters']['SiteBlacklist'].remove( black )
                            reg = black[0:-1]
                            self.campaigns[c]['parameters']['SiteBlacklist'].extend( [site for site in (SI.all_sites) if site.startswith(reg)] )

    def content(self):
        uc = {}
        for c in self.db.find():
            c.pop("_id")
            uc[c.pop("name")] = c
        return uc

    def all(self, c_type = None):
        return [o['name'] for o in self.db.find() if (c_type == None or o.get('type',None) == c_type)]

    def update(self, c_dict, c_type=None):
        for k,v in c_dict.items():
            self.add( k, v, c_type=c_type)
            
    def add(self, name, content, c_type=None):
        ## update if needed
        content['name'] = name
        if c_type:
            content['type'] = c_type
        self.db.update_one({'name' : name},
                           {"$set": content},
                           upsert = True
                       )

    def pop(self, item_name):
        print "removing",item_name,"from campaign configuration"
        self.db.delete_one({'name' : item_name})

    def go(self, c, s=None):
        GO = False
        if c in self.campaigns and self.campaigns[c]['go']:
            if 'labels' in self.campaigns[c]:
                if s!=None:
                    GO = (s in self.campaigns[c]['labels']) or any([l in s for l in self.campaigns[c]['labels']])
                else:
                    print "Not allowed to go for",c,s
                    GO = False
            else:
                GO = True
        elif c in self.campaigns and not self.campaigns[c]['go']:
            if s and 'pilot' in s.lower():
                GO = True
        else:
            print "Not allowed to go for",c
            GO = False
        return GO

    def get(self, c, key, default):
        if c in self.campaigns:
            if key in self.campaigns[c]:
                return copy.deepcopy(self.campaigns[c][key])
        return copy.deepcopy(default)

    def parameters(self, c):
        if c in self.campaigns and 'parameters' in self.campaigns[c]:
            return self.campaigns[c]['parameters']
        else:
            return {}

    def allSecondaries(self):
        secs = set()
        for c in self.campaigns:
            if self.campaigns[c].get('go'):
                for sec in self.campaigns[c].get('secondaries',{}):
                    if not sec: continue
                    secs.add( sec )
        return sorted(secs)

class moduleLock(object):
    def __init__(self,component=None, silent=False, wait=False, max_wait = 18000, locking=True):
        if not component:
            component = sys._getframe(1).f_code.co_name

        self.poll = 30
        self.pid = os.getpid()
        self.host = socket.gethostname()
        self.component = component
        self.wait = wait
        self.silent= silent
        self.max_wait = max_wait
        self.locking = locking

        self.client = mongo_client()
        self.db = self.client.unified.moduleLock
        
    def check(self, hours_before_kill = 24):
        host = socket.gethostname()
        locks = [l for l in self.db.find({'host' : host})]
        now = time.mktime(time.gmtime())
        for lock in locks:
            pid = lock.get('pid',None)
            print "checking on %s on %s"%( pid, host)
            on_since = now - lock.get('time',now)
            if on_since > (hours_before_kill*60*60):
                alarm = "process %s on %s for module %s is running since %s : killing"%( pid, host, lock.get('component',None), display_time( on_since))
                sendLog('heartbeat', alarm, level='critical')
                os.system('sudo kill -9 %s'%(pid))
                time.sleep(2)
            if not os.path.isdir('/proc/%s'% pid):
                alarm = "process %s is not present on %s"%( pid, host)
                sendLog('heartbeat', alarm, level='critical')
                self.db.delete_one({ '_id' : lock.get('_id',None)})


    def all_locks(self):
        locks = [l for l in self.db.find()]
        print "module locks available in mongodb"
        print sorted(locks)
        
    def clean(self, component=None, pid=None, host=None):
        sdoc = {'component' : component}
        if pid is not None:
            sdoc.update({'pid' : pid})
        if host is not None:
            sdoc.update({'host' : host})
        self.db.delete_many( sdoc )
               
    def __call__(self):
        print "module lock for component",self.component,"from mongo db"
        polled = 0
        nogo = True
        locks = []
        i_try = 0
        while True:
            if not self.locking:
                nogo = False
                break
            ## check from existing such lock, solely based on the component, nothing else
            locks = [l for l in self.db.find({'component' : self.component})]
            if locks:
                if not self.wait:
                    nogo =True
                    break
                else:
                    print "Waiting for other %s components to stop running \n%s" % ( self.component , locks)
                    time.sleep( self.poll )
                    polled += self.poll
            else:
                ## nothing is in the way. go ahead
                nogo = False
                break
            i_try += 1
            if self.max_wait and polled > self.max_wait:
                print "stop waiting for %s to be released"% ( self.component )
                break
        if not nogo:
            ## insert a lock doc
            n = time.gmtime()
            now = time.mktime( n )
            nows = time.asctime( n )
            lockdoc = {'component' : self.component,
                       'host' : self.host,
                       'pid' : self.pid,
                       'time' : now,
                       'date' : nows}
            self.db.insert_one( lockdoc )
            #print lockdoc
        else:
            if not self.silent:
                msg = 'There are %s instances running.Possible deadlock. Tried for %d [s] \n%s'%(len(locks),
                                                                                                 polled,
                                                                                                 locks)
                sendLog('heartbeat', msg , level='critical')
                print msg
        return nogo

    def __del__(self):
        # remove the lock doc
        self.clean( component = self.component,
                    pid = self.pid,
                    host = self.host)

def userLock(component=None):
    if not component:
        ## get the caller
        component = sys._getframe(1).f_code.co_name
    lockers = ['dmytro','mcremone','vlimant']
    for who in lockers:
        if os.path.isfile('/afs/cern.ch/user/%s/%s/public/ops/%s.lock'%(who[0],who,component)):
            print "disabled by",who
            return True
    return False

def getWMStats(url):
    conn = make_x509_conn(url)
    url = '/wmstatsserver/data/requestcache'
    r1=conn.request("GET",url,headers={"Accept":"application/json"})
    r2=conn.getresponse()
    return json.loads(r2.read())['result'][0]

def get_dashbssb(path_name, ssb_metric):
    return runWithRetries(_get_dashbssb, [path_name, ssb_metric],{})

def _get_dashbssb(path_name, ssb_metric):
    with open('Unified/monit_secret.json') as monit:
        conf = json.load(monit)
    query = """'{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_cmssst_*","monit_prod_cmssst_*"]}
{"size":1,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":"now-2d","lte":"now","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"metadata.type: ssbmetric AND metadata.type_prefix:raw AND metadata.path: %s"}}]}},"sort":{"metadata.timestamp":{"order":"desc","unmapped_type":"boolean"}},"script_fields":{},"docvalue_fields":["metadata.timestamp"]}
'"""%(str(path_name))
    TIMESTAMP = json.loads(os.popen('curl -s -X POST %s -H "Authorization: Bearer %s" -H "Content-Type: application/json" -d %s'%(conf["url"],conf["token"],query)).read())["responses"][0]["hits"]["hits"][0]["_source"]["metadata"]["timestamp"]
    query2 = """'{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_cmssst_*","monit_prod_cmssst_*"]}
{"size":500,"_source": {"includes":["data.name","data.%s"]},"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":"now-2d","lte":"now","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"metadata.type: ssbmetric AND metadata.type_prefix:raw AND metadata.path: %s AND metadata.timestamp:%s"}}]}},"sort":{"metadata.timestamp":{"order":"desc","unmapped_type":"boolean"}},"script_fields":{},"docvalue_fields":["metadata.timestamp"]}
'"""%(str(ssb_metric),str(path_name),str(TIMESTAMP))
    result = json.loads(os.popen('curl -s --retry 5 -X POST %s -H "Authorization: Bearer %s" -H "Content-Type: application/json" -d %s'%(conf["url"],conf["token"],query2)).read())["responses"][0]["hits"]["hits"]    
    result = [ item['_source']['data'] for item in result] 
    if result: 
        return result
    else:
        raise Exception("get_dashbssb returns an empty collection")

class ThreadHandler(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.threads = args.get('threads', [])
        self.n_threads = args.get('n_threads', 10)
        self.r_threads = []
        self.sleepy = args.get('sleepy',10)
        self.start_wait = args.get('start_wait',5)
        self.show_eta = args.get('show_eta', 10)
        self.timeout = args.get('timeout',None)
        self.verbose = args.get('verbose',False)
        self.label = args.get('label', 'ThreadHandler')

    def run(self):
        self._run()
        self.threads = self.r_threads

    def get_eta(self):
        n_done = sum([not t.is_alive() for t in self.r_threads])
        now= time.mktime(time.gmtime())
        spend = now - self.start_now 
        n_remaining = ntotal - n_done


    def _run(self):
        random.shuffle(self.threads)
        ntotal=len(self.threads)
        print "[%s] Processing %d threads with %d max concurrent and timeout %s [min]"%( self.label, ntotal,self.n_threads, self.timeout)
        start_now = time.mktime(time.gmtime())
        self.r_threads = []
        

        def get_eta():
            n_done = sum([not t.is_alive() for t in self.r_threads])
            now= time.mktime(time.gmtime())
            spend = (now - start_now)
            n_remaining = ntotal - n_done
            eta = None
            total_expected = 0
            if n_done:
                time_per_thread = spend / float(n_done)
                eta = time_per_thread * n_remaining
                total_expected = int(ntotal * time_per_thread)
                if n_done > int(ntotal*0.05):
                    ## shoot for 10 reminder in total                                                                                                              
                    self.show_eta = max(self.show_eta, int(total_expected/10.))
            print "[%s] Will finish in about %s. %d/%d. spend %s. expected total %s, ping in %d [s] "%(
                self.label,
                display_time(eta) if eta else "N/A", 
                n_done, ntotal,
                display_time(spend),
                display_time(total_expected),
            self.show_eta
            )
        


        bug_every=max(len(self.threads) / 10., 100.) ## 10 steps of eta verbosity
        next_ping = int(len(self.threads)/bug_every)
        time_per_thread = None
        refresh_ping = self.sleepy
        eta = None
        last_talk = time.mktime(time.gmtime())
        while self.threads:
            if self.timeout and (time.mktime(time.gmtime()) - start_now) > (self.timeout*60.):
                print "[%s] Stopping to start threads because the time out is over %s "%(self.label,time.asctime(time.gmtime()))
                for t in self.r_threads:
                    while t.is_alive():
                        time.sleep(refresh_ping)
                ## transfer all to running
                while self.threads:
                    self.r_threads.append( self.threads.pop(-1))
                ## then we have to kill all threads
                for t in self.r_threads:
                    pass
                return


            ## check on running and start enough new threads
            running = sum([t.is_alive() for t in self.r_threads])
            now= time.mktime(time.gmtime())

            if self.n_threads==None or running < self.n_threads:
                startme = self.n_threads-running if self.n_threads else len(self.threads)

                if self.verbose and now - last_talk > self.show_eta:
                    last_talk = now
                    get_eta()
                    
                    if time_per_thread and refresh_ping > 5*time_per_thread:
                        refresh_ping = 2*time_per_thread

                ## start enough threads
                for it in range(startme):
                    if self.threads:
                        self.r_threads.append( self.threads.pop(-1))
                        self.r_threads[-1].start()
                        ## just wait a sec if needed
                        time.sleep(self.start_wait)

        ##then wait for completion
        while sum([t.is_alive() for t in self.r_threads]):
            now= time.mktime(time.gmtime())
            if self.timeout and (now - start_now) > (self.timeout*60.):
                print "[%s] Stopping to start threads because the time out is over %d "%(self.label,time.asctime(time.gmtime()))
                return
            if now - last_talk > self.show_eta:
                last_talk = now
                get_eta()

class docCache:
    def __init__(self):
        self.cache = {}        
        def default_expiration():
            ## a random time between 20 min and 30 min.
            return int(20 + random.random()*10)
        self.cache['ssb_prod_status'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : get_dashbssb('sts15min','prod_status'),
            'cachefile' : None,
            'default' : {}
            }
        self.cache['ssb_core_max_used'] = {
            'data': None,
            'timestamp': time.mktime(time.gmtime()),
            'expiration': default_expiration(),
            'getter': lambda: get_dashbssb('scap15min', 'core_max_used'),
            'cachefile': None,
            'default': []
        }
        self.cache['ssb_core_production'] = {
            'data': None,
            'timestamp': time.mktime(time.gmtime()),
            'expiration': default_expiration(),
            'getter': lambda: get_dashbssb('scap15min', 'core_production'),
            'cachefile': None,
            'default': []
        }
        self.cache['ssb_core_cpu_intensive'] = {
            'data': None,
            'timestamp': time.mktime(time.gmtime()),
            'expiration': default_expiration(),
            'getter': lambda: get_dashbssb('scap15min', 'core_cpu_intensive'),
            'cachefile': None,
            'default': []
        }
        self.cache['gwmsmon_totals'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl --retry 5 -s https://cms-gwmsmon.cern.ch/poolview/json/totals').read()),
            'cachefile' : None,
            'default' : {}
            }
        self.cache['mcore_ready'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl --retry 5 -s http://cmsgwms-frontend-global.cern.ch/vofrontend/stage/mcore_siteinfo.json').read()),
            'cachefile' : None,
            'default' : {}
            }
        self.cache['gwmsmon_prod_site_summary' ] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl --retry 5 -s https://cms-gwmsmon.cern.ch/prodview//json/site_summary').read()),
            'cachefile' : None,
            'default' : {}
            }
        self.cache['gwmsmon_prod_maxused' ] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl --retry 5 -s https://cms-gwmsmon.cern.ch/prodview//json/maxusedcpus').read()),
            'cachefile' : None,
            'default' : {}
            }
        self.cache['detox_sites'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : os.popen('curl --retry 5 -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt').read().split('\n'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['site_queues'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodesQueue('cmsweb.cern.ch'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['site_storage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getSiteStorage('cms-cric.cern.ch'),
            'cachefile' : None,
            'default' : ""
            }
        def get_invalidation():
            import csv
            TMDB_invalid = set([row[3] for row in csv.reader( os.popen('curl -s "https://docs.google.com/spreadsheets/d/11fFsDOTLTtRcI4Q3gXw0GNj4ZS8IoXMoQDC3CbOo_2o/export?format=csv"'))])
            TMDB_invalid = map(lambda e : e.split(':')[-1], TMDB_invalid)
            print len(TMDB_invalid),"globally invalidated files"
            return TMDB_invalid

        self.cache['file_invalidation'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : get_invalidation,
            'cachefile' : None,
            'default' : {}
            }
        self.cache['wmstats'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getWMStats('cmsweb.cern.ch'),
            'cachefile' : None,
            'default' : {}
            }
        #create the cache files from the labels
        for src in self.cache:
            self.cache[src]['cachefile'] = '.'+src+'.cache.json'


    def get(self, label, fresh=False, lastdoc=True):
        if not label in self.cache:
            print "unkown cache doc key",label
            return None
        cache = cacheInfo()
        cached = cache.get( label ) if not fresh else None
        if cached:
            return cached
        else:
            o = self.cache[label]
            try:
                data =  o['getter']()
            except Exception as e:
                sendLog('doccache','Failed to get {}\n{}\n{}'.format(label,type(e),str(e)), level='critical')
                print "failed to get",label
                print str(e)
                if lastdoc:
                    last_doc = cache.get( label, no_expire=True)
                    print "last document in cache for",label,"is",last_doc
                    if last_doc:
                        print "returning the last doc in cache"
                        return last_doc
                data = o['default']
            cache.store( label, 
                         data = data,
                         lifetime_min = o['expiration'] )
            return data

def getSiteStorage(url):
    conn = make_x509_conn(url)
    r1=conn.request("GET",'/api/cms/site/query/?json&preset=data-processing', headers={"Accept":"application/json"})
    r2=conn.getresponse()
    r = json.loads(r2.read())['result']
    return r


def getNodesQueue(url):
    ret = defaultdict(int)
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/nodeusagehistory')
    r2=conn.getresponse()
    result = json.loads(r2.read())

    for node in result['phedex']['node']:
        for usage in node['usage']:
            ret[node['name']] += int(usage['miss_bytes'] / 1023.**4) #in TB

    return ret
    r1=conn.request("GET",'/phedex/datasvc/json/prod/nodeusage')
    r2=conn.getresponse()
    result = json.loads(r2.read())

    for node in result['phedex']['node']:
        ret[node['name']] += int(node['miss_bytes'] / 1023.**4) #in TB
    return ret


dataCache = docCache()

class siteInfo:
    def __init__(self, override_good = None):

        UC = unifiedConfiguration()

        self.sites_ready_in_agent = set()

        try:
            agents = getAllAgents( reqmgr_url )
            for team,agents in agents.items():
                if team !='production': continue
                for agent in agents:
                    if agent['status'] != 'ok':
                        continue
                    for site,site_info in agent['WMBS_INFO']['thresholds'].iteritems():
                        if site_info['state'] in ['Normal']:
                            self.sites_ready_in_agent.add( site )
        except Exception as e :
            print e
            pass

        try:
            ## get all sites from SSB readiness
            self.sites_ready = []
            self.sites_not_ready = []
            self.all_sites = []

            self.sites_banned = UC.get('sites_banned')

            data = dataCache.get('ssb_prod_status')
            for siteInfo in data:
                self.all_sites.append( siteInfo['name'] )
                override = (override_good and siteInfo['name'] in override_good)
                if siteInfo['name'] in self.sites_banned and not override:
                    continue
                if (self.sites_ready_in_agent and siteInfo['name'] in self.sites_ready_in_agent) or override:
                    self.sites_ready.append( siteInfo['name'] )
                elif self.sites_ready_in_agent and not siteInfo['name'] in self.sites_ready_in_agent:
                    self.sites_not_ready.append( siteInfo['name'] )
                elif siteInfo['prod_status'] == 'enabled':
                    self.sites_ready.append( siteInfo['name'] )
                else:
                    self.sites_not_ready.append( siteInfo['name'] )

            ##over-ride those since they are only handled through jobrouting
            add_as_ready = [
                'T3_US_OSG',
                'T3_US_Colorado',
                'T3_CH_CERN_HelixNebula',
                'T3_CH_CERN_HelixNebula_REHA',
                'T3_US_NERSC',
                'T3_US_TACC',
                'T3_US_PSC',
                'T3_US_SDSC'
                            ]
            for aar in add_as_ready:
                if not aar in self.sites_ready:
                    self.sites_ready.append(aar)
                if not aar in self.all_sites:
                    self.all_sites.append(aar)



        except Exception as e:
            print "issue with getting SSB readiness"
            print str(e)
            sys.exit(-9)

        self.sites_auto_approve = UC.get('sites_auto_approve')

        self.sites_eos = [ s for s in self.sites_ready if s in ['T2_CH_CERN','T2_CH_CERN_HLT'] ]
        self.sites_T3s = [ s for s in self.sites_ready if s.startswith('T3_')]
        self.sites_T2s = [ s for s in self.sites_ready if s.startswith('T2_')]
        self.sites_T1s = [ s for s in self.sites_ready if s.startswith('T1_')]# or s.startswith('T0_'))] ## put the T0 in the T1 : who cares
        self.sites_T0s = [ s for s in self.sites_ready if s.startswith('T0_')]

        self.sites_T3s_all = [ s for s in self.all_sites if s.startswith('T3_')]
        self.sites_T2s_all = [ s for s in self.all_sites if s.startswith('T2_')]
        self.sites_T1s_all = [ s for s in self.all_sites if s.startswith('T1_')]# or s.startswith('T0_')]
        self.sites_T0s_all = [ s for s in self.all_sites if s.startswith('T0_')]

        self.sites_AAA = list(set(self.sites_ready) - set(['T2_CH_CERN_HLT']))
        ## good enough to read lightweight
        add_on_aaa = ['T3_CH_CERN_HelixNebula',
                      'T3_CH_CERN_HelixNebula_REHA',
                      
                      
        ]
        ## good enough to do premixing
        add_on_good_aaa = ['T3_IN_TIFRCloud',
                           'T3_US_NERSC',
                           'T3_US_PSC',
                           'T3_US_TACC',
                           'T3_US_OSG',
                           'T3_US_Colorado',
                           'T3_US_SDSC'
        ]
        add_on_aaa = list(set(add_on_good_aaa + add_on_aaa))
        self.sites_AAA = list(set(self.sites_AAA + add_on_aaa ))

        ## could this be an SSB metric ?
        self.sites_with_goodIO = UC.get('sites_with_goodIO')
        #restrict to those that are actually ON
        self.sites_with_goodIO = [s for s in self.sites_with_goodIO if s in self.sites_ready]

        self.sites_veto_transfer = []  ## do not prevent any transfer by default

        ## new site lists for better matching
        self.sites_with_goodAAA = UC.get('sites_with_goodAAA')
        self.sites_with_goodAAA = self.sites_with_goodAAA
        self.sites_with_goodAAA = list(set([ s for s in self.sites_with_goodAAA if s in self.sites_ready]))

        self.HEPCloud_sites = UC.get('HEPCloud_sites')
        self.HEPCloud_sites = [s for s in self.HEPCloud_sites if s in self.sites_ready]


        self.storage = defaultdict(int)
        self.disk = defaultdict(int)
        self.queue = defaultdict(int)
        self.free_disk = defaultdict(int)
        self.quota = defaultdict(int)
        self.locked = defaultdict(int)
        self.cpu_pledges = defaultdict(int)
        ## this is the most natural way to handle this
        self.addHocStorageS = defaultdict(set)
        self.addHocStorage = {
            'T2_CH_CERN_T0': 'T2_CH_CERN',
            'T2_CH_CERN_AI' : 'T2_CH_CERN',
            'T3_US_NERSC' : 'T1_US_FNAL_Disk',
            'T3_US_TACC' : 'T1_US_FNAL_Disk',
            'T3_US_SDSC' : 'T1_US_FNAL_Disk',
            'T3_US_PSC' : 'T1_US_FNAL_Disk',
            'T3_US_OSG' : 'T1_US_FNAL_Disk',
            'T3_US_Colorado' : 'T1_US_FNAL_Disk',
            'T3_CH_CERN_HelixNebula' : 'T2_CH_CERN',
            'T3_CH_CERN_HelixNebula_REHA' : 'T2_CH_CERN'
            }
        self.addHocStorageS['T2_CH_CERN_T0'].add( 'T2_CH_CERN')
        self.addHocStorageS['T2_CH_CERN_AI'].add('T2_CH_CERN')

        for s,d in self.addHocStorage.items():
            self.addHocStorageS[s].add( d )

        self._map_SE_to_CE = defaultdict(set)
        self._map_CE_to_SE = defaultdict(set)
        for (phn,psn) in dataCache.get('site_storage'):
            self._map_SE_to_CE[phn].add(psn)
            self._map_CE_to_SE[psn].add(phn)
            self.addHocStorageS[psn].add( phn )
            if self.SE_to_CE(phn) == psn: continue
            if psn in ['T2_CH_CERN']: continue
            self.addHocStorage[psn] = phn

        ## list here the site which can accomodate high memory requests
        self.sites_memory = {}

        self.sites_mcore_ready = []
        mcore_mask = dataCache.get('mcore_ready')
        if mcore_mask:
            self.sites_mcore_ready = [s for s in mcore_mask['sites_for_mcore'] if s in self.sites_ready]
        else:
            pass

        for s in self.all_sites:
            ## will get it later from SSB
            self.cpu_pledges[s]=1
            ## will get is later from SSB
            self.disk[ self.CE_to_SE(s)]=0

        ## and get SSB sync
        self.fetch_ssb_info(talk=False)
        sites_space_override = UC.get('sites_space_override')


        self.fetch_queue_info()
        ## and detox info
        self.fetch_detox_info(talk=False, buffer_level=UC.get('DDM_buffer_level'), sites_space_override=sites_space_override)

        ## transform no disks in veto transfer
        for (dse,free) in self.disk.items():
            if free<=0:
                if not dse in self.sites_veto_transfer:
                    self.sites_veto_transfer.append( dse )

        ## and glidein info
        self.fetch_glidein_info(talk=False)

    def total_disk(self,what='disk'):
        s = 0
        for site in self.sites_ready:
            se = self.CE_to_SE(site)
            s += getattr(self,what).get(se,0)
        return s

    def availableSlots(self, sites=None):
        s=0
        for site in self.cpu_pledges:
            if sites and not site in sites: continue
            s+=self.cpu_pledges[site]
        return s

    def fetch_glidein_info(self, talk=True):
        self.sites_memory = dataCache.get('gwmsmon_totals')
        for site in self.sites_memory.keys():
            if not site in self.sites_ready:
                self.sites_memory.pop( site )

        #for_max_running = dataCache.get('gwmsmon_site_summary')
        for_better_max_running = dataCache.get('gwmsmon_prod_maxused')
        for site in self.cpu_pledges:
            new_max = self.cpu_pledges[site]
            #if site in for_max_running: new_max = int(for_max_running[site]['MaxWasCpusUse'] * 0.70)
            if site in for_better_max_running:
                new_max = int(for_better_max_running[site]['sixdays'])

            if new_max: #new_max > self.cpu_pledges[site] or True:
                self.cpu_pledges[site] = new_max

        for_site_pressure = dataCache.get('gwmsmon_prod_site_summary')
        self.sites_pressure = {}
        for site in self.cpu_pledges:#sites_ready:
            pressure = 0
            m = 0
            r = 0
            if site in for_site_pressure:
                #m = for_site_pressure[site]['MatchingIdle']
                #r = for_site_pressure[site]['Running']
                m = for_site_pressure[site]['CpusPending']
                r = for_site_pressure[site]['CpusInUse']
                if r:
                    pressure = m /float(r)
                else:
                    pressure = -1 ## does not matter
                    self.sites_pressure[site] = (m, r, pressure)

    def sitesByMemory( self, maxMem, maxCore=1):
        if not self.sites_memory:
            print "no memory information from glidein mon"
            return None
        allowed = set()
        for site,slots in self.sites_memory.items():
            #for slot in slots: print site,slot['MaxMemMB'],maxMem,slot['MaxMemMB']>= maxMem,slot['MaxCpus'],maxCore,int(slot['MaxCpus'])>=int(maxCore)
            if any([slot['MaxMemMB']>= maxMem and slot['MaxCpus']>=maxCore for slot in slots]):
                allowed.add(site)
        return list(allowed)

    def fetch_queue_info(self):
        self.queue = dataCache.get('site_queues')
        #for (k,v) in dataCache.get('site_queues').items():
        #    self.queue[k] = v

    def fetch_detox_info(self, talk=True, buffer_level=0.8, sites_space_override=None):
        ## put a retry in command line
        info = dataCache.get('detox_sites')
        #info = os.popen('curl --retry 5 -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt').read().split('\n')
        if len(info) < 15:
            ## fall back to dev
            info = dataCache.get('detox_sites', fresh = True)
            #info = os.popen('curl --retry 5 -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS-Dev/DetoxDataOps/SitesInfo.txt').read().split('\n')
            if len(info) < 15:
                print "detox info is gone"
                return
        pcount = 0
        read = False
        for line in info:
            if 'Partition:' in line:
                read = ('DataOps' in line)
                continue
            if line.startswith('#'): continue
            if not read: continue
            try:
                _,quota,taken,locked,site = line.split()
            except:
                sendLog('fetch_detox_info','Unable to read Detox DataOps report',level='critical')
                break
            ## bypass

            if 'MSS' in site: continue
            #queued = self.queue.get(site,0)
            #print site,self.queue.get(site,0)
            ## consider quota to be 80% of what's available
            queued_used = 0
            available = int(float(quota)*buffer_level) - int(locked) - int(queued_used)

            #### .disk = 80%*quota - locked : so it's the effective space
            #### .free_disk = the buffer space that there is above the 80% quota
            self.disk[site] = available if available >0 else 0
            ddm_free = int(float(quota) - int(locked) - self.disk[site])
            self.free_disk[site] = ddm_free if ddm_free>0 else 0
            if sites_space_override and site in sites_space_override:
                self.disk[site] = sites_space_override[site]

            self.quota[site] = int(quota)
            self.locked[site] = int(locked)


    def fetch_ssb_info(self,talk=True):
        ## and complement information from ssb
        columns= {
            'realCPU' : 'core_max_used',
            'prodCPU' : 'core_production',
            'CPUbound' : 'core_cpu_intensive'
            }

        all_data = {}
        for name,column in columns.items():
            if talk: print name,column
            try:
                all_data[name] =  dataCache.get('ssb_%s'% column) 
            except:
                print "cannot get info from ssb for",name
        _info_by_site = {}
        for info in all_data:
            for item in all_data[info]:
                site = item['name']
                if site.startswith('T3'): continue
                value = item[columns[info]]
                if not site in _info_by_site: _info_by_site[site]={}
                _info_by_site[site][info] = value

        if talk: print json.dumps( _info_by_site, indent =2 )

        if talk: print self.disk.keys()
        for (site,info) in _info_by_site.items():
            if talk: print "\n\tSite:",site
            ssite = self.CE_to_SE( site )
            tsite = site+'_MSS'
            #key_for_cpu ='prodCPU'
            key_for_cpu ='CPUbound'
            if key_for_cpu in info and site in self.cpu_pledges and info[key_for_cpu]:
                if self.cpu_pledges[site] < info[key_for_cpu]:
                    if talk: print site,"could use",info[key_for_cpu],"instead of",self.cpu_pledges[site],"for CPU"
                    self.cpu_pledges[site] = int(info[key_for_cpu])
                elif self.cpu_pledges[site] > 1.5* info[key_for_cpu]:
                    if talk: print site,"could correct",info[key_for_cpu],"instead of",self.cpu_pledges[site],"for CPU"
                    self.cpu_pledges[site] = int(info[key_for_cpu])


    def types(self):
        return ['sites_T1s','sites_T2s','sites_T3s']
        #return ['sites_with_goodIO','sites_T1s','sites_T2s','sites_mcore_ready']#,'sites_veto_transfer']#,'sites_auto_approve']

    def CE_to_SEs(self, ce ):
        if type(ce) in [list,set]:
            r = set()
            for c in ce:
                r.update( self.addHocStorageS[c])
            return list(r)
        else:
            return list( self.addHocStorageS[ce])

    def CE_to_SE(self, ce):
        if (ce.startswith('T1') or ce.startswith('T0')) and not ce.endswith('_Disk'):
            return ce+'_Disk'
        if ce in self._map_CE_to_SE:
            return sorted(self._map_CE_to_SE[ce])[0]
        else:
            if ce in self.addHocStorage:
                return self.addHocStorage[ce]
            else:
                return ce

    def SE_to_CEs(self, se):
        if se in self._map_SE_to_CE:
            return sorted(self._map_SE_to_CE[se])
        else:
            return [self.SE_to_CE(se)]

    def SE_to_CE(self, se):
        if se in self._map_SE_to_CE:
            return sorted(self._map_SE_to_CE[se])[0]

        if se.endswith('_Disk'):
            return se.replace('_Disk','')
        elif se.endswith('_MSS'):
            return se.replace('_MSS','')
        else:
            return se

    def pick_dSE(self, sites=None):
        return self._pick(sites, self.disk, and_fail=True)

    def _pick(self, sites, from_weights, and_fail=True):
        r_weights = {}
        if sites:
            for site in sites:
                if site in from_weights or and_fail:
                    r_weights[site] = from_weights[site]
                else:
                    r_weights = from_weights
                    break
        else:
            r_weights = from_weights

        return r_weights.keys()[self._weighted_choice_sub(r_weights.values())]

    def _weighted_choice_sub(self,ws):
        if sum(ws)>0:
            rnd = random.random() * sum(ws)
            #print ws
            for i, w in enumerate(ws):
                rnd -= w
                if rnd <= 0:
                    return i
        else:
            rnd = random.random() * len(ws)  ## add a random selection if all sites are 0
            for i, w in enumerate(ws):
                rnd -= 1
                if rnd <= 0:
                    return i
        print "could not make a choice from ",ws,"and",rnd
        return None


    def pick_CE(self, sites):
        #print len(sites),"to pick from"
        #r_weights = {}
        #for site in sites:
        #    r_weights[site] = self.cpu_pledges[site]
        #return r_weights.keys()[self._weighted_choice_sub(r_weights.values())]
        return self._pick(sites, self.cpu_pledges)

class remainingDatasetInfo:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.remainingDatasetInfo
    
    def __del__(self):
        self.purge(60)

    def purge(self, grace=30):
        now = time.mktime(time.gmtime()) - (grace*24*60*60)
        self.db.delete_many({'time': { '$lt': then}})

    def clean(self):
        existings = self.db.find()
        for o in existings:
            self.db.delete_one({'_id' :o['_id']})
 
    def sync(self, site=None):
        if not site:
            print "synching with all site possible"
            sites = []
            try:
                r = json.loads(eosRead('%s/remaining.json'%( monitor_dir)))
                sites = r.keys()
            except:
                try:
                    for fn in filter(None,os.popen('ls -1 %s/remaining_*.json | sort '%monitor_dir).read().split('\n')):
                        site = fn.split('_',1)[-1].split('.')[0]
                        if not any([site.endswith(v) for v in ['_MSS','_Export']]):
                            sites.append( site )
                except:
                    pass
            for site in sites:
                self.sync( site )
        else:
            print "synching on site",site
            remaining_reasons = json.loads(eosRead('%s/remaining_%s.json'%(monitor_dir,site)))
            self.set(site, remaining_reasons)

    def set(self, site, info):
        if not info: return
        existings = [o['dataset'] for o in self.db.find({'site' : site})]
        updatings = info.keys()
        n = time.gmtime()
        now = time.mktime( n )
        nows = time.asctime( n )
        ## drop existing datasets that are not to be updated
        for dataset in sorted(set(existings) - set(updatings)):
            self.db.delete_one({'site' : site, 'dataset' : dataset})
        for dataset,dinfo in info.items():
            content = { 'site' : site,
                        'dataset' : dataset,
                        'reasons' : dinfo.get('reasons',[]),
                        'size' : dinfo.get('size',0),
                        'time' : now,
                        'date' : nows
                    }
            self.db.update_one({'site' : site, 'dataset' : dataset},
                               {"$set": content},
                               upsert = True)

    def sites(self):
        r_sites = sorted(set([r['site'] for r in self.db.find() ]))
        return r_sites

    def get(self, site):
        existings = self.db.find({'site' : site})
        r = {}
        for o in existings:
            r[o['dataset']] = { 'size' : o.get('size',0),
                                'reasons' : o.get('reasons',[])}
        return r

    def tell(self, site):
        info = self.get(site)
        print json.dumps(info, indent=2)

def global_SI(a=None):
    if a or not global_SI.instance:
        print "making a new instance of siteInfo",a
        global_SI.instance = siteInfo(a)
    return global_SI.instance
global_SI.instance = None

class reportInfo:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.reportInfo

    def purge(self ,wfn=None, grace=None, wipe=False):
        ## clean in a way of another
        if wfn:
            self.db.delete_many( {'workflow' : wfn} )
        if grace:
            then = time.mktime( time.gmtime()) - (grace*24*60*60) ## s
            self.db.delete_many( { 'time': { '$lt': then } } )
        if wipe == True:
            if raw_input('wipe repor db?').lower() in ['y','yes']:
                for o in self.db.find():
                    self.db.delete_one({'_id': o['_id']})
    
    def get(self, wfn , strip=True):
        doc = self.db.find_one({'workflow' : wfn})
        if strip: doc.pop('_id')
        return doc

    def _convert( self, d ):
        d = copy.deepcopy(d)
        ## convert recursively the types that cannot go in as is
        for k,v in d.items():
            if '.' in k:
                d.pop(k)
                k = k.replace('.','__dot__')
                #print k,"with a dot"
            if type(v) == set:
                d[k] = list(v)
            elif type(v) in [dict, defaultdict]:
                d[k] = self._convert(v)
            else:
                ## for re-keying
                d[k] =v 

        return dict(d)

    def _put(self, updating_doc):
        now = time.gmtime()
        date = time.asctime( now )
        now = time.mktime( now )
        updating_doc = self._convert( updating_doc )
        updating_doc.update({'time' : now, 'date' : date})
        #print updating_doc
        exist = self.db.find_one({'workflow' : updating_doc.get('workflow')})
        if exist:
            # nested info update and pop from new doc
            for nested in ['tasks']:
                new_info = updating_doc.pop(nested) if nested in updating_doc else {}
                for k in sorted(exist.get(nested,{}).keys()+new_info.keys()):
                    exist.setdefault(nested,{}).setdefault(k,{}).update( new_info.get(k,{}))

            #root info update
            exist.update( updating_doc )
            self.db.update_one({'workflow' : updating_doc.get('workflow')},
                               {'$set': exist})
            
        else:
            self.db.insert_one( updating_doc )

    def set_IO(self, wfn, IO):
        doc = { 'workflow' : wfn }
        doc.update( IO )
        self._put( doc )
        
    def set_errors(self, wfn, task, errors):
        self._set_for_task( wfn, task, errors, 'errors')

    def set_blocks(self, wfn, task, blocks):
        self._set_for_task( wfn, task, blocks, 'needed_blocks')

    def set_files(self, wfn, task, files):
        self._set_for_task( wfn, task, files, 'files')

    def set_ufiles(self, wfn, task, files):
        self._set_for_task( wfn, task, files, 'ufiles')

    def set_missing(self, wfn, task, missing):
        self._set_for_task( wfn, task, missing, 'missing')

    def set_logs(self, wfn, task, logs):
        self._set_for_task( wfn, task, logs, 'logs')

    def _set_for_task( self, wfn, task, content , field_name):
        task = task.split('/')[-1]
        doc = { 'workflow' : wfn,
                'tasks' : { task : {field_name : content}}}
        self._put( doc )

class cacheInfo:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.cacheInfo

    def get(self, key, no_expire=False):
        now = time.mktime(time.gmtime())
        o =self.db.find_one({'key':key})
        if o:
            if no_expire or (o['expire'] > now):
                if not 'data' in o:
                    return self.from_file(key)
                else:
                    print "cache hit",key
                    return o['data']
            else:
                print "expired doc",key
                return None
        else:
            print "cache miss",key
            return None

    def _file_key(self, key):
        cache_file = '{}/{}'.format(cache_dir, key.replace('/','_'))
        return cache_file

    def from_file(self, key):
        fn = self._file_key(key)
        if os.path.isfile( fn):
            print "file cache hit",key
            return json.loads(open(fn).read())
        else:
            print "file cachemiss",key
            return None
    def store(self, key, data, lifetime_min=10):
        import pymongo
        now = time.mktime(time.gmtime())
        content = {'data': data,
                   'key' : key,
                   'time' : int(now),
                   'expire' : int(now + 60*lifetime_min),
                   'lifetime' : lifetime_min}
        try:
            self.db.update_one({'key': key},
                               {"$set": content},
                               upsert = True)
        except (pymongo.errors.WriteError,pymongo.errors.DocumentTooLarge) as e:
            print ("cannot go in mongo. in file instead")
            open(self._file_key(key),'w').write( json.dumps( content.pop('data') ))
            self.db.update_one({'key': key},
                               {"$set": content},
                               upsert = True)
        except Exception as e:
            print type(e)
            print str(e)

    def purge(self, grace = 2):
        limit = time.mktime(time.gmtime())
        # delete all documents with passed expiration time
        # by more than <grace> days
        limit -= grace*24*60*60
        self.db.delete_many({'expire' : { '$lt' : limit}})

class closeoutInfo:
    def __init__(self):
        self.owner = "%s-%s"%( socket.gethostname(), os.getpid())
        self.removed_keys = set()
        self.client = mongo_client()
        self.db = self.client.unified.closeoutInfo
        self.record = {}


    def pop(self, wfn):
        if wfn in self.record:
            self.record.pop(wfn)
        self.removed_keys.add( wfn )
        while self.db.find_one({'name' : wfn}):
            self.db.delete_one({'name' : wfn})

    def update(self, wfn ,record):
        put_record = self.db.find_one({'name' : wfn})
        if not put_record: put_record={}
        put_record.update( record )
        self.db.update_one( {'name' : wfn},
                            {"$set": put_record},
                            upsert = True)
        self.record[wfn] = put_record
        
    def get(self, wfn):
        if not wfn in self.record:
            record = self.db.find_one({'name' : wfn})
            if record:
                record.pop('name')
                record.pop('_id')
                self.record[wfn] = record
            else:
                return None
        return self.record[wfn]

    def table_header(self):
        text = """<table border=1>
<thead><tr>
<th>workflow</th>
<th>OutputDataSet</th>
<th>%Compl</th>
<th>acdc</th>
<th>Events</th>
<th>Lumis</th>
<th>dbsF</th>
<th>dbsIF</th>
<th>phdF</th>
<th>Updated</th>
<th>Priority</th>
</tr></thead>"""
        return text

    def one_line(self, wf, wfo, count):
        if count%2:            color='lightblue'
        else:            color='white'
        text=""
        _ = self.get( wf ) ## cache the value
	if not wf in self.record: return ""
        tpid = self.record[wf]['prepid']
        pid = tpid.replace('task_','')

        ## return the corresponding html
        order = ['percentage','acdc','events','lumis','dbsFiles','dbsInvFiles','phedexFiles']#,'updated']
        wf_and_anchor = '<a id="%s">%s</a>'%(wf,wf)
        n_out = len(self.record[wf]['datasets'])
        o_and_c = [(out,self.record[wf]['datasets'][out].get('percentage')) for out in self.record[wf]['datasets'] ]
        o_and_c.sort( key = lambda o : o[1])
        for io,out in enumerate([o[0] for o in o_and_c]):
            text += '<tr bgcolor=%s>'%color

            ## a spanning row
            wf_text = ""
            wf_text += '<td rowspan="%d">%s<br>'%(n_out,wf_and_anchor)
            wf_text += '<a href="https://%s/reqmgr2/fetch?rid=%s" target="_blank">dts</a>'%(reqmgr_url, wf)
            wf_text += ', <a href="https://%s/reqmgr2/data/request/%s" target="_blank">wfc</a>'%(reqmgr_url, wf)
            wf_text += ', <a href="https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=full&reverse=0&reverse=1&npp=20&subtext=%s&sall=q" target="_blank">elog</a>'%(pid)
            wf_text += ', <a href=https://%s/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s>perf</a>'%(reqmgr_url, wf)
            wf_text += ', <a href=assistance.html#%s>%s</a>'%(wf,wfo.status)
            wf_text += '<br>'
            wf_text += '<a href="https://cms-unified.web.cern.ch/cms-unified/showlog/?search=%s" target="_blank">history</a>'%(pid)
            wf_text += ', <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s>%s</a>'%(tpid,tpid)
            wf_text += ', <a href=report/%s>report</a>'%(wf)
            #wf_text += ', <a href=%/report/%s>e_report</a>'%(unified_url_eos,wf)
            if 'ReReco' in tpid:
                wf_text += ', <a href=%s/datalumi/lumi.%s.html>lumis</a>'%(unified_url,tpid)
            wf_text += ', <a href="https://its.cern.ch/jira/issues/?jql=(text~%s OR text~task_%s ) AND project = CMSCOMPPR" target="_blank">jira</a>'%(pid,pid)
            wf_text += '</td>'
            if io==0: text += wf_text

            text+='<td>%s/<b>%s<b></td>'% tuple(out.rsplit('/',1))
            for f in order:
                if f in self.record[wf]['datasets'][out]:
                    value = self.record[wf]['datasets'][out][f]
                else:
                    value = "-NA-"
                    

                if f =='acdc':
                    text+='<td><a href=https://%s/reqmgr2/data/request?prep_id=%s&detail=false>%s</a></td>'%(reqmgr_url, tpid , value)
                elif f=='percentage':
                    frac = self.record[wf]['datasets'][out].get('fractionpass',0)
                    if value >= frac:
                        text+='<td>%s</td>'% value
                    else:
                        ## the one in bold will show the ones that need work
                        text+='<td><b>%s</b></td>'% value
                elif f=='events':
                    text+='<td>%s/%s</td>'% (display_N(self.record[wf]['datasets'][out].get('producedN','NA')), display_N(self.record[wf]['datasets'][out].get('expectedN','NA')))
                elif f=='lumis':
                    text+='<td>%s/%s</td>'% (display_N(self.record[wf]['datasets'][out].get('producedL','NA')), display_N(self.record[wf]['datasets'][out].get('expectedL','NA')))                    
                else:
                    text+='<td>%s</td>'% value
            u_text = '<td rowspan="%d">%s</td>'%( n_out, self.record[wf]['datasets'][out]['updated'])
            if io==0: text+=u_text
            p_text = '<td rowspan="%d">%s</td>'%( n_out, self.record[wf]['priority'])
            if io==0: text+=p_text
            text+='</tr>'
            wf_and_anchor = wf

        return text

    def html(self):
        self.summary()
        self.assistance()

    def summary(self):
        html = eosFile('%s/closeout.html'%monitor_dir,'w')
        html.write('<html>')
        html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/ target=_blank> logs</a> <br><br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))

        html.write( self.table_header() )

        from assignSession import session, Workflow
        for (count,wf) in enumerate(sorted([o['name'] for o in self.db.find()])):
            wfo = session.query(Workflow).filter(Workflow.name == wf).first()
            if not wfo: continue
            if not (wfo.status == 'away' or wfo.status.startswith('assistance')):
                print "Taking",wf,"out of the close-out record"
                self.pop( wf )
                continue
            html.write( self.one_line( wf, wfo , count) )

        html.write('</table>')
        html.write('<br>'*100) ## so that the anchor works ok
        html.write('bottom of page</html>')

        html.close() ## and copy to eos
        #self.save()

    def save(self):
        return
        ## all the rest is useless with pymongo
        ## gather all existing content
        while True:
            existings = glob.glob('%s/closedout.*.lock'%base_eos_dir)
            if existings:
                ## wait until there are no such files any more
                print len(existings),"existing content",sorted(existings)
                time.sleep(10)
            else:
                break

        ## lock everyone else from collecting info
        my_file = '%s/closedout.%s.lock'%(base_eos_dir,self.owner)
        os.system('env EOS_MGM_URL=root://eoscms.cern.ch eos touch %s'%my_file)

        ## write the information out to disk
        new_base_eos_dir = 'root://eoscms.cern.ch/'+base_eos_dir
        os.system('env EOS_MGM_URL=root://eoscms.cern.ch eos cp %s/closedout.json %s/closedout.json.last'%(new_base_eos_dir, new_base_eos_dir))

        ## merge the content
        try:
            old = json.loads(eosRead('%s/closedout.json'%base_eos_dir))
        except:
            old = {}

        for wf in old:
            if wf in self.removed_keys:
                print wf,"was removed in the process, skipping"
                continue
            update_to_old = False
            if wf not in self.record:
                update_to_old = True
            else:
                # the content is in both place
                old_ts = [dsi['timestamp'] for ds,dsi in old[wf].get('datasets',{}).items()]
                new_ts = [dsi['timestamp'] for ds,dsi in self.record[wf].get('datasets',{}).items()]
                if not new_ts:
                    ##weird
                    update_to_old = True
                else:
                    if old_ts:
                        #then we can make the comparison
                        if max(old_ts) > max(new_ts):
                            ## the existing timestamp is bigger than the one we have :keep the old one
                            update_to_old = True

            if update_to_old:
                self.record[wf] = old[wf]

        out = eosFile('%s/closedout.json'%base_eos_dir)
        out.write( json.dumps( self.record , indent=2 ) )
        out.close()
        time.sleep(100)

        os.system('env EOS_MGM_URL=root://eoscms.cern.ch eos rm -f %s'% my_file )

    def assistance(self):
        from assignSession import session, Workflow
        wfs = session.query(Workflow).filter(Workflow.status.startswith('assistance')).all()
        short_html = eosFile('%s/assistance_summary.html'%monitor_dir,'w')
        html = eosFile('%s/assistance.html'%monitor_dir,'w')
        html.write("""
<html>
<head>
<META HTTP-EQUIV="refresh" CONTENT="900">
</head>
""")
        short_html.write("""
<html>
<head>
<META HTTP-EQUIV="refresh" CONTENT="900">
</head>
""")

        short_html.write('<a href=logs/checkor/last.log target=_blank> log</a> <a href=logs/recoveror/last.log target=_blank> postlog</a> <br>')
        html.write('<a href=logs/checkor/last.log target=_blank> log</a> <a href=logs/recoveror/last.log target=_blank> postlog</a><br>')

        html.write('<a href=assistance_summary.html> Summary </a> <br>')
        short_html.write('<a href=assistance.html> Details </a> <br>')


        ## a few lines of explanation
        explanation="""
Updated on %s (GMT) <br>
<br><ul>
<li> <b>custodial</b> : one the output dataset is waiting for the subscription to tape to be made. The request has been created already.<font color=green>(Automatic)</font></li>
<li> <b>parentcustodial</b> : the parent of the dataset is not set on tape <font color=red>(Operator)</font></li>
<li> <b>recovering</b> : there is at least one active ACDC for the worflow <font color=orange>(Wait)</font></li>
<li> <b>recovered</b> : there is at least one inactive ACDC for the workflow <font color=green>(Automatic)</font>
<li> <b>recovery</b> : the final statistics of the sample is not passing the requirements <font color=green>(Automatic)</font> </li>
<li> <b>announce</b> : the final statistics of the sample is enough to announce the outputs <font color=green>(Automatic)</font> </li>
<li> <b>announced</b> : the final statistics of the sample is enough and the outputs are announced <font color=green>(Automatic)</font> </li>
<li> <b>over100</b> : the final statistics is over 100%% <font color=red>(Operator)</font></li>
<li> <b>biglumi</b> : the maximum size of the lumisection in one of the output has been exceeded <font color=red>(Operator)</font></li>
<li> <b>smalllumi</b> : the size of the lumisection of one of the output is too small <font color=red>(Operator)</font></li>
<li> <b>bigoutput</b> : the maximum size for an output dataset to go to tape was exceeded (<font color=blue>Requester</font>/<font color=red>Operator)</font></li>
<li> <b>filemismatch</b> : there is a mismatch in the number of files in DBS and Phedex <font color=red>(Operator)</font></li>
<li> <b>agentfilemismatch</b> : there is a mismatch in the number of files in DBS and Phedex, and the grace period for the agent is not over <font color=green>(Automatic)</font></li>
<li> <b>duplicates</b> : duplicated lumisection have been found and need to be invalidated <font color=green>(Automatic)</font></li>
<li> <b>inconsistent</b> : the output of the recovery workflow is not consistent with the main workflow <font color=red>(Operator)</font></li>
<li> <b>manual</b> : no automatic recovery was possible <font color=red>(Operator)</font></li>
<li> <b>on-hold</b> : there was a notification made that a decision needs to be taken to move forward. Check the JIRA for details <font color=blue>(Requester</font>/<font color=red>Operator)</font></li>
</ul><br>
"""%( time.asctime(time.gmtime()))

        html.write( explanation )
        short_html.write( explanation )

        assist = defaultdict(list)
        for wfo in wfs:
            assist[wfo.status].append( wfo )

        header="<ul>"
        for status in sorted(assist.keys()):
            header += '<li> <a href="#%s">%s</a> in status %s</li>\n'%(status, len(assist[status]), status)
        header+="</ul>"
        html.write( header )
        short_html.write( header )


        for status in sorted(assist.keys()):
            html.write("<a name=%s>Workflow in status <b> %s </b> (%d)</a>"% (status,status, len(assist[status])))
            html.write( self.table_header() )
            short_html.write("""
<a name="%s">Workflow in status <b> %s </b> (%d)</a>
<table border=1>
<thead>
<tr>
<th> workflow </th> <th> output dataset </th><th> completion </th>
</tr>
</thead>
"""% (status,status, len(assist[status])))
            prio_ordered_wf = []
            for (count,wfo) in enumerate(assist[status]):
                if not self.get( wfo.name ):
                    continue
                prio = self.record[wfo.name]['priority']
                prio_ordered_wf.append( (prio, wfo) )
            ## sort by priority
            prio_ordered_wf.sort( key = lambda item:item[0] ,reverse=True )
            ## and use only the wfo
            prio_ordered_wf = [ item[1] for item in prio_ordered_wf ]
            for (count,wfo) in enumerate(prio_ordered_wf):
                ## change the line color for visibility
                if count%2:            color='lightblue'
                else:            color='white'

                if not wfo.name in self.record:
                    print "wtf with",wfo.name
                    continue
                html.write( self.one_line( wfo.name, wfo, count))
                for out in self.record[wfo.name]['datasets']:
                    line ="""
<tr bgcolor=%s>
<td> <a id=%s>%s</a> </td><td> %s </td><td> <a href=closeout.html#%s>%s</a> </td>
</tr>
"""%( color,
      wfo.name,wfo.name,
      out,
      wfo.name,
      self.record[wfo.name]['datasets'][out]['percentage'],

      )

                    short_html.write(line)


            html.write("</table><br><br>")
            short_html.write("</table><br><br>")

        short_html.write("<br>"*100)
        short_html.write("bottom of page</html>")
        html.write("<br>"*100)
        html.write("bottom of page</html>")

        # close and put on eos
        html.close()
        short_html.close()

def getFileBlock( file_name ):
    return runWithRetries(_getFileBlock, [file_name],{})

def _getFileBlock( f ):
    ## this function should get in one shot all files and blocks
    ## cache it, return the block of that file
    ## if not found, get that file info again and amend
    dbsapi = DbsApi(url=dbs_url)
    r = dbsapi.listFileArray( logical_file_name = f, detail=True)
    return [df['block_name'] for df in r][0] if r else None

def getDatasetFileArray( dataset, validFileOnly=0, detail=False, cache_timeout=30, use_array=False):
    ## check for cache content
    call = 'listFileArray' if use_array else 'listFile'
    cache_key = 'dbs_{}_{}'.format( call , dataset )
    cache = cacheInfo()
    cached = cache.get(cache_key)
    
    if cached:
        print ("{} {} taken from cache".format(call, dataset ))
        all_files = cached
    else:
        dbsapi = DbsApi(url=dbs_url)
        if use_array:
            all_files = dbsapi.listFileArray( dataset= dataset, detail=True)
        else:
            all_files = dbsapi.listFiles( dataset = dataset, detail = True)
        cache.store( cache_key, all_files)
    
    if validFileOnly:
        all_files = [f for f in all_files if f['is_file_valid']==1]
    if not detail:
        keys= ['logical_file_name','is_file_valid']
        all_files = [ dict([ (k,v) for k,v in f.items() if k in keys]) for f in all_files]
    return all_files


def getDatasetBlocks( dataset, runs=None, lumis=None):
    return runWithRetries(_getDatasetBlocks, [dataset],{'runs':runs,'lumis':lumis})
def _getDatasetBlocks( dataset, runs=None, lumis=None):
    dbsapi = DbsApi(url=dbs_url)
    all_blocks = set()
    if lumis:
        print "Entering a heavy check on block per lumi"
        for run in lumis:
            try:
                ## to be fixed, if run==1, this call fails. one needs to provide the following
                # Exception in listFileArray 'Invalid input: files API does not supprt run_num=1 without logical_file_name.'
                ### so get first the list of files, then make the call
                all_files = dbsapi.listFileArray( dataset = dataset, lumi_list = lumis[run], run_num=int(run), detail=True)
            except Exception as e:
                print "Exception in listFileArray",str(e)
                all_files = []
            print len(all_files)
            all_blocks.update( [f['block_name'] for f in all_files])

    elif runs:
        for run in runs:
            r=3
            while r>0:
                r-=1
                try:
                    all_blocks.update([b['block_name'] for b in dbsapi.listBlocks(dataset = dataset, run_num = int(run))])
                    break
                except Exception as e:
                    time.sleep(1)
            if r==0:
                raise e


    if runs==None and lumis==None:
        all_blocks.update([item['block_name'] for item in dbsapi.listBlocks(dataset= dataset) ])

    return list( all_blocks )

def getDatasetPresence( url, dataset, complete='y', only_blocks=None, group=None, vetoes=None, within_sites=None):
    try:
        return try_getDatasetPresence( url, dataset, complete, only_blocks, group, vetoes, within_sites)
    except Exception as e:
        #sendEmail("fatal exception in getDatasetPresence",str(e))
        sendLog('getDatasetPresence','fatal exception in getDatasetPresence for %s\n%s'%( dataset, str(e)), level='critical')
        return {}

def try_getDatasetPresence( url, dataset, complete='y', only_blocks=None, group=None, vetoes=None, within_sites=None):
    if vetoes==None:
        vetoes = ['MSS','Buffer','Export']
    #print "presence of",dataset
    dbsapi = DbsApi(url=dbs_url)
    try:
        all_blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    except Exception as e:
	print("dbsapi.listBlockSummaries failed on {}".format(dataset))
	print(str(e))
	raise
    #print json.dumps( all_blocks, indent=2)
    all_block_names=set([block['block_name'] for block in all_blocks])
    #print sorted(all_block_names)
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset=%s'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['block']


    locations=defaultdict(set)
    blocks_in_phedex = set()
    size_in_phedex = 0
    for item in items:
        for replica in item['replica']:
            blocks_in_phedex.add( item['name'] )
            if not any(v in replica['node'] for v in vetoes):
                #if replica['node'] == 'T2_US_Nebraska':                    print item['name']
                if within_sites and not replica['node'] in within_sites: continue
                if replica['group'] == None: replica['group']=""
                if complete and not replica['complete']==complete: continue
                #if group!=None and replica['group']==None: continue
                if group!=None and not replica['group'].lower()==group.lower(): continue
                locations[replica['node']].add( item['name'] )
                #print "\t",replica['node'],item['name']
                if item['name'] not in all_block_names and not only_blocks:
                    print item['name'],'not yet injected in dbs, counting anyways'
                    all_block_names.add( item['name'] )
                    size_in_phedex += item['bytes']


    if set(all_block_names) != set(blocks_in_phedex):
        missing_in_phedex = sorted(set(all_block_names) - set(blocks_in_phedex))
        missing_in_dbs = sorted(set(blocks_in_phedex) - set(all_block_names))
        is_valid = False
        for block in missing_in_phedex:
            block_info = dbsapi.listFileSummaries( block_name = block, validFileOnly=1)
            for bi in block_info:
                if bi['num_file'] !=0:
                    is_valid = True
                    break
            if is_valid:
                print block,"is valid"
                continue
            else:
                all_block_names.remove( block )
        print "Mismatch in phedex/DBS blocks"
        missing_in_phedex = sorted(set(all_block_names) - set(blocks_in_phedex))
        missing_in_dbs = sorted(set(blocks_in_phedex) - set(all_block_names))
        if missing_in_phedex: print missing_in_phedex,"missing in phedex"
        if missing_in_dbs: print missing_in_dbs,"missing in dbs"

    if only_blocks:
        all_block_names = filter( lambda b : b in only_blocks, all_block_names)
        full_size = sum([block['file_size'] for block in all_blocks if (block['block_name'] in only_blocks)])
        #print all_block_name
        #print [block['block_name'] for block in all_blocks if block['block_name'] in only_blocks]
    else:
        full_size = sum([block['file_size'] for block in all_blocks if block['block_name'] in all_block_names])
    if not full_size:
        print dataset,"is nowhere"
        return {}
    full_size = full_size+size_in_phedex
    #print locations.items()
    presence={}
    for (site,blocks) in locations.items():
        site_size = sum([ block['file_size'] for block in all_blocks if (block['block_name'] in blocks and block['block_name'] in all_block_names)])
        ### print site,blocks,all_block_names
        #presence[site] = (set(blocks).issubset(set(all_block_names)), site_size/float(full_size)*100.)
        presence[site] = (set(all_block_names).issubset(set(blocks)), site_size/float(full_size)*100.)
#        if site =='T2_US_Nebraska' and False:
        """
        print site,
        print set(all_block_names) - set(blocks)
        print '\n'.join( sorted( all_block_names ))
        print site
        print '\n'.join( sorted( blocks ))
        """
    #print json.dumps( presence , indent=2)
    return presence

def getDatasetSize(dataset):
    return runWithRetries(_getDatasetSize, [dataset],{})
def _getDatasetSize(dataset):
    dbsapi = DbsApi(url=dbs_url)
    blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    ## put everything in terms of GB
    return sum([block['file_size'] / (1024.**3) for block in blocks])

def getDatasetEventsAndLumis(dataset, blocks=None):
    try:
        r = try_getDatasetEventsAndLumis( dataset, blocks)
    except:
        try:
            time.sleep(2)
            r = try_getDatasetEventsAndLumis( dataset, blocks)
        except Exception as e:
            print "fatal exception in getDatasetEventsAndLumis",dataset,blocks
            #sendEmail("fatal exception in getDatasetEventsAndLumis",str(e)+dataset)
            sendLog('getDatasetEventsAndLumis','fatal execption in processing getDatasetEventsAndLumis for %s \n%s'%( dataset, str(e)), level='critical')
            r = 0,0
    return r


def try_getDatasetEventsAndLumis(dataset, blocks=None):
    dbsapi = DbsApi(url=dbs_url)
    all_files = []
    if blocks:
        for b in blocks:
            all_files.extend( dbsapi.listFileSummaries( block_name = b  , validFileOnly=1))
    else:
        all_files = dbsapi.listFileSummaries( dataset = dataset , validFileOnly=1)
    if all_files == [None]:        all_files = []

    all_events = sum([f['num_event'] for f in all_files])
    all_lumis = sum([f['num_lumi'] for f in all_files])
    return all_events,all_lumis


def getDatasetRuns(dataset):
    dbsapi = DbsApi(url=dbs_url)
    reply = dbsapi.listRuns(dataset=dataset)
    runs = []
    for run in reply:
        if type(run['run_num']) is list:
            runs.extend(run['run_num'])
        else:
            runs.append(run['run_num'])
    return runs

def getFilesWithLumiInRun(dataset, run):
    dbsapi = DbsApi(url=dbs_url)
    start = time.mktime(time.gmtime())
    try:
        reply = dbsapi.listFiles(dataset=dataset, detail=True, run_num=run, validFileOnly=1) if run!=1 else dbsapi.listFiles(dataset=dataset, detail=True,validFileOnly=1)
    except:
        try:
            reply = dbsapi.listFiles(dataset=dataset, detail=True, run_num=run, validFileOnly=1) if run!=1 else dbsapi.listFiles(dataset=dataset, detail=True,validFileOnly=1)
        except:
            sendLog('getFilesWithLumiInRun','Fatal exception in running dbsapi.listFiles for %s %s '% (dataset,run), level='critical')
            reply = []

    files = [f['logical_file_name'] for f in reply if f['is_file_valid'] == 1]
    start = 0
    bucket = 100
    rreply = []
    while True:
        these = files[start:start+bucket]
        if len(these)==0: break
        rreply.extend( dbsapi.listFileLumiArray(logical_file_name=these,run_num=run) if run!=1 else dbsapi.listFileLumiArray(logical_file_name=these))
        start+=bucket
    return rreply

class duplicateAnalyzer:
    def __init__(self):
        """
        credits to whoever implemented this in the first place
        """
    def _buildGraph(self, lumis):
        graph = {}

        for lumi in lumis:
            files = lumis[lumi]
            #text lines with file names
            f1 = files[0]
            f2 = files[1]
            #create edge (f1, f2)
            if f1 not in graph:
                graph[f1] = {}
            if f2 not in graph[f1]:
                graph[f1][f2] = 0
                graph[f1][f2] += 1
            #create edge (f2, f1)
            if f2 not in graph:
                graph[f2] = {}
            if f1 not in graph[f2]:
                graph[f2][f1] = 0
                graph[f2][f1] += 1
        return graph

    def _hasEdges(self,graph):
        """
        True if at least one edge is between to vertices,
        that is, there is at least one lumi present in two different
        files
        """
        for v in graph.values():
            if v:
                return True
        return False

    def _colorBipartiteGraph(self, graph, events):
        """
        Removes duplication by identifying a bipartite graph and removing
        the smaller side
        """
        red = set()
        green = set()

        for f1, f2d in graph.items():
            f1red = f1 in red
            f1green = f1 in green
            for f2 in f2d.keys():
                f2red = f2 in red
                f2green = f2 in green
                #both have no color
                if not(f1red or f1green or f2red or f1green):
                    red.add(f1)
                    green.add(f2)
                #some has two colors:
                elif (f1red and f1green) or (f2red and f2green):
                    print "NOT BIPARTITE GRAPH"
                    raise Exception("Not a bipartite graph, cannot use this algorithm for removing")
                #have same color
                elif (f1red and f2red) or (f1green and f2green):
                    print "NOT BIPARTITE GRAPH"
                    raise Exception("Not a bipartite graph, cannot use this algorithm for removing")

                #both are colored but different
                elif f1red != f2red and f1green != f2green:
                    continue
                #color opposite
                elif f1red:
                    green.add(f2)
                elif f1green:
                    red.add(f2)
                elif f2red:
                    green.add(f1)
                elif f2green:
                    green.add(f1)
        #validate against the # of events of the files
        eventsRed = sum(events[f] for f in red)
        eventsGreen = sum(events[f] for f in green)
        if eventsRed < eventsGreen:
            return list(red)
        else:
            return list(green)

    def _deleteSmallestVertexFirst(self, graph, events):
        """
        Removes duplication by deleting files in a greedy fashion.
        That is, removing the files smallest files
        first, and keep doing so until there is no edge on the graph (no lumi
        in two different files)
        """
        files = []
        print "Initial files:", len(graph)
        #sort by number of events
        ls = sorted(graph.keys(), key=lambda x: events[x])
        #quadratic first
        while self._hasEdges(graph):
        #get smallest vertex
            minv = ls.pop()
            #remove minv from all its adjacent vertices
            for v in graph[minv]:
                del graph[v][minv]
            #remove maxv entry
            del graph[minv]
            files.append(minv)

        #print "End Files:",len(graph), "Invalidated:",len(graph)
        return files

    def files_to_remove(self, files_per_lumis):
        lumi_count_per_file = defaultdict(int)
        for rl,fns in files_per_lumis.items():
            for fn in fns: lumi_count_per_file[fn]+=1
        bad_lumis = dict([(rl,files) for rl,files in files_per_lumis.items() if len(files)>1])
        graph = self._buildGraph(bad_lumis)
        try:
            files = self._colorBipartiteGraph(graph, lumi_count_per_file)
        except:
            #print "not with colorBipartiteGraph"
            files = self._deleteSmallestVertexFirst(graph, lumi_count_per_file)
        return files


def getDatasetLumisAndFiles(dataset, runs=None, lumilist=None, with_cache=False,force=False, check_with_invalid_files_too=False):
    if runs and lumilist:
        print "should not be used that way"
        return {},{}
    lumis =set()
    if lumilist:
        for r in lumilist:
            lumis.update( [(r,l) for l in lumilist[r]])

    now = time.mktime(time.gmtime())
    dbsapi = DbsApi(url=dbs_url)
    cache_key = 'json_lumis_{}'.format( dataset )
    cache = cacheInfo()
    cached = None
    if force: with_cache=False
    if with_cache:
        cached = cache.get( cache_key )
        
    if not cached:
        ## do the full query
        
        print "querying getDatasetLumisAndFiles", dataset
        full_lumi_json = defaultdict(set)
        files_per_lumi = defaultdict(set) ## the revers dictionnary of files by r:l

        class getFilesFromBlock(threading.Thread):
            def __init__(self, b, dbs=None):
                threading.Thread.__init__(self)
                self.b = b
                self.a = dbs if dbs else DbsApi(url=dbs_url)
                self.res = None
            
            def run(self):
                self.res = self.a.listFileLumis( block_name = self.b , validFileOnly=int(not check_with_invalid_files_too))
                            
        threads = []
        all_blocks = dbsapi.listBlocks( dataset = dataset )
        for block in all_blocks:
            threads.append( getFilesFromBlock( block.get('block_name') ))

        run_rthreads = ThreadHandler( threads = threads,
                                      n_threads = 10,
                                      label = 'getDatasetLumisAndFiles')
        run_rthreads.start()
        while run_rthreads.is_alive():
            time.sleep(1)

        for t in run_rthreads.threads:
            if not t.res: continue
            for f in t.res:
                full_lumi_json[ str(f['run_num']) ].update( f['lumi_section_num'])
                for lumi in f['lumi_section_num']:
                    files_per_lumi['{}:{}'.format(f['run_num'], lumi)].add( f['logical_file_name'])
        for k,v in full_lumi_json.items():
            full_lumi_json[k] = list(v)
        for k,v in files_per_lumi.items():
            files_per_lumi[k] = list(v)

        cache.store( cache_key,
                     {'files' : files_per_lumi,
                      'lumis' : full_lumi_json},
                     lifetime_min =600)
    else:
        files_per_lumi = cached['files']
        full_lumi_json = cached['lumis']

    ## need to filter on the runs
    lumi_json = dict([(int(k),v) for (k,v) in full_lumi_json.items()])
    files_json = dict([(tuple(map(int,k.split(":"))),v) for (k,v) in files_per_lumi.items()])
    if runs:
        lumi_json = dict([(int(k),v) for (k,v) in full_lumi_json.items() if int(k) in runs])
        files_json = dict([(tuple(map(int,k.split(":"))),v) for (k,v) in files_per_lumi.items() if int(k.split(':')[0]) in runs])
    elif lumilist:
        runs = map(int(lumilist.keys()))
        lumi_json = dict([(int(k),v) for (k,v) in full_lumi_json.items() if int(k) in runs])
        files_json = dict([(tuple(map(int,k.split(":"))),v) for (k,v) in files_per_lumi.items() if map(int,k.split(":")) in lumis])

    return lumi_json,files_json


def getDatasetLumis(dataset, runs=None, with_cache=False):
    l,f = getDatasetLumisAndFiles(dataset, runs=runs, lumilist=None, with_cache=with_cache)
    return l

def getDatasetEventsPerLumi(dataset):
    return runWithRetries(_getDatasetEventsPerLumi,[dataset],{})
def _getDatasetEventsPerLumi(dataset):
    event_count,lumi_count = getDatasetEventsAndLumis(dataset)
    if lumi_count:
        return float(event_count) / float(lumi_count)
    else:
        return 0.

def invalidateFiles( files ):
    all_OK = True
    conn  =  httplib.HTTPSConnection('dynamo.mit.edu', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    for fn in files:
        if not all_OK :break ## stop at the first file
        try:
            r1 = conn.request("GET", "/registry/invalidation/invalidate?item=%s"%(fn))
            r2 = conn.getresponse()
            res = json.loads(r2.read())
            all_OK = res['result'] == 'OK'
            print fn,"set for invalidation"
        except Exception as e:
            print str(e)
            print "could not set to invalidate", fn
            all_OK = False

    return all_OK

def findParent( dataset ):
    return runWithRetries( _findParent, [dataset], {})
def _findParent( dataset ):
    dbsapi = DbsApi(url=dbs_url)
    ret = dbsapi.listDatasetParents( dataset= dataset)
    parents = [r.get('parent_dataset',None) for r in ret]
    return parents

def setFileStatus(file_names, validate=True):
    dbswrite = DbsApi(url=dbs_url_writer)
    dbsapi = DbsApi(url=dbs_url)
    files = dbsapi.listFiles(logical_file_name = file_names, detail=True)
    for fn in files:
        status = fn['is_file_valid']
        if status != validate:
            ## then change the status
            print "Turning",fn['logical_file_name'],"to",validate
            dbswrite.updateFileStatus( logical_file_name= fn['logical_file_name'], is_file_valid = int(validate) )


def setDatasetStatus(dataset, status, withFiles=True):
    return runWithRetries(_setDatasetStatus, [dataset, status], {'withFiles':withFiles})
def _setDatasetStatus(dataset, status, withFiles=True):
    dbswrite = DbsApi(url=dbs_url_writer)

    new_status = getDatasetStatus( dataset )
    if new_status == None:
        ## means the dataset does not exist in the first place
        print "setting dataset status",status,"to inexistant dataset",dataset,"considered succeeding"
        return True

    file_status = -1
    if status in ['DELETED', 'DEPRECATED', 'INVALID']:
        file_status = 0
    else:
        file_status = 1
    max_try=3
    while new_status != status:
        dbswrite.updateDatasetType(dataset = dataset, dataset_access_type= status)
        new_status = getDatasetStatus( dataset )
        if withFiles and file_status!=-1:
            files = dbswrite.listFiles(dataset=dataset)
            for this_file in files:
                dbswrite.updateFileStatus(logical_file_name=this_file['logical_file_name'], is_file_valid=file_status)
        max_try-=1
        if max_try<0: return False
    return True

def getDatasetStatus(dataset):
    return runWithRetries(_getDatasetStatus,[dataset],{})
def _getDatasetStatus(dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs_url)
        # retrieve dataset summary
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*',detail=True)
        if len(reply):
            return reply[0]['dataset_access_type']
        else:
            return None

def getDatasets(dataset):
    count=5
    label = "getDatasets"
    while count>0:
        try:
            return _getDatasets(dataset)
        except Exception as e:
            print "[%d] Failed on %s with \n%s"%(count,label,str(e))
            time.sleep(5)
    raise Exception("Failed on %s with \n%s"%( label,str(e)))
    
def _getDatasets(dataset):
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs_url)

    # retrieve dataset summary
    _,ds,p,t = dataset.split('/')
    reply = dbsapi.listDatasets(primary_ds_name = ds,
                                processed_ds_name = p,
                                data_tier_name = t,
                                dataset_access_type='*')
    return reply

def getWorkLoad(url, wf ):
    try:
        return _getWorkLoad(url, wf )
    except:
        time.sleep(5)
        try:
            return _getWorkLoad(url, wf )
        except:
            print "failed twice to _getWorkLoad(url, wf )"
            return None
def getReqmgrInfo(url):
    conn = make_x509_conn(url)
    r1= conn.request("GET",'/reqmgr2/data/info', headers={"Accept":"*/*"})
    r2=conn.getresponse()
    data = json.loads(r2.read())
    return data['result']
    
def _getWorkLoad(url, wf ):
    conn = make_x509_conn(url)
    r1= conn.request("GET",'/reqmgr2/data/request/'+wf, headers={"Accept":"*/*"})
    r2=conn.getresponse()
    data = json.loads(r2.read())
    return data['result'][0][wf]

def getWorkflowByCampaign(url, campaign, details=False):
    conn = make_x509_conn(url)
    there = '/reqmgr2/data/request?campaign=%s'% campaign
    there += '&detail=true' if details else '&detail=false'
    r1=conn.request("GET",there , headers={"Accept":"*/*"})
    r2=conn.getresponse()
    data = json.loads(r2.read())['result']
    if details:
        ## list of dict
        r = []
        for it in data:
            r.extend( it.values())
        return r
    else:
        return data

def getWorkflowByOutput( url, dataset , details=False):
    conn = make_x509_conn(url)
    there = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byoutputdataset?key="%s"'%(dataset)
    if details:
        there+='&include_docs=true'
    r1=conn.request("GET",there)
    r2=conn.getresponse()
    data = json.loads(r2.read())
    items = data['rows']
    if details:
        return [item['doc'] for item in items]
    else:
        return [item['id'] for item in items]

def display_N( n ):
    if not str(n).isdigit(): return str(n)
    k,u = divmod( n, 1000)
    m,k = divmod( k, 1000)
    b,m = divmod( m ,1000)
    dis=""
    if b:
        dis += "%dB"%b
        return dis
    if m:
        dis += "%dM"%m
        return dis
    if k:
        dis += "%dK"%k
        return dis
    return str(n)

def display_time( sec ):
    if not sec: return sec

    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    dis=""
    if d:
        dis += "%d [d]"%d
    if h or d :
        dis += "%d [h] "%h
    if h or m or d:
        dis += "%d [m] "%m
    if h or m or s or d :
        dis += "%d [s]"%s

    return dis

def getWorkflowById( url, pid , details=False):
    conn = make_x509_conn(url)
    there = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byprepid?key="%s"'%(pid)
    if details:
        there+='&include_docs=true'
    r1=conn.request("GET",there)
    r2=conn.getresponse()
    data = json.loads(r2.read())
    items = data['rows']
    if details:
        return [item['doc'] for item in items]
    else:
        return [item['id'] for item in items]

def invalidate(url, wfi, only_resub=False, with_output=True):
    tries = 3
    while tries:
        tries-=1
        check = _invalidate(url, wfi, only_resub, with_output)
        if all(check):
            break
    return check

def _invalidate(url, wfi, only_resub=False, with_output=True):
    import reqMgrClient
    familly = wfi.getFamilly( and_self=True, only_resub=only_resub)
    outs = set()
    check = []
    for fwl in familly:
        print "checking wf family:", fwl['RequestName'], fwl['RequestStatus'], fwl['OutputDatasets']
        check.append(reqMgrClient.invalidateWorkflow(url, fwl['RequestName'], current_status=fwl['RequestStatus'], cascade=False))
        outs.update( fwl['OutputDatasets'] )
    if with_output:
        for dataset in outs:
            print "printing datasets again", dataset
            check.append(setDatasetStatus(dataset, 'INVALID'))
    check = [r in ['None',None,True] for r in check]
    return check

def forceComplete(url, wfi):
    import reqMgrClient
    familly = getWorkflowById( url, wfi.request['PrepID'] ,details=True)
    for member in familly:
        print "considering",member['RequestName'],"as force complete"
        ### if member['RequestName'] == wl['RequestName']: continue ## set himself out
        if member['RequestDate'] < wfi.request['RequestDate']: continue
        if member['RequestStatus'] in ['None',None]: continue
        ## then set force complete all members
        if member['RequestStatus'] in ['running-open','running-closed']:
            #sendEmail("force completing","%s is worth force completing\n%s"%( member['RequestName'] , percent_completions))
            print "setting",member['RequestName'],"force-complete"
            reqMgrClient.setWorkflowForceComplete(url, member['RequestName'])
        elif member['RequestStatus'] in ['acquired','assignment-approved']:
            print "rejecting",member['RequestName']
            reqMgrClient.invalidateWorkflow(url, member['RequestName'], current_status=member['RequestStatus'])

def agentInfoDB():
    client = mongo_client()
    return client.unified.agentInfo

def agent_speed_draining(db=None):
    if db is None:
        db = agentInfoDB()
    return set([a['name'] for a in db.find() if a.get('speeddrain',False)])

class agentInfo:
    def __init__(self, **args):
        self.url = args.get('url')
        self.verbose = args.get('verbose')
        self.busy_fraction = args.get('busy_fraction',0.8)
        self.idle_fraction = args.get('idle_fraction',0.1)
        self.speed_draining_fraction = args.get('speed_draining_fraction',0.05)
        self.open_draining_threshold = args.get('open_draining_threshold', 100)
        self.max_pending_cpus = args.get('max_pending_cpus', 10000000)
        self.wake_up_draining = args.get('wake_up_draining', False)
        ## keep some info in a local file
        self.db = agentInfoDB()
        self.all_agents = [a['name'] for a in self.db.find()]
        self.buckets = defaultdict(list)
        self.drained = set()
        self.wake_draining = False ## do not wake up agents that are on drain already
        self.release = defaultdict(set)
        self.m_release = defaultdict(set)
        self.ready = self.getStatus()
        if not self.ready:
            print "AgentInfo could not initialize properly"
        print json.dumps(self.content(), indent=2)

    def content(self):
        r= {}
        for a in self.db.find():
            a.pop('_id')
            an = a.pop('name')
            r[an] = a
        return r

    def speed_draining(self):
        return agent_speed_draining(self.db)
        
    def _getA(self, agent):
        a = self.db.find_one({'name' : agent})
        return a if a else {}

    def _get(self, agent, field, default=None):
        return self._getA(agent).get(field, default)

    def _update(self, agent, info):
        put_info = self._getA(agent)
        put_info.update( info )
        put_info['name'] = agent
        self.db.update_one( {'name': agent},
                            {"$set": put_info},
                            upsert = True)

    def agentStatus(self, agent):
        return self._get(agent, 'status', 'N/A')

    def checkTrello(self, sync_trello=None, sync_agents=None, acting=False):
        from TrelloClient import TrelloClient
        if not hasattr(self, 'tc'):
            self.tc = TrelloClient()
        tc = self.tc

        now,nows = self.getNow()

        for agent in self.all_agents:
            astatus = self._get(agent,'status')
            ti = tc.getCard( cn = agent)
            lid = tc.lists.get( astatus )
            lid_name = tc.getList(ln=lid).get('name')
            clid = ti.get('idList',None)
            if lid and clid and lid!=clid:
                print "there is a mismatch for agent",agent
                print "sync_trello",sync_trello,"sync_agents",sync_agents
                if sync_trello==True or (sync_trello and agent in sync_trello):
                    print "changing",agent,"into list",lid,lid_name
                    if acting:
                        tc.changeList( cn = agent, ln = lid )
                if sync_agents==True or (sync_agents and agent in sync_agents):
                    ## make the operation locally
                    print "Should operate on the agent",agent
                    do_drain = False
                    new_status = None
                    if clid == tc.lists.get('draining'):
                        ## put the agent in draining
                        do_drain = True
                        new_status = 'draining'
                    elif clid == tc.lists.get('running'):
                        ## should undrain the agent
                        do_drain = False
                        new_status = 'running'
                    elif clid == tc.lists.get('standby'):
                        ## should set the agent in standby
                        do_drain = True
                        new_status = 'standby'
                    elif clid == tc.lists.get('drained'):
                        do_drain = True
                        new_status = 'offline'
                    elif clid == tc.lists.get('offline'):
                        do_drain = True
                        new_status = 'offline'
                    else:
                        print "trello status",clid,"not recognized"
                        continue

                    ## operate the agent
                    print "wish to operate",agent,do_drain,new_status
                    if acting:
                        setAgentDrain(self.url, agent, drain=do_drain)
                        ## change the local information
                        self._update(agent, {'status' : new_status,
                                             'update' : now,
                                             'date' : nows }
                                     )


    def getStatus(self):
        all_agents_prod = getAllAgents(self.url).get('production',None)
        if not all_agents_prod:
            ## we cannot go on like that. there is something disruptive here
            print "cannot get production agent information"
            return False
        prod_info = dict([(a['agent_url'].split(':')[0], a) for a in all_agents_prod])
        all_agents_name = sorted(set(self.all_agents + prod_info.keys()))
        ## do you want to use this to make an alarm on agent in error for too long ?
        self.in_error = [a['agent_url'].split(':')[0] for a in all_agents_prod if a.get('down_components',[])]
        print "agents with errors",self.in_error

        now,nows = self.getNow()
        def drained( stats ):
            upload = stats.get('upload_status',{})
            condor = stats.get('condor_status',{})
            ## match 
            # {u'upload_status': {u'dbs_notuploaded': 0, u'dbs_open_blocks': 0, u'phedex_notuploaded': 0}, u'workflows_completed': True, u'condor_status': {u'idle': 0, u'running': 0}}
            if upload.get('dbs_notuploaded',1) == 0 and upload.get('dbs_open_blocks',1)==0 and upload.get('phedex_notuploaded',1)==0 and condor.get('idle',1)==0 and condor.get('running',1)==0:
                return True
            return False
        for agent in all_agents_name:
            linfo = self._getA(agent)
            pinfo = prod_info.get( agent, {})
            p_release = linfo.get('version',None)
            release = pinfo.get('agent_version',None)
            #is_drained = drained( pinfo.get('drain_stats',{}) )
            is_drained = False
                
            if self.verbose:
                print agent
                print linfo
                print pinfo
                print "drained",is_drained

            if release:
                self.release[ release ].add( str(agent) )
            if linfo:
                ## this was already known
                if pinfo:
                    ## and is in production
                    st = 'running'
                    if pinfo['drain_mode']:
                        if linfo['status'] == 'standby':
                            st = 'standby'
                        elif p_release and p_release != release:
                            ## new release means put in standby
                            st = 'standby'
                        else:
                            st = 'draining'
                            if is_drained:
                                self.drained.add( agent )
                            
                else:
                    ## and is gone from production
                    st = 'offline'
                self._update(agent, {'status': st})
            else:
                ## the agent is new here. let's assume it is in standby if in drain
                st = 'running'
                if pinfo['drain_mode']:
                    st = 'standby'
                    ## for the first time
                    print "A new agent in the pool",agent,"setting",st

                ## add it
                self._update(agent, {'status' : st,
                                     'update' : now,
                                     'date' : nows }
                         )
                if self.verbose:
                    print self._getA(agent)

            if release:
                self._update(agent, {'version': release})

        for a in self.all_agents:
            self.buckets[self._get(a, 'status')].append( a )

        if not self.buckets.get('standby',[]):
            msg = "There are no agent in standby!!"
            sendLog('agentInfo', msg, level='critical')

        if self.verbose:
            print json.dumps( self.buckets, indent=2)

        return True

    def getNow(self):
        now = time.gmtime()
        nows = time.asctime( now )
        now = time.mktime( now )
        return now,nows

    def change_status(self, agent, st):
        now,nows = self.getNow()
        self._update(agent, { 'status' : st,
                              'update': now,
                              'date' : nows
                          })

    def flag_standby(self, agent):
        if self._getA(agent).get('status',None) == 'draining':
            if self.verbose:
                print "Able to set",agent,"in standby"
            self.change_status( agent, 'standby')
        else:
            print "not changing status from",self._getA(agent).get('status',None)

    def poll(self, wake_up_draining=False, acting=False, verbose=False):
        if not self.ready:
            print "cannot poll the agents without fresh information about them"
            return False

        verbose = verbose or self.verbose
        wake_up_draining = wake_up_draining or self.wake_up_draining
        now,nows = self.getNow()

        ## annotate who could go
        candidates_to_standby = set()
        candidates_to_drain = set()
        candidates_to_wakeup = set()
        fully_empty = set()
        manipulated_agents = set()

        ### decides if you need to boot and agent
        need_one = False
        ### decides if you need to get one out becuase there are too many already
        # put in standby if some agent is running way low
        retire_agent = False
        ### decide if agents are good to get in drain
        # put in drain if several agents have a new release
        drain_agent = False

        ## go through some metric
        ## collect the number of jobs per agent.
        ##sum over those running+draining+standby
        recent_running = set()
        recent_standby = set()
        recent_draining = set()
        one_recent_running = False
        one_recent_standby = False
        one_recent_draining = False
        timeout_last_running = None
        timeout_last_standby = None
        timeout_last_draining = None
        last_action_timeout = 5 *60*60 # hours
        last_action_static = 20*60*60 # hours
        for agent in self.buckets.get('running',[]):
            agent_update = self._get(agent, 'update')
            if (now-agent_update)<(last_action_timeout):
                one_recent_running = True
                timeout_for_running = last_action_timeout - (now-agent_update)
                if timeout_last_running == None or timeout_last_running < timeout_for_running:
                    timeout_last_running = timeout_for_running
            if (now-agent_update)<(last_action_static):
                recent_running.add(agent )

        for agent in self.buckets.get('standby',[]):
            agent_update = self._get(agent, 'update')
            if (now-agent_update)<(last_action_timeout):
                one_recent_standby = True
                timeout_for_standby = last_action_timeout - (now-agent_update)
                if timeout_last_standby == None or timeout_last_standby < timeout_for_standby:
                    timeout_last_standby = timeout_for_standby
            if (now-agent_update)<(last_action_static):
                recent_standby.add(agent )

        for agent in self.buckets.get('draining',[]):
            agent_update = self._get(agent, 'update')
            if (now-agent_update)<(last_action_timeout):
                one_recent_draining = True
                timeout_for_draining = last_action_timeout - (now-agent_update)
                if timeout_last_draining == None or timeout_last_draining < timeout_for_draining:
                    timeout_last_draining = timeout_for_draining
            if (now-agent_update)<(last_action_static):
                recent_draining.add( agent )

        timeout_action = max([timeout_last_running,timeout_last_standby,timeout_last_draining])


        all_agents = dataCache.get('gwmsmon_pool')
	if not all_agents: return
        over_threshold = True
        under_threshold = False
        capacity = 0
        running = 0
        pending = 0
        cpu_pending = 0
        cpu_running = 0
        ## reduce the release name to major numbers
        for r in self.release:
            for ra in self.release[r]:
                m_r = '.'.join( r.split('.')[:3]) ## limit to major three numbers
                self.m_release[m_r].add( ra )

        rel_num = [(r, map(lambda frag : int(frag.replace('patch','')), r.split('.'))) for r in self.release.keys()]
        rel_num = sorted( rel_num, key = lambda o: o[1], reverse=True)
        m_rel_num = [(r, map(lambda frag : int(frag.replace('patch','')), r.split('.'))) for r in self.m_release.keys()]
        m_rel_num = sorted( m_rel_num , key = lambda o: o[1], reverse=True)
        #print rel_num
        sorted_release = [r[0] for r in rel_num]
        top_release = sorted_release[0] ## this is the latest release
        oldest_release = sorted_release[-1] #this is the oldest release
        sorted_m_release = [r[0] for r in m_rel_num]
        top_m_release = sorted_m_release[0]
        oldest_m_release = sorted_m_release[-1]
        if top_m_release == oldest_m_release:
            oldest_m_release = None
        if top_release == oldest_release:
            oldest_release = None

        running_top_release = 0
        running_old_release = 0
        standby_top_release = 0
        speed_draining = set()
        open_draining = set()
        wake_up_metric = []
        for agent,ainfo in all_agents.items():
            if not 'Name' in ainfo: continue
            agent_name = ainfo['Name']
            if not agent_name in self.all_agents: continue
            r = ainfo['TotalRunningJobs']
            cr = ainfo['TotalRunningCpus']
            t = ainfo['MaxJobsRunning']
            p = ainfo['TotalIdleJobs']
            cp = ainfo['TotalIdleCpus']
            cpu_pending += cp
            cpu_running += cr
            running += r
            pending += p
            wake_up_metric.append( (agent_name, p-r ) )

        runnings = self.buckets.get('running',[])
        drainings = self.buckets.get('draining',[])
        standbies = self.buckets.get('standby',[])
        for agent,ainfo in all_agents.items():
            if not 'Name' in ainfo: continue
            agent_name = ainfo['Name']
            if not agent_name in self.all_agents: continue
            r = ainfo['TotalRunningJobs']
            cr = ainfo['TotalRunningCpus']
            t = ainfo['MaxJobsRunning']
            p = ainfo['TotalIdleJobs']
            cp = ainfo['TotalIdleCpus']
            stuffed = (r >= t*self.busy_fraction)
            light = (r <= t*self.idle_fraction)

            if verbose:
                print json.dumps(ainfo, indent=2)
            if agent_name in drainings:
                if not agent_name in self.release[oldest_release]:
                    ## you can candidate those not running the oldest release, and in drain
                    if not stuffed and not light:
                        candidates_to_wakeup.add( agent_name )
                if (cp <= cpu_running*self.speed_draining_fraction) and (r <= t*self.speed_draining_fraction) and (cr <= cpu_running*self.speed_draining_fraction):
                    speed_draining.add( agent_name )
                if len(standbies)<=1 and (r+p <= self.open_draining_threshold):
                    open_draining.add( agent_name )
                if r==0 and p==0 and agent_name in self.drained:
                    fully_empty.add( agent_name )

            if agent_name in standbies:
                if agent_name in self.release[top_release]:
                    standby_top_release += 1

            if agent_name in runnings:
                if agent_name in self.release[top_release]:
                    running_top_release += 1
                elif not agent_name in self.m_release[top_m_release]: ## use the major release number to select agents to drain: i.e. ignore patches versions.
                    candidates_to_drain.add( agent_name )
                    running_old_release += 1
                else:
                    pass

                capacity += t
                if verbose:
                    print agent
                    print "is Stuffed?",stuffed
                    print "is underused?",light
                over_threshold &= stuffed

                light = False ## prevent this for now

                under_threshold |= light
                if light:
                    candidates_to_standby.add( agent_name )
                    candidates_to_drain.add( agent_name )

        candidates_to_drain = candidates_to_drain - recent_running
        candidates_to_wakeup = candidates_to_wakeup - recent_draining -recent_standby - set(self.in_error)
        candidates_to_standby = candidates_to_standby - recent_running


        if verbose or verbose or True:
            print "agent releases",sorted_release
            print "agent major releases",sorted_m_release
            print "latest release",top_release
            print "latest major release",top_m_release
            print "oldest release",oldest_release
            print "oldest major release",oldest_m_release
            print "Capacity",capacity
            print "Running jobs", running
            print "Running cpus", cpu_running
            print "Pending jobs", pending
            print "Pending cpus",cpu_pending
            print "Running lastest release",running_top_release
            print "Standby in latest release",standby_top_release
            print "Running with old release",running_old_release
            print "These are candidates for draining",sorted(candidates_to_drain)
            print "These are good for speed drainig",sorted(speed_draining)
            print "These are good for open draining",sorted(open_draining)
            print "These are fully empty",sorted(fully_empty)
        if not acting:
            speed_draining = set()
            open_draining = set()

        over_cpus = self.max_pending_cpus
        over_pending = (cpu_pending > over_cpus)
        release_deploy = ((running_top_release>=2) or (standby_top_release>=2)) and running_old_release


        ## all agents are above the understood limit. so please boot one
        if not (one_recent_running or one_recent_draining or one_recent_standby):
            if over_threshold:
                msg = 'All agents are maxing out. We need a new agent'
                sendLog('agentInfo', msg, level='critical')
            if under_threshold:
                msg = 'There are agents under-doing and that could be set in standby'
                sendLog('agentInfo', msg , level='critical')
                sendEmail('agentInfo', msg)
            if over_pending:
                msg = 'There is more than %d cpus pending, %d. We need to set an agent aside.'% (over_cpus, cpu_pending)
                sendLog('agentInfo', msg, level='critical')
                sendEmail('agentInfo', msg)
            if release_deploy:
                msg = 'There is a new agent release in town %s. Starting to drain other agents from %s'%( top_release, sorted( candidates_to_drain ))
                sendLog('agentInfo', msg, level='critical')

            if acting:
                need_one = over_threshold
                if not over_threshold:
                    retire_agent = under_threshold or over_pending
                if release_deploy:
                    drain_agent = True
        else:
            print "An agent was recently put in running/draining/standby. Cannot do any further acting for another %s last running or %s last standby"% (display_time(timeout_action),
                                                                                                                                        display_time(timeout_last_standby))
            candidates_to_wakeup = candidates_to_wakeup - fully_empty
        if need_one:
            pick_from = self.buckets.get('standby',[])
            if not pick_from:
                if wake_up_draining:
                    print "wake up an agent that was already draining"
                    if candidates_to_wakeup:
                        print "picking up from candidated agents"
                        pick_from = candidates_to_wakeup
                    else:
                        print "picking up from draining agents"
                        pick_from = self.buckets.get('draining',[])

                    ## order by the metric
                    pick_from = [o[0] for o in sorted([(a,m) for (a,m) in wake_up_metric if a in pick_from ], key = lambda o:o[1], reverse=True)]
                    print wake_up_metric
                    print pick_from
                    pick_from = pick_from[:1]
            if not pick_from:
                print "need to wake an agent up, but there are none available"
                ## this is a major issue!!!
                msg = 'We urgently need a new agent in the pool, but none seem to be available'
                sendEmail('agentInfo', msg)
                sendLog('agentInfo', msg, level='critical')
            else:
                # pick one at random in the one idling
                wake_up = random.choice( pick_from )
                print "waking up", wake_up
                if wake_up in speed_draining: speed_draining.remove( wake_up )
                if wake_up in open_draining: open_draining.remove( wake_up )
                if setAgentOn(self.url, wake_up):
                    msg = "putting agent %s in production"% wake_up
                    sendLog('agentInfo', msg, level='critical')
                    manipulated_agents.add( wake_up )
                    self.change_status(wake_up, 'running')
        elif drain_agent:
            ## pick one agent to drain
            sleep_up = None
            if candidates_to_drain:
                sleep_up = random.choice( list(candidates_to_drain))
            if sleep_up:
                if setAgentDrain(self.url, sleep_up):
                    msg = "putting agent %s in drain mode"% sleep_up
                    sendLog('agentInfo', msg, level='critical')
                    manipulated_agents.add( sleep_up )
                    self.change_status(sleep_up, 'draining')
            else:
                print "Agents need to be set in drain, but nothing is there to be drained"

        elif retire_agent:
            # pick one with most running jobs
            if candidates_to_standby:
                print "picking up from the candidated agents for standby"
                sleep_up = random.choice( list( candidates_to_standby ))
            else:
                print "picking up from the running agents"
                pick_from = self.buckets.get('running',[])
                sleep_up = random.choice( pick_from )
            if setAgentDrain(self.url, sleep_up):
                msg = "putting agent %s in drain mode for retiring"% sleep_up
                sendLog('agentInfo', msg, level='critical')
                manipulated_agents.add( sleep_up )
                self.change_status(sleep_up, 'standby')
        else:
            if not acting:
                print "The polling is not proactive"
            else:
                print "Everything is fine. No need to retire or add an agent"

        for agent in open_draining:
            ## set the config tweaks to enable retry=0 and thresold=0
            pass

        already_speed_drain = self.speed_draining()
        all_in_priority_drain = set()
        if (already_speed_drain & speed_draining):
            ## lets keep that agent in speed drainig
            print sorted(already_speed_drain),"already in speed draining. not changing this"
            all_in_priority_drain = already_speed_drain & speed_draining
        else:
            speed_draining = list(speed_draining)
            ## shuffle and pick on new
            random.shuffle(speed_draining)
            all_in_priority_drain.update( speed_draining[:1] )

        all_in_priority_drain.update( open_draining )
        ## operate this
        for agent in self.all_agents:
            self._update(agent, {'speeddrain' : agent in all_in_priority_drain})

        if fully_empty:
            msg = 'These agents are fully empty %s and ready for redeploy'% sorted(fully_empty)
            ## manipulate them into the "drained" list
            for agent in fully_empty:
                self._update(agent, {'status': 'drained'})
                manipulated_agents.add( agent )

        ## should update trello with the agents that got manipulated at this time
        self.checkTrello(sync_trello=list(manipulated_agents), acting=acting)

        ## then be slave to the trello board
        self.checkTrello(sync_agents=True,acting=acting)

        ## and sync locally
        self.getStatus()

def getAgentConfig(url, agent):
    conn = make_x509_conn(url)
    go_url= '/reqmgr2/data/wmagentconfig/%s'% agent
    conn.request("GET",go_url, headers={"Accept":"application/json"})
    r2=conn.getresponse()
    info = json.loads(r2.read())['result'][-1]
    return info

def sendAgentConfig(url, agent, config_dict):
    encodedParams = json.dumps(config_dict)
    conn = make_x509_conn(url)
    go_url= '/reqmgr2/data/wmagentconfig/%s'% agent
    r1 = conn.request("PUT", go_url , encodedParams, headers={"Accept":"application/json",
                                                              "Content-type": "application/json",
                                                              "Host" : "cmsweb.cern.ch"})
    r2 = conn.getresponse()
    res = r2.read()
    r=  json.loads(res)['result'][0]["ok"]
    return r

def setAgentDrain(url, agent, drain=True):
    #the agent name has to have .cern.ch and all
    info = getAgentConfig(url, agent)
    print agent,"is draining?",info['UserDrainMode']

    info['UserDrainMode'] = drain
    r = sendAgentConfig(url, agent, info)
    return r

def setAgentOn(url, agent):
    return setAgentDrain(url, agent, drain=False)

def getAllAgents(url):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    url = '/couchdb/wmstats/_design/WMStats/_view/agentInfo?stale=update_after'
    r1=conn.request("GET",url)
    r2=conn.getresponse()
    teams = defaultdict(list)
    for r in [i['value'] for i in json.loads( r2.read() )['rows']]:
        teams[r['agent_team']].append( r )
    return teams

def getWorkflowsByName(url, names, details=False):
    return runWithRetries(_getWorkflowsByName, [url, names], {'details': details}, retries =5, wait=5)

def _getWorkflowsByName(url, names, details=False):
    conn = make_x509_conn(url)

    go_to = '/reqmgr2/data/request?'
    if isinstance(names, basestring):
        names = [names]
    for wfName in names:
        go_to += '&name=%s' % wfName
    go_to += '&detail=%s'%('true' if details else 'false')

    conn.request("GET",go_to, headers={"Accept":"application/json"})
    r2=conn.getresponse()
    data = json.loads(r2.read())
    items = data['result']

    if details and items:
        workflows = items[0].values()
    else:
        workflows = items

    print "%d retrieved for %d workflow names with details: %s" % (len(workflows), len(names), details)
    return workflows

def getWorkflows(url,status,user=None,details=False,rtype=None, priority=None):
    return runWithRetries(try_getWorkflows, [url, status],
                          {'user': user, 'details': details, 'rtype': rtype, 'priority': priority},
                          retries =5, wait=5)

def try_getWorkflows(url,status,user=None,details=False,rtype=None, priority=None):
    conn = make_x509_conn(url)
    go_to = '/reqmgr2/data/request?status=%s'%status
    if rtype:
        go_to+='&request_type=%s'%rtype
    if user:
        for u in user.split(','):
            go_to+='&requestor=%s'%u
    if priority!=None:
        print priority,"is requested"
        go_to+='&initialpriority=%d'%priority ### does not work...

    go_to+='&detail=%s'%('true' if details else 'false')
    r1=conn.request("GET",go_to, headers={"Accept":"application/json"})
    r2=conn.getresponse()
    data = json.loads(r2.read())
    items = data['result']

    print len(items),"retrieved",status,user,details,rtype
    workflows = []

    for item in items:
        if details:
            those = item.keys()
        else:
            those = [item]

        if details:
            workflows.extend([item[k] for k in those])
        else:
            workflows.extend(those)
    return workflows

def getPrepIDs(wl):
    pids = list()
    if 'Chain' in wl['RequestType']:
        base= wl['RequestType'].replace('Chain','')
        itask=1
        while True:
            t = '%s%d'%(base, itask)
            itask+=1
            if t in wl:
                if 'PrepID' in wl[t]:
                    if not wl[t]['PrepID'] in pids:
                        pids.append( wl[t]['PrepID'] )
            else:
                break
        if pids:
            return pids
        else:
            return [wl['PrepID']]
    elif 'PrepID' in wl:
        return [wl['PrepID']]
    else:
        return []

def runWithRetries( glb_fcn, 
                    fcn_pargs,
                    fcn_args,
                    default='NoDefaultValue',
                    retries = 10,
                    wait = 5
                ):
    message = ""
    tries=0
    while tries<retries:
        tries+=1
        try:
            return glb_fcn(*fcn_pargs,**fcn_args)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            ## signal this somewhere
            message = "Failed to run function {} with arguments {} {} for {}/{} times and {}[s] wait (%s). Exception\n {}\n {}".format( glb_fcn.__name__,
                                                                                                                                        str(fcn_pargs),
                                                                                                                                        str(fcn_args),
                                                                                                                                        tries,
                                                                                                                                        retries,
                                                                                                                                        wait,
                                                                                                                                        time.asctime(),
                                                                                                                                        str(e),
                                                                                                                                        tb)
            print (message)
            time.sleep(wait)
    if default != 'NoDefaultValue':
        return default
    else:
        raise Exception(message)

def getLFNbase(dataset):
    return runWithRetries(_getLFNbase, [dataset],{})
def _getLFNbase(dataset):
        # initialize API to DBS3
	dbsapi = DbsApi(url=dbs_url)
        reply = dbsapi.listFiles(dataset=dataset)
        # retrieve file
        file = reply[0]['logical_file_name']
        return '/'.join(file.split('/')[:3])

class wtcInfo:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.wtcInfo

    def add(self, action, keyword, user=None):
        ##add an item for the action (hold, bypass, force) for the keyword
        if not keyword:
            print "blank keyword is not allowed"
            return
        n = time.gmtime()
        now = time.mktime( n )
        nows = time.asctime( n )
        document= {
            'user' : user if user else os.environ.get('USER',None),
            'keyword' : keyword,
            'action' : action,
            'time' : now,
            'date' : nows}
        self.db.update_one( {'keyword' : keyword},
                            {"$set": document},
                            upsert = True
                        )

    def _get(self, action):
        r= defaultdict(list)
        for i in self.db.find({'action' : action}):
            r[i['user']].append(i['keyword'])
        return dict(r)
        
    def getHold(self):
        return self._get('hold')
    def getBypass(self):
        return self._get('bypass')
    def getForce(self):
        return self._get('force')

    def clean(self):
        wfns = []
        for s in ['announced','normal-archived','rejected','aborted','aborted-archived','rejected-archived']:
            wfns.extend( getWorkflows( reqmgr_url , s))
        for item in self.db.find():
            key = item['keyword']
            if any([key in wfn for wfn in wfns]):
                print item.get('keyword'),"can go"
                self.db.delete_one({'_id' : item.get('_id',None)})
            
    def remove(self, keyword):
        ## will remove from the db anything that the item matches on
        for item in self.db.find():
            if item.get('keyword',None) in keyword:
                print item,"goes away"
                self.db.delete_one({'_id' : item.get('_id',None)})

class workflowInfo:
    def __init__(self, url, workflow, spec=True, request=None,stats=False, wq=False, errors=False):
        self.logs = defaultdict(str)
        self.url = url
        self.conn = make_x509_conn(self.url)
        if request == None:
            try:
                r1=self.conn.request("GET",'/reqmgr2/data/request/'+workflow, headers={"Accept":"*/*"})
                r2=self.conn.getresponse()
                ret = json.loads(r2.read())
                ret = ret['result'][0][workflow] ##new
                self.request = ret
            except Exception as e:
                print "failed to get workload"
                print str(e)
                try:
                    r1=self.conn.request("GET",'/couchdb/reqmgr_workload_cache/'+workflow)
                    r2=self.conn.getresponse()
                    ret = json.loads(r2.read())
                    self.request = ret
                except Exception as e:
                    print "Failed to get workload cache for",workflow
                    print str(e)
                    raise Exception("Failed to get workload cache for %s"%workflow)
        else:
            self.request = copy.deepcopy( request )
        self.full_spec=None
        if spec:
            self.get_spec()

        self.wmstats = None
        if stats:
            self.getWMStats()

        self.errors = None
        if errors:
            self.getWMErrors()

        self.recovery_doc = None

        self.workqueue = None
        if wq:
            self.getWorkQueue()

        self.summary = None

        self.UC = unifiedConfiguration()

    def getFamilly(self, details=True, only_resub=False, and_self=False):
        familly = getWorkflowById( self.url, self.request['PrepID'] ,details=True)
        true_familly = []
        for member in familly:
            acdc = member['RequestType']=='Resubmission'
            myself = member['RequestName'] == self.request['RequestName']
            if member['RequestDate'] < self.request['RequestDate']: continue
            if member['RequestStatus'] in ['None',None]: continue
            
            if (myself and and_self) or (only_resub and acdc and not myself) or (not only_resub and not myself): 
                if details:
                    true_familly.append( member )
                else:
                    true_familly.append( member['RequestName'] )

        return true_familly

    def isRelval(self):
        if 'SubRequestType' in self.request and 'RelVal' in self.request['SubRequestType']:
            return True
        else:
            return False

    def getTimeInfoForChain(self):
        timeInfo = dict()
        if 'Chain' in self.request['RequestType']:
            base = self.request['RequestType'].replace('Chain','')
            for itask in range(1, self.request['TaskChain'] + 1):
                t = '%s%d'%(base, itask)
                if t in self.request:
                    task = self.request[t]
                    timeInfo[t] = {
                        'tpe':task.get('TimePerEvent')/task.get('FilterEfficiency',1),
                        'cores':task.get('Multicore', self.request['Multicore'])
                    }
                else:
                    break
        return timeInfo

    def isGoodToConvertToStepChain(self ,keywords=None, talk=False, debug=False):
        # Conversion is supported only from TaskChain to StepChain
        if self.request['RequestType'] != 'TaskChain': return False

        # Conversion is not supported if there is a task whose EventStreams is nonzero 
        for key,value in self.request.items():
            if key.startswith('Task') and type(value) is dict: 
                if 'EventStreams' in value:
                    if value['EventStreams'] != 0:
                        print('EventStreams is ' + str(value['EventStreams']) + ' do not convert')
                        return False

        all_same_arch = True

        ## efficiency 
        try:
            time_info = self.getTimeInfoForChain()
            if debug: print time_info
            totalTimePerEvent = 0
            efficiency = 0
            max_ncores = 1
            for i,info in time_info.items():
                totalTimePerEvent += info['tpe']
                efficiency += info['tpe']*info['cores']
                if info['cores']>max_ncores: max_ncores = info['cores']
            if debug: print "Total time per event for TaskChain: %0.1f" % totalTimePerEvent
            if totalTimePerEvent > 0:
                efficiency /= totalTimePerEvent*max_ncores
                if debug: print "CPU efficiency of StepChain with %u cores: %0.1f%%" % (max_ncores,efficiency*100)
                acceptable_efficiency = efficiency > self.UC.get("efficiency_threshold_for_stepchain")
            else:
                acceptable_efficiency = False
        except TypeError:
            acceptable_efficiency = False
            if debug:
                print "Caught TypeError"

        ## only one value throughout the chain
        all_same_cores = len(set(self.getMulticores()))==1
        ##make sure not tow same data tier is produced
        all_tiers = map(lambda o : o.split('/')[-1], self.request['OutputDatasets'])
        single_tiers = (len(all_tiers) == len(set(all_tiers)))
        ## more than one task with output until https://github.com/dmwm/WMCore/issues/8269 gets solved
        output_from_single_task = True ## the parentage
        ## more than one task to not convert single task in a step
        more_than_one_task = self.request.get('TaskChain',0)>1
        ## so that conversion happens only for a selected few
        found_in_transform_keywords = True

        listScrams = self.getArchs()
        setOSs = set()
        for sc in listScrams:
            setOSs.add(sc[:4])
        if len(setOSs) > 1:
            all_same_arch = False
            
        pss = self.processingString()
	if type(pss)==dict:
	    pssString = ''.join('{}{}'.format(key, val) for key, val in pss.items())
	else:
	    pssString = pss
        wf = pssString+self.request['RequestName']
	
        if keywords:
            found_in_transform_keywords = any([keyword in wf for keyword in keywords])
        good = self.request['RequestType'] == 'TaskChain' and more_than_one_task and found_in_transform_keywords and single_tiers and (all_same_cores or acceptable_efficiency) and output_from_single_task and all_same_arch
        if not good and talk:
            print "cores",all_same_cores
            print "parentage",output_from_single_task
        return good


    def notifyRequestor(self, message, do_request=True, do_batch=True, mcm=None):
        if not message: return
        try:

            if mcm == None:
                from McMClient import McMClient
                mcm = McMClient(dev=False)
            pids = self.getPrepIDs()
            wf_name = self.request['RequestName']
            items_notified = set()
            for pid in set(pids):
                replacements = {'PREPID': pid,
                                'WORKFLOW' : self.request['RequestName']
                                }
                dedicated_message = message
                add_batch ="This message concerns PREPID WORKFLOW"
                for src,dest in replacements.items():
                    dedicated_message = dedicated_message.replace(src, dest)
                    add_batch = add_batch.replace(src, dest)

                batches = mcm.getA('batches',query='contains=%s'%wf_name)
                batches = filter(lambda b : b['status'] in ['announced','done','reset'], batches)
                if not batches:
                    batches = mcm.getA('batches',query='contains=%s'%pid)
                    batches = filter(lambda b : b['status'] in ['announced','done','reset'], batches)
                if batches:
                    bid = batches[0]['prepid']
                    print "batch nofication to",bid
                    if not bid in items_notified:
                        mcm.put('/restapi/batches/notify', { "notes" : dedicated_message+"\n"+add_batch, "prepid" : bid})
                        items_notified.add( bid )
                if not pid in items_notified:
                    print "request notification to",pid
                    mcm.put('/restapi/requests/notify',{ "message" : dedicated_message, "prepids" : [pid] })
                    items_notified.add( pid )
        except Exception as e:
            print "could not notify back to requestor\n%s"%str(e)
            self.sendLog('notifyRequestor','could not notify back to requestor\n%s'%str(e))


    def sendLog( self, subject, text, show=True):
        if show:
            print text ## to avoid having to duplicate it
        self.logs[subject] += '\n'+text

    def __del__(self):
        self.flushLog()

    def flushLog(self):
        ## flush the logs
        for sub,text in self.logs.items():
            sendLog(sub, text, wfi = self, show=False, level='workflow')

    def get_spec(self):
        try:
            return self._get_spec()
        except:
            time.sleep(1)
            try:
                return self._get_spec()
            except Exception as e:
                print "cannot get spec for",self.request['RequestName']
                return None

    def _get_spec(self):
        if not self.full_spec:
            self.conn = make_x509_conn(self.url)
            r1=self.conn.request("GET",'/couchdb/reqmgr_workload_cache/%s/spec'%self.request['RequestName'])
            r2=self.conn.getresponse()
            self.full_spec = pickle.loads(r2.read())
        return self.full_spec

    def getWMErrors(self,cache=0):
        try:
            f_cache = '%s/%s.wmerror'%(cache_dir, self.request['RequestName'])
            if cache:
                if os.path.isfile(f_cache):
                    d_cache = json.loads(open(f_cache).read())
                    now = time.mktime(time.gmtime())
                    stamp = d_cache['timestamp']
                    if (now-stamp) < cache:
                        print "wmerrors taken from cache",f_cache
                        self.errors = d_cache['data']
                        return self.errors

            self.conn = make_x509_conn(self.url)
            r1=self.conn.request("GET",'/wmstatsserver/data/jobdetail/%s'%(self.request['RequestName']), headers={"Accept":"*/*"})
            r2=self.conn.getresponse()

            self.errors = json.loads(r2.read())['result'][0][self.request['RequestName']]
            try:
                open(f_cache,'w').write( json.dumps({'timestamp': time.mktime(time.gmtime()),
                                                     'data' : self.errors}))
            except Exception as e:
                print "failed getting getWMErrors"
                print str(e)
            return self.errors
        except:
            print "Could not get wmstats errors for",self.request['RequestName']
            self.conn = make_x509_conn(self.url)
            return {}

    def getWMStats(self ,cache=0):
        trials = 1
        while trials>0:
            try:
                return self._getWMStats(cache=cache)
            except Exception as e:
                print "Failed",trials,"at reading getWMStats"
                print str(e)
                print self.request['RequestName']
                self.conn = make_x509_conn(self.url)

            trials-=1
            time.sleep(1)
        return {}

    def _getWMStats(self ,cache=0):
        f_cache = '%s/%s.wmstats'%(cache_dir, self.request['RequestName'])
        if cache:
            if os.path.isfile(f_cache):
                d_cache = json.loads(open(f_cache).read())
                now = time.mktime(time.gmtime())
                stamp = d_cache['timestamp']
                if (now-stamp) < cache:
                    print "wmstats taken from cache",f_cache
                    self.wmstats = d_cache['data']
                    return self.wmstats
        r1=self.conn.request("GET",'/wmstatsserver/data/request/%s'%self.request['RequestName'], headers={"Accept":"application/json"})
        r2=self.conn.getresponse()
        self.wmstats = json.loads(r2.read())['result'][0][self.request['RequestName']]
        try:
            open(f_cache,'w').write( json.dumps({'timestamp': time.mktime(time.gmtime()),
                                                 'data' : self.wmstats}) )
        except Exception as e:
            print "failed getWMStats"
            print str(e)

        return self.wmstats

    def getRecoveryBlocks(self ,collection_name=None, for_task= None):
        doc = self.getRecoveryDoc(collection_name=collection_name)
        all_files = set()
        files_and_loc = defaultdict(set)
        for d in doc:
            task = d.get('fileset_name',"")
            if for_task and not task.endswith(for_task):continue
            all_files.update( d['files'].keys())
            for fn in d['files']:
                files_and_loc[ fn ].update( d['files'][fn]['locations'] )

        print len(all_files),"file in recovery"
        dbsapi = DbsApi(url=dbs_url)
        all_blocks = set()
        all_blocks_loc = defaultdict(set)
        files_no_block = set()
        files_in_block = set()
        datasets = set()
        cache = cacheInfo()
        file_block_cache = defaultdict( str )
        for f in all_files:
            if not f.startswith('/store/unmerged/') and not f.startswith('MCFakeFile-'):
                if f in file_block_cache:
                    file_block = file_block_cache[ f ]
                else:
                    file_block = getFileBlock( f ) 
                    if file_block:
                        for _f in getDatasetFileArray( file_block.split('#')[0], detail=True, cache_timeout=12*60*60 ):
                            file_block_cache[ _f['logical_file_name' ] ] = _f['block_name']
                    
                files_in_block.add( f )
                all_blocks.add( file_block )
                all_blocks_loc[file_block].update( files_and_loc.get( f, []) )
            else:
                files_no_block.add( f )

        file_block_doc = defaultdict( lambda : defaultdict( set ))
        dataset_blocks = set()
        for _f,_b in file_block_cache.iteritems():
            file_block_doc[ _b.split('#')[0]][_b].add( _f )
            dataset_blocks.add( _b )

        ## skim out the files
        files_and_loc_noblock = dict([(k,list(v)) for (k,v) in files_and_loc.items() if k in files_no_block])
        files_and_loc = dict([(k,list(v)) for (k,v) in files_and_loc.items() if k in files_in_block])
        return dataset_blocks,all_blocks_loc,files_in_block,files_and_loc,files_and_loc_noblock

    def getRecoveryDoc(self, collection_name=None):
        if collection_name == None:
            collection_name = self.request['RequestName']

        if 'CollectionName' in self.request and self.request['CollectionName']:
            if collection_name == True:
                collection_name = self.request['CollectionName']
                print "using collection name from schema"

        if self.recovery_doc != None:
            print "returning cached self.recovery_doc"
            return self.recovery_doc
        try:
            print "using",collection_name
            r1=self.conn.request("GET",'/couchdb/acdcserver/_design/ACDC/_view/byCollectionName?key="%s"&include_docs=true&reduce=false'% collection_name)
            r2=self.conn.getresponse()
            rows = json.loads(r2.read())['rows']
            self.recovery_doc = [r['doc'] for r in rows]
        except:
            self.conn = make_x509_conn(self.url)
            print "failed to get the acdc document for",self.request['RequestName']
            self.recovery_doc = None
        return self.recovery_doc

    def getRecoveryInfo(self):
        self.getRecoveryDoc()
        if not self.recovery_doc:
            print "nothing retrieved"
            return {},{},{}
        where_to_run = defaultdict(list)
        missing_to_run = defaultdict(int)
        missing_to_run_at = defaultdict(lambda : defaultdict(int))
        original_whitelist = self.request['SiteWhitelist']
        for doc in self.recovery_doc:
            task = doc['fileset_name']
            for f,info in doc['files'].iteritems():
                missing_to_run[task] += info['events']
                if f.startswith('MCFakeFile'):
                    locations = original_whitelist
                else:
                    locations = info['locations']
                where_to_run[task] = list(set(where_to_run[task] + locations))
                for s in info['locations']:
                    missing_to_run_at[task][s] += info['events']

        return dict(where_to_run),dict(missing_to_run),missing_to_run_at

    def getPileUpJSON(self, task):
        res = {}
        agents = self.getActiveAgents()
        agents = map(lambda s : s.split('/')[-1].split(':')[0], agents)
        wf = self.request['RequestName']
        inagent=None
        for agent in agents:
            if 'fnal' in agent: continue
            src = '%s:/data/srv/wmagent/current/install/wmagent/WorkQueueManager/cache/%s/WMSandbox/%s/cmsRun1/pileupconf.json'%(agent, wf, task)
            dest = '/tmp/%s-%s.json'%( wf, task)
            if os.path.isfile( dest ) and False:
                inagent=agent
                res = json.loads(open( dest ).read())
                break
            print agent
            com = 'scp %s %s'%( src, dest)
            os.system( com )
            if os.path.isfile( dest ):
                inagent=agent
                res = json.loads(open( dest ).read())
                break
        if inagent:
            print "found PU json in:",inagent
        else:
            print "PU json not found"
                
        return res

    def getClassicalPUOverflow(self, task):
        pu = self.getPileUpJSON(task)
        if not pu: return []
        ret=set()
        intersection = None
        count_blocks = defaultdict(int)

        for block in pu['mc']:
            ret.update( pu['mc'][block]['PhEDExNodeNames'])
            for site in pu['mc'][block]['PhEDExNodeNames']:
                count_blocks[site]+=pu['mc'][block]['NumberOfEvents']
            if intersection:
                intersection = intersection & set(pu['mc'][block]['PhEDExNodeNames'])
            else:
                intersection = set(pu['mc'][block]['PhEDExNodeNames'])

        max_blocks = max( count_blocks.values())
        site_with_enough = [ site for site,count in count_blocks.items() if count > 0.90*max_blocks]
        SI = global_SI()
        ret = sorted(set([ SI.SE_to_CE(s) for s in ret if not 'Buffer' in s] ))
        inter = sorted(set([ SI.SE_to_CE(s) for s in intersection if not ('Buffer' in s or 'MSS' in s)]))
        enough = sorted(set([ SI.SE_to_CE(s) for s in site_with_enough if not ('Buffer' in s or 'MSS' in s)]))
        return enough

    def getWorkQueueElements(self):
        wq = self.getWorkQueue()
        wqes = [w[w['type']] for w in wq]
        return wqes

    def getWorkQueue(self):
        if not self.workqueue:
            TT = 0
            while TT<5:
                TT+=1
                try:
                    r1=self.conn.request("GET",'/couchdb/workqueue/_design/WorkQueue/_view/elementsByParent?key="%s"&include_docs=true'% self.request['RequestName'])
                    r2=self.conn.getresponse()
                    self.workqueue = list([d['doc'] for d in json.loads(r2.read())['rows'] if d['doc'] is not None])
                except Exception as e:
                    self.conn = make_x509_conn(self.url)
                    time.sleep(1) ## time-out
                    print "Failed to get workqueue"
                    print str(e)
                    self.workqueue = []
        return self.workqueue

    def getActiveBlocks(self, select=['Running','Acquired']):
        wq = self.getWorkQueue()
        wqes = [w[w['type']] for w in wq]
        blocks = defaultdict(set)
        selected = set()
        for wqe in wqes:
            blocks[wqe['Status']].update( wqe['Inputs'].keys() )
        for s in select:
            selected.update( blocks[s] )
        return blocks,selected

    def getAgents(self):
        wq = self.getWorkQueue()
        wqes = [w[w['type']] for w in wq]
        statuses = list(set([wqe['Status'] for wqe in wqes]))
        active_agents= defaultdict(lambda :defaultdict( int ))
        for status in statuses:
            wq_s = [wqe for wqe in wqes if wqe['Status'] == status]
            for wqe in wq_s: active_agents[status][wqe['ChildQueueUrl']]+=1
        return active_agents

    def getActiveAgents(self):
        wq = self.getWorkQueue()
        wqes = [w[w['type']] for w in wq]
        active_agents= defaultdict(int)
        wq_running = [wqe for wqe in wqes if wqe['Status'] == 'Running']
        for wqe in wq_running: active_agents[wqe['ChildQueueUrl']]+=1
        return dict( active_agents )

    def getSummary(self):
        if self.summary:
            return self.summary

        self.conn = make_x509_conn(self.url)
        r1=self.conn.request("GET",'/couchdb/workloadsummary/'+self.request['RequestName'], headers={"Accept":"application/json"} )
        r2=self.conn.getresponse()

        self.summary = json.loads(r2.read())
        return self.summary

    def getGlideMon(self):
        try:
            gmon = json.loads(os.popen('curl -s https://cms-gwmsmon.cern.ch/prodview/json/%s/summary'%self.request['RequestName']).read())
            return gmon
        except:
            print "cannot get glidemon info",self.request['RequestName']
            return None

    def _tasks(self):
        return self.get_spec().tasks.tasklist

    def firstTask(self):
        return self._tasks()[0]

    def getPrepIDs(self):
        return getPrepIDs(self.request)

    def getComputingTime(self,unit='h'):
        cput = None
        ## look for it in a cache

        if 'InputDataset' in self.request:
            ds = self.request['InputDataset']
            if 'BlockWhitelist' in self.request and self.request['BlockWhitelist']:
                (ne,_) = getDatasetEventsAndLumis( ds , self.request['BlockWhitelist'] )
            else:
                (ne,_) = getDatasetEventsAndLumis( ds )
            tpe = self.request['TimePerEvent']

            cput = ne * tpe
        elif 'Chain' in self.request['RequestType']:
            base = self.request['RequestType'].replace('Chain','')
            itask=1
            cput=0
            carry_on = {}
            while True:
                t = '%s%d'%(base, itask)
                itask+=1
                if t in self.request:
                    #print t
                    task = self.request[t]
                    if 'InputDataset' in task:
                        ds = task['InputDataset']
                        if 'BlockWhitelist' in task and task['BlockWhitelist']:
                            (ne,_) = getDatasetEventsAndLumis( ds, task['BlockWhitelist'] )
                        else:
                            (ne,_) = getDatasetEventsAndLumis( ds )
                    elif 'Input%s'%base in task:
                        ## we might have a problem with convoluted tasks, but maybe not
                        ne = carry_on[task['Input%s'%base]]
                    elif 'RequestNumEvents' in task:
                        ne = float(task['RequestNumEvents'])
                    else:
                        print "this is not supported, making it zero cput"
                        ne = 0
                    tpe =task.get('TimePerEvent',1) ## harsh
                    carry_on[task['%sName'%base]] = ne
                    if 'FilterEfficiency' in task:
                        carry_on[task['%sName'%base]] *= task['FilterEfficiency']
                    cput += tpe * ne
                    #print cput,tpe,ne
                else:
                    break
        else:
            ne = float(self.request['RequestNumEvents'])
            fe = float(self.request['FilterEfficiency'])
            tpe = self.request['TimePerEvent']

            cput = ne/fe * tpe

        if cput==None:
            return 0

        if unit=='m':
            cput = cput / (60.)
        if unit=='h':
            cput = cput / (60.*60.)
        if unit=='d':
            cput = cput / (60.*60.*24.)
        return cput

    def getNCopies(self, CPUh=None, m = 2, M = 3, w = 50000, C0 = 100000):
        def sigmoid(x):
            return 1 / (1 + math.exp(-x))
        if CPUh==None:
            CPUh = self.getComputingTime()
        f = sigmoid(-C0/w)
        D = (M-m) / (1-f)
        O = (f*M - m)/(f-1)
        return int(O + D * sigmoid( (CPUh - C0)/w)), CPUh

    def getBlowupFactors(self):
        ## that does not exists for StepChain
        if self.request['RequestType']=='TaskChain':
            min_child_job_per_event=None
            root_job_per_event=None
            max_blow_up=0
            splits = self.getSplittings()
            for task in splits:
                c_size=None
                p_size=None
                t=task['splittingTask']
                for k in ['events_per_job','avg_events_per_job']:
                    if k in task: c_size = task[k]
                parents = filter(lambda o : t.startswith(o['splittingTask']) and t!=o['splittingTask'], splits)
                if parents:
                    for parent in parents:
                        for k in ['events_per_job','avg_events_per_job']:
                            if k in parent: p_size = parent[k]

                        if not min_child_job_per_event or min_child_job_per_event > c_size:
                            min_child_job_per_event = c_size
                else:
                    root_job_per_event = c_size

                if c_size and p_size:
                    blow_up = float(p_size)/ c_size

                    if blow_up > max_blow_up:
                        max_blow_up = blow_up
            return (min_child_job_per_event, root_job_per_event, max_blow_up)
        return (1.,1.,1.)
    def heavyRead(self,secondary):
        ## Fasle by default. True if "minbias" appears in the secondary
        response = False
        if any(['minbias' in c.lower() for c in secondary]):
            response = True
        return response
    
    def producePremix(self):
        # Determine whether workflow is producing PREMIX 
        output_datatiers = [x.split('/')[-1].lower() for x in self.request['OutputDatasets']]
        return 'premix' in output_datatiers

    def getSiteWhiteList( self, pickone=False, verbose=True):
        SI = global_SI()
        (lheinput,primary,parent,secondary) = self.getIO()
        sites_allowed=[]
        sites_not_allowed=[]
        if lheinput:
            sites_allowed = sorted(SI.sites_eos) #['T2_CH_CERN'] ## and that's it
        elif secondary:
            if self.heavyRead(secondary):
                # Get PU locations which are protected by wmcore_transferor in terms of CE/PSN name
                rucioClient = RucioClient()
                for sec in secondary:
                    pileup_locations = rucioClient.getDatasetLocationsByAccount(sec, "wmcore_transferor")
                    sites_allowed += pileup_locations
                sites_allowed = sorted(set(sites_allowed))
                print "Reading minbias"
            else:
                sites_allowed = sorted(set(SI.sites_T0s + SI.sites_T1s + SI.sites_with_goodAAA))
                if self.request['RequestType'] == 'StepChain':
                    sites_allowed = sorted(set(sites_allowed + SI.HEPCloud_sites))
                    print "Include HEPCloud in the sitewhitelist of ",self.request['RequestName']
                print "Reading premix"
        elif primary:
            sites_allowed =sorted(set(SI.sites_T0s + SI.sites_T1s + SI.sites_T2s))# + SI.sites_T3s))
            if self.request['RequestType'] == 'StepChain':
                    sites_allowed = sorted(set(sites_allowed + SI.HEPCloud_sites))
                    print "Include HEPCloud in the sitewhitelist of ",self.request['RequestName']
        else:
            # no input at all
            ## all site should contribute
            sites_allowed =sorted(set( SI.sites_T0s + SI.sites_T2s + SI.sites_T1s))# + SI.sites_T3s ))
            if self.request['RequestType'] == 'StepChain':
                    sites_allowed = sorted(set(sites_allowed + SI.HEPCloud_sites))
                    print "Include HEPCloud in the sitewhitelist of ",self.request['RequestName']
        if pickone:
            sites_allowed = sorted([SI.pick_CE( sites_allowed )])

        print("Initially allow {}".format(sites_allowed))

        # do further restrictions based on memory
        # do further restrictions based on blow-up factor
        (min_child_job_per_event, root_job_per_event, blow_up) = self.getBlowupFactors()
        UC = unifiedConfiguration()
        max_blow_up,needed_cores = UC.get('blow_up_limits')
        if blow_up > max_blow_up:
            ## then restrict to only sites with >4k slots
            new_sites_allowed = list(set(sites_allowed) & set([site for site in sites_allowed if SI.cpu_pledges[site] > needed_cores]))
            if new_sites_allowed :
                sites_allowed = new_sites_allowed
                print "swaping",verbose
                if verbose:
                    print "restricting site white list because of blow-up factor",min_child_job_per_event, root_job_per_event, max_blow_up

        CI = campaignInfo()
        for campaign in self.getCampaigns():
            c_sites_allowed = CI.get(campaign, 'SiteWhitelist' , [])
            c_sites_allowed.extend(CI.parameters(campaign).get('SiteWhitelist',[]))
            if c_sites_allowed:
                if verbose:
                    print "Using site whitelist restriction by campaign,",campaign,"configuration",sorted(c_sites_allowed)
                sites_allowed = list(set(sites_allowed) & set(c_sites_allowed))
                if not sites_allowed:
                    sites_allowed = list(c_sites_allowed)

            c_black_list = CI.get(campaign, 'SiteBlacklist', [])
            c_black_list.extend( CI.parameters(campaign).get('SiteBlacklist', []))
            if c_black_list:
                if verbose:
                    print "Reducing the whitelist due to black list in campaign configuration"
                    print "Removing",sorted(c_black_list)
                sites_allowed = list(set(sites_allowed) - set(c_black_list))
                sites_not_allowed = c_black_list

        print("After all of these, allowing: {}".format(sorted(sites_allowed)))
        print("After all of these, not allowing: {}".format(sorted(sites_not_allowed)))
        return (lheinput,primary,parent,secondary,sites_allowed,sites_not_allowed)

    def checkSplitting(self):
        #returns hold,<list of params
        ##return None to indicate that things should not proceed
        splits = self.getSplittingsNew(strip=True)
        ncores = self.getMulticore()
        hold = False
        ## for those that are modified, add it and return it
        modified_splits = []
        config_GB_space_limit = unifiedConfiguration().get('GB_space_limit')
        GB_space_limit = config_GB_space_limit*ncores
        output_size_correction = unifiedConfiguration().get('output_size_correction')

        if self.request['RequestType']=='StepChain':
            ## the number of event/lumi should not matter at all.
            ## what you want to be on the look out is that the sum of output size is not too enormeous
            sizeperevent = self.request.get('SizePerEvent',None) ## already summed up by definition of schema
            for spl in splits:
                task = spl['splitParams']
                tname = spl['taskName'].split('/')[-1]
                avg_events_per_job = task.get('events_per_job',None)
                if avg_events_per_job and sizeperevent and (avg_events_per_job * sizeperevent ) > (GB_space_limit*1024.**2):
                    print "The output size of task %s is expected to be large : %d x %.2f kB = %.2f GB > %f GB "% ( tname ,
                                                                                                                    avg_events_per_job, sizeperevent,
                                                                                                                    avg_events_per_job * sizeperevent / (1024.**2 ),
                                                                                                                    GB_space_limit)
                    avg_events_per_job_for_task = int( (GB_space_limit*1024.**2) / sizeperevent)
                    modified_split_for_task = spl
                    modified_split_for_task['splitParams']['events_per_job'] = avg_events_per_job_for_task
                    modified_splits.append( modified_split_for_task )

        elif self.request['RequestType']=='TaskChain':
            events_per_lumi=None
            events_per_lumi_inputs = None
            max_events_per_lumi=[]

            def find_task_dict( name ):
                i_task=1
                while True:
                    tname = 'Task%d'%i_task
                    i_task+=1
                    if not tname in self.request: break
                    if self.request[tname]['TaskName'] == name:
                        return copy.deepcopy( self.request[tname] )
                return None
            
            # Safeguard for small lumi:
            UC = unifiedConfiguration()
            min_lumi = UC.get("min_events_per_lumi_output")
            
            # Flag for small lumi
            small_lumi = False

	        # No safeguard if the input dataset already has small lumi
            (_,prim,_,_) = self.getIO()
		     
            for spl in splits:
                #print spl
                task = spl['splitParams']
                tname = spl['taskName'].split('/')[-1]
                t = find_task_dict( tname )
                ncores = t.get('Multicore', ncores)
                GB_space_limit = config_GB_space_limit ## not multiplied by ncores
                if t.get('KeepOutput',True) == False:
                    ## we can shoot the limit up, as we don't care too much.
                    #GB_space_limit = 10000 * ncores
                    print "the output is not kept, but keeping the output size to",GB_space_limit

                sizeperevent = t.get('SizePerEvent',None)
                for keyword,factor in output_size_correction.items():
                    if keyword in spl['taskName']:
                        sizeperevent *= factor
                        break

                inputs = t.get('InputDataset',None)
                events_per_lumi_inputs = getDatasetEventsPerLumi(inputs) if inputs else events_per_lumi_inputs
                events_per_lumi = events_per_lumi_inputs if events_per_lumi_inputs else events_per_lumi
                timeperevent = t.get('TimePerEvent',None)
                #print "the task split",task
                if 'events_per_lumi' in task:
                    events_per_lumi = task['events_per_lumi']

                ## avg_events_per_job is base on 8h. we could probably put some margin
                if events_per_lumi and 'events_per_job' in task:
                    avg_events_per_job = task['events_per_job']
                    
                    # For the size constraint, need both the filter efficiency of the previous task (efficiency_factor) and of the current task (filter_efficiency_at_this_task). 
                    # The output size of the current task need to take into account of both
                    filter_efficiency_at_this_task = 1.
                    if 'FilterEfficiency' in t:
                        filter_efficiency_at_this_task *= t['FilterEfficiency']
                            
                    ## climb up all task to take the filter eff of the input tasks into account
                    efficiency_factor = 1.
                    while t and 'InputTask' in t:
                        t = find_task_dict( t['InputTask'] )
                        if 'FilterEfficiency' in t:
                            efficiency_factor *= t['FilterEfficiency']
                    
                    # efficiency_factor is the accummulate filter efficiency of the input tasks, not the current task
                    events_per_lumi_at_this_task = events_per_lumi * efficiency_factor  
                    # This is the input events per lumi of the task, after taking filter efficiency of the previous tasks into account

                    if timeperevent:
                        job_timeout = 45. ## hours
                        job_target = 8. ## hours
                        time_per_input_lumi = events_per_lumi_at_this_task*timeperevent

                        if (time_per_input_lumi > (job_timeout*60*60)): ##45h
                            ## even for one lumisection, the job will time out.

                            this_max_events_per_lumi = int( job_target*60.*60. / timeperevent)
                            # This is the possible events per lumi for this task, after taking filter efficiencies of the input tasks into account

                            max_events_per_lumi.append(this_max_events_per_lumi / efficiency_factor) 
                            # This is the possible events per lumi for the whole chain, before taking any filter efficiency into account
                            
                            msg = "The running time of task {} is expected to be too large even for one lumi section: {} x {:.2f} s = {:.2f} h > {} h. Should go as low as {}".format( tname,
                                                                                                                                                     events_per_lumi_at_this_task, timeperevent,
                                                                                                                                                     time_per_input_lumi / (60.*60.),
                                                                                                                                                     job_timeout, this_max_events_per_lumi)
                            self.sendLog('assignor', msg)  

                        else:
                            pass
                    
                    if sizeperevent:# and (avg_events_per_job * sizeperevent ) > (GB_space_limit*1024.**2):
                        size_per_input_lumi = events_per_lumi_at_this_task * sizeperevent * filter_efficiency_at_this_task
                        # This is the output size of this task, after taking filter efficiencies of the input task and current task into account

                        this_max_events_per_lumi = int( (GB_space_limit*1024.**2) / sizeperevent / efficiency_factor / filter_efficiency_at_this_task) 
                        # This is the possible events per lumi for the whole chain, before taking any filter efficiency into account
                        
                        this_max_events_per_job = int( (GB_space_limit*1024.**2) / sizeperevent / filter_efficiency_at_this_task) 
                        # This is the allowed events per job for this task, not relevant to filter efficiency of the previous task
                        
                        if (size_per_input_lumi > (GB_space_limit*1024.**2)):
                            ## derive a value for the lumisection
                            msg = "The output size task %s is expected to be too large : %.2f GB > %f GB even for one lumi (effective lumi size is ~%d), should go as low as %d"% ( tname ,
                                                                                                                                                size_per_input_lumi / (1024.**2 ),
                                                                                                                                                GB_space_limit,
                                                                                                                                                events_per_lumi_at_this_task,
                                                                                                                                                this_max_events_per_lumi)
                            self.sendLog('assignor', msg) 

                            max_events_per_lumi.append( this_max_events_per_lumi ) ## adding this to that later on we can check and adapt the split 0

                        elif (avg_events_per_job * sizeperevent * filter_efficiency_at_this_task) > (GB_space_limit*1024.**2):
                            # avg_events_per_job is the current events_per_job setting in this task dict
                            # should still change the avg_events_per_job setting of that task
                            
                            msg = "The output size of task %s is expected to be too large : %d x %.2f kB * %.4f = %.2f GB > %f GB. Reducing to %d "% ( tname ,
                                                                                                                                                 avg_events_per_job, sizeperevent, filter_efficiency_at_this_task, 
                                                                                                                                                 avg_events_per_job * sizeperevent * filter_efficiency_at_this_task / (1024.**2),
                                                                                                                                                 GB_space_limit,
                                                                                                                                                 this_max_events_per_job)
                            self.sendLog('assignor', msg) # We don't hold the workflow here so no need to send critical message

                            modified_split_for_task = spl
                            modified_split_for_task['splitParams']['events_per_job'] = this_max_events_per_job
                            modified_splits.append( modified_split_for_task )
                            max_events_per_lumi.append( this_max_events_per_lumi ) ## adding this to that later on we can check and adapt the split 0

                    # Safeguard for small lumi per task:
                    effective_output_lumi_at_this_task = events_per_lumi_at_this_task * filter_efficiency_at_this_task 
                    if max_events_per_lumi:
                        effective_output_lumi_at_this_task = min(events_per_lumi_at_this_task, min(max_events_per_lumi)) * filter_efficiency_at_this_task
                    
                    if (effective_output_lumi_at_this_task < min_lumi) and (not prim) and (not self.isRelval()) and (not small_lumi): # Only do this once per workflow
                            msg = "{} will get {} events per lumi in output. Smaller than {} is troublesome.".format(tname, effective_output_lumi_at_this_task, min_lumi)
                            self.sendLog('assignor',msg)
                            critical_msg = msg + '\nWorkflow URL: https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=task_{}'.format(self.getPrepIDs()[0])
                            sendLog('assignor', critical_msg, level='critical')
                            hold = True
                            small_lumi = True
            
            # Double check across the whole chain:
            if max_events_per_lumi:
                if events_per_lumi_inputs:
                    if min(max_events_per_lumi)<events_per_lumi_inputs:
                        ## there was an input dataset somewhere and we cannot break down that lumis, except by changing to EventBased
                        msg = "Possible events per lumi of this wf (min(%s)) is smaller than %s evt/lumi of the input dataset"%(max_events_per_lumi, events_per_lumi_inputs)
                        self.sendLog('assignor', msg)
			critical_msg = msg + '\nWorkflow URL: https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=task_{}'.format(self.getPrepIDs()[0])
                        sendLog('assignor', critical_msg, level='critical')
                        hold = True
                    else:
                        print "The smallest value of %s is ok compared to %s evt/lumi in the input"%(max_events_per_lumi, events_per_lumi_inputs)
                else:
                    root_split = splits[0]
                    current_split = root_split.get('splitParams',{}).get('events_per_lumi',None)
		    
                    if current_split:
                        if current_split > min(max_events_per_lumi):
                            root_split['splitParams']['events_per_lumi'] = min(max_events_per_lumi)
                            modified_splits.append( root_split )

        ## the return list can easily be used to call the splitting api of reqmgr2
        return hold,modified_splits


    def getFilterEfficiency( self, taskName ):
        feff = 1.

        itask = 1
        while True:
            ti = 'Task%d'% itask
            itask +=1
            if ti in self.request:
                if self.request[ti]['TaskName'] == taskName:
                    feff = self.request[ti].get('FilterEfficiency',feff)
                    break
            else:
                break

        return feff


    def getSchema(self):
        self.conn = make_x509_conn(self.url)
        r1=self.conn.request("GET",'/reqmgr2/data/request?name=%s'%self.request['RequestName'], headers={"Accept":"application/json"} )
        r2=self.conn.getresponse()
        new_schema = copy.deepcopy( json.loads(r2.read())['result'][0][self.request['RequestName']])

        ## put in the era accordingly ## although this could be done in re-assignment
        ## take care of the splitting specifications ## although this could be done in re-assignment
        for (k,v) in new_schema.items():
            if v in [None,'None']:
                new_schema.pop(k)
        return new_schema

    def _taskDescending(self, node, select=None):
        all_tasks=[]
        if (not select):# or (select and node.taskType == select):
            all_tasks.append( node )
        else:
            for (key,value) in select.items():
                if (type(value)==list and getattr(node,key) in value) or (type(value)!=list and getattr(node, key) == value):
                    all_tasks.append( node )
                    break

        for child in node.tree.childNames:
            ch = getattr(node.tree.children, child)
            all_tasks.extend( self._taskDescending( ch, select) )
        return all_tasks

    def getWorkTasks(self):
        return self.getAllTasks(select={'taskType':['Production','Processing','Skim']})

    def getExpectedPerTask(self):
        return {}

    def getCompletionFraction(self, caller='getCompletionFraction', with_event=True):
        output_per_task = self.getOutputPerTask()
        task_outputs = {}
        for task,outs in output_per_task.items():
            for out in outs:
                task_outputs[out] = task

        percent_completions = {}
        event_expected_per_task = {}
        ## for all the outputs
        event_expected,lumi_expected = self.request.get('TotalInputEvents',0),self.request.get('TotalInputLumis', 0)

        ttype = 'Task' if 'TaskChain' in self.request else 'Step'
        it = 1
        tname_dict = {}
        while True:
            tt = '%s%d'%(ttype,it)
            it+=1
            if tt in self.request:
                tname = self.request[tt]['%sName'% ttype]
                tname_dict[tname] = tt
                if not 'Input%s'%ttype in self.request[tt] and 'RequestNumEvents' in self.request[tt]:
                    ## pick up the value provided by the requester, that will work even if the filter effiency is broken
                    event_expected = self.request[tt]['RequestNumEvents']
            else:
                break

        if '%sChain'%ttype in self.request:
            ## go on and make the accounting
            it = 1
            while True:
                tt = '%s%d'%(ttype,it)
                it+=1
                if tt in self.request:
                    tname = self.request[tt]['%sName'% ttype]
                    event_expected_per_task[tname] = event_expected
                    ### then go back up all the way to the root task to count filter-efficiency
                    a_task = self.request[tt]
                    while 'Input%s'%ttype in a_task:
                        event_expected_per_task[tname] *= a_task.get('FilterEfficiency',1)
                        mother_task = a_task['Input%s'%ttype]
                        ## go up
                        a_task = self.request[ tname_dict[mother_task] ]
                else:
                    break

        for output in self.request['OutputDatasets']:
            event_count,lumi_count = getDatasetEventsAndLumis(dataset=output)
            percent_completions[output] = 0.
            if lumi_expected:
                percent_completions[output] = lumi_count / float( lumi_expected )
                self.sendLog(caller, "lumi completion %s expected %d for %s"%( lumi_count, lumi_expected, output))
            output_event_expected = event_expected_per_task.get(task_outputs.get(output,'NoTaskFound'))
            if output_event_expected and with_event:
                e_fraction = float(event_count) / float( output_event_expected )
                if e_fraction > percent_completions[output]:
                    percent_completions[output] = e_fraction
                    self.sendLog(caller, "overiding : event completion real %s expected %s for %s"%(event_count, output_event_expected, output))

        return percent_completions


    def getOutputPerTask(self):
        all_outputs = self.request['OutputDatasets']
        output_per_task = defaultdict(list)
        if 'ChainParentageMap' in self.request:
            for t,info in self.request['ChainParentageMap'].items():
                for dsname in info.get('ChildDsets'):
                    output_per_task[t].append( dsname)
            return dict(output_per_task)
        for t in self.getWorkTasks():
            parse_what = t.subscriptions.outputModules if hasattr(t.subscriptions,'outputModules') else t.subscriptions.outputSubs
            if parse_what:
                for om in parse_what:
                    dsname = getattr(t.subscriptions, om).dataset
                    if dsname in all_outputs: ## do the intersection with real outputs
                        output_per_task[t._internal_name].append( dsname )
            else:
                print "no output subscriptions..."
                
        return dict(output_per_task)

    def getAllTasks(self, select=None):
        all_tasks = []
        for task in self._tasks():
            ts = getattr(self.get_spec().tasks, task)
            all_tasks.extend( self._taskDescending( ts, select ) )
        return all_tasks

    def getSplittingsNew(self ,strip=False,all_tasks=False):
        self.conn = make_x509_conn(self.url)
        r1=self.conn.request("GET",'/reqmgr2/data/splitting/%s'%self.request['RequestName'], headers={"Accept":"application/json"} )
        r2=self.conn.getresponse()
        result = json.loads( r2.read() )['result']
        splittings = []
        for spl in result:
            if not all_tasks and not spl['taskType'] in ['Production','Processing','Skim'] : continue
            if strip:
                for drop in ['algorithm','trustPUSitelists','trustSitelists','deterministicPileup','type','include_parents','lheInputFiles','runWhitelist','runBlacklist','collectionName','group','couchDB','couchURL','owner','initial_lfn_counter','filesetName', 'runs','lumis']:
                    spl['splitParams'].pop(drop, None)
                if spl['splitAlgo'] == 'LumiBased':
                    for drop in ['events_per_job','job_time_limit']:
                        spl['splitParams'].pop(drop, None)                        
                    
            splittings.append( spl )

        return splittings

    def getSplittings(self):
        spl =[]
        for task in self.getWorkTasks():
            ts = task.input.splitting
            spl.append( { "splittingAlgo" : ts.algorithm,
                          "splittingTask" : task.pathName,
                          } )
            get_those = ['events_per_lumi','events_per_job','lumis_per_job','halt_job_on_file_boundaries','max_events_per_lumi','halt_job_on_file_boundaries_event_aware']#,'couchdDB']#,'couchURL']#,'filesetName']
            translate = {
                'EventAwareLumiBased' : [('events_per_job','avg_events_per_job')]
                }
            include = {
                'EventAwareLumiBased' : { 'halt_job_on_file_boundaries_event_aware' : 'True' },
                'LumiBased' : { 'halt_job_on_file_boundaries' : 'True'}
                }
            if ts.algorithm in include:
                for k,v in include[ts.algorithm].items():
                    spl[-1][k] = v

            for get in get_those:
                if hasattr(ts,get):
                    set_to = get
                    if ts.algorithm in translate:
                        for (src,des) in translate[ts.algorithm]:
                            if src==get:
                                set_to = des
                                break
                    spl[-1][set_to] = getattr(ts,get)

        return spl


    def getCurrentStatus(self):
        return self.request['RequestStatus']

    def getRequestNumEvents(self):
        if 'RequestNumEvents' in self.request and int(self.request['RequestNumEvents']):
            return int(self.request['RequestNumEvents'])
        else:
            return int(self.request['Task1']['RequestNumEvents'])

    def getPriority(self):
        return self.request['RequestPriority']

    def getMemoryPerTask(self, task):
        mems = self.request.get('Memory',None)
        mems_d = {}
        if 'Chain' in self.request['RequestType']:
            mems_d = self._collectinchain('Memory',default=None)
        return int(mems_d.get( task, mems))
    def getMemory(self):
        mems = self.getMemories()
        return max(mems) if mems else None

    def getCampaignPerTask(self, task):
        c = self.request.get('Campaign',None)
        c_d = {}
        if 'Chain' in self.request['RequestType']:
            c_d = self._collectinchain('Campaign', default=None)
        return c_d.get(task, c)

    def getMemories(self):
        mems = [self.request.get('Memory',None)]
        mems_d = {}
        if 'Chain' in self.request['RequestType']:
            mems_d = self._collectinchain('Memory',default=None)
        mems = filter(None, mems_d.values()) if mems_d else mems
        return mems

    def getMulticores(self):
        mcores = self.request.get('Multicore',1)
        mcores_d = {}
        if 'Chain' in self.request['RequestType']:
            mcores_d = self._collectinchain('Multicore',default=1)
        return mcores_d.values() if mcores_d else [mcores]

    def getCorePerTask(self, task):
        mcores = self.request.get('Multicore',1)
        mcores_d = {}
        if 'Chain' in self.request['RequestType']:
            mcores_d = self._collectinchain('Multicore',default=1)
        return int(mcores_d.get( task, mcores ))

    def getArchs(self):
        archs = self.request.get('ScramArch')
        if 'Chain' in self.request['RequestType']:
            arch_d = self._collectinchain('ScramArch',default=[])
            for t in arch_d:
                archs.extend( arch_d[t] )
        return archs

    def getMulticore(self):
        mcores = [int(self.request.get('Multicore',1))]
        if 'Chain' in self.request['RequestType']:
            mcores_d = self._collectinchain('Multicore',default=1)
            mcores.extend( map(int, mcores_d.values() ))
        return max(mcores)

    def getBlockWhiteList(self):
        bwl=[]
        if 'Chain' in self.request['RequestType']:
            bwl_t = self._collectinchain('BlockWhitelist')
            for task in bwl_t:
                bwl.extend(bwl_t[task])
        else:
            if 'BlockWhitelist' in self.request:
                bwl.extend(self.request['BlockWhitelist'])

        return list(set(bwl))
    def getLumiWhiteList(self):
        lwl={}
        if 'Chain' in self.request['RequestType']:
            lwl_t = self._collectinchain('LumiList')
            for task in lwl_t:
                lwl.update(lwl_t[task])
        else:
            if 'LumiList' in self.request:
                lwl.update(self.request['LumiList'])
        return lwl
    def getRunWhiteList(self):
        lwl=[]
        if 'Chain' in self.request['RequestType']:
            lwl_t = self._collectinchain('RunWhitelist')
            for task in lwl_t:
                lwl.extend(lwl_t[task])
        else:
            if 'RunWhitelist' in self.request:
                lwl.extend(self.request['RunWhitelist'])
        return list(set(lwl))

    def getBlocks(self):
        blocks = set()
        (_,primary,_,_) = self.getIO()

        blocks.update( self.getBlockWhiteList() )
        run_list = self.getRunWhiteList()
        if run_list:
            for dataset in primary:
                blocks.update( getDatasetBlocks( dataset, runs=run_list ) )
        lumi_list = self.getLumiWhiteList()
        if lumi_list:
            for dataset in primary:
                blocks.update( getDatasetBlocks( dataset, lumis=lumi_list ) )
        return list( blocks )
    def getIO(self):
        lhe=False
        primary=set()
        parent=set()
        secondary=set()
        def IOforTask( blob ):
            lhe=False
            primary=set()
            parent=set()
            secondary=set()
            if 'InputDataset' in blob:
                primary = set(filter(None,[blob['InputDataset']]))
            if primary and 'IncludeParents' in blob and blob['IncludeParents']:
                for p in primary:
                    parent.update(findParent( p ))
            if 'MCPileup' in blob:
                secondary = set(filter(None,[blob['MCPileup']]))
            if 'LheInputFiles' in blob and blob['LheInputFiles'] in ['True',True]:
                lhe=True

            return (lhe,primary, parent, secondary)

        if 'Chain' in self.request['RequestType']:
            base = self.request['RequestType'].replace('Chain','')
            t=1
            while '%s%d'%(base,t) in self.request:
                (alhe,aprimary, aparent, asecondary) = IOforTask(self.request['%s%d'%(base,t)])
                if alhe: lhe=True
                primary.update(aprimary)
                parent.update(aparent)
                secondary.update(asecondary)
                t+=1
        else:
            (lhe,primary, parent, secondary) = IOforTask( self.request )

        return (lhe,primary, parent, secondary)

    def _collectinchain(self, member, func=None, default=None):
        if self.request['RequestType'] == 'StepChain':
            return self._collectin_uhm_chain(member,func,default,base='Step')
        elif self.request['RequestType'] == 'TaskChain':
            return self._collectin_uhm_chain(member,func,default,base='Task')
        else:
            raise Exception("should not call _collectinchain on non-chain request")

    def _collectin_uhm_chain( self , member, func=None,default=None, base=None):
        coll = {}
        t=1
        while '%s%d'%(base,t) in self.request:
            if member in self.request['%s%d'%(base,t)]:
                if func:
                    coll[self.request['%s%d'%(base,t)]['%sName'%base]] = func(self.request['%s%d'%(base,t)][member])
                else:
                    coll[self.request['%s%d'%(base,t)]['%sName'%base]] = self.request['%s%d'%(base,t)].get(member, default)
            t+=1
        return coll

    def getCampaign( self ):
        return self.request['Campaign']

    def getPrimaryDSN(self):
        if 'Chain' in self.request['RequestType']:
            return self._collectinchain('PrimaryDataset').values()
        else:
            return [self.request['PrimaryDataset']]

    def getConfigCacheID(self, taskname =None ):
        tasks = self.getWorkTasks()
        mapping = {}
        for task in tasks:
            name =  task.pathName.split('/')[-1]
            cid = task.steps.cmsRun1.application.configuration.configId
            mapping[name] = cid
        return mapping

    def getCampaigns(self):
        if 'Chain' in self.request['RequestType'] and not self.isRelval():
            return list(set(self._collectinchain('AcquisitionEra').values()))
        else:
            return [self.request['Campaign']]

    def campaigns(self):
        if 'Chain' in self.request['RequestType']:
            if self.isRelval():
                return self._collectinchain('Campaign')
            else:
                return self._collectinchain('AcquisitionEra')
        else:
            return self.request['Campaign']

    def acquisitionEra( self ):
        def invertDigits( st ):
            if st[0].isdigit():
                number=''
                while st[0].isdigit():
                    number+=st[0]
                    st=st[1:]
                isolated=[(st[i-1].isupper() and st[i].isupper() and st[i+1].islower()) for i in range(1,len(st)-1)]
                if any(isolated):
                    insert_at=isolated.index(True)+1
                    st = st[:insert_at]+number+st[insert_at:]
                    return st
                print "not yet implemented",st
                sys.exit(34)
            return st

        if 'Chain' in self.request['RequestType']:
            acqEra = self._collectinchain('AcquisitionEra')
        else:
            acqEra = self.request['AcquisitionEra']
        return acqEra

    def processingString(self):
        if 'Chain' in self.request['RequestType']:
            return self._collectinchain('ProcessingString')
        else:
            return self.request['ProcessingString']



    def go(self,log=False):
        CI = campaignInfo()
        pss = self.processingString()
        #aes = self.acquisitionEra()
        aes = self.campaigns()

        if type(pss) == dict:
	    try:
            	pas = [(aes[t],pss[t]) for t in pss]
	    except Exception as e:
		err_msg = str(e)
		err_msg += "\nAvailable processing string: {}".format(pss)
		err_msg += "\nAvailable campaign: {}".format(aes)
		err_msg += "\nNo go because keys don't match."
		print(err_msg)
		self.sendLog('go', err_msg)
		sendEmail('failed assigning',err_msg)
		return False
        else:
            pas = [(aes,pss)]
	
	# If there is 'pilot' keyword in any of processing String, just assign the workflow
	for campaign,label in pas:
	    if 'pilot' in label.lower():
		msg = "Detected 'pilot' keyword in processingString {} in campaign {}. Assigning the workflow.".format(label,campaign)
		if log:
		    self.sendLog('go',msg)
		else:
		    print(msg)
		return True
        # If there is 'pilot' in SubRequestType (an alternative pilot)
        if 'SubRequestType' in self.request and 'pilot' in self.request['SubRequestType']:
            print "Alternative pilot"
            return True        
 	
        for campaign,label in pas:
            if not CI.go( campaign, label):
                if log:
                    self.sendLog('go',"no go due to %s %s"%(campaign,label))
                else:
                    print "no go due to",campaign,label
                return False
        return True



    def getNextVersion( self ):
        ## returns 1 if nothing is in the way
        if 'ProcessingVersion' in self.request:
            version = max(0,int(self.request['ProcessingVersion'])-1)
        else:
            version = 0
        outputs = self.request['OutputDatasets']
        era = self.acquisitionEra()
        ps = self.processingString()
        if self.request['RequestType'] == 'TaskChain':
            for output in outputs:
                (_,dsn,ps,tier) = output.split('/')
                if ps.count('-')==2:
                    (aera,aps,_) = ps.split('-')
                elif ps.count('-')==3:
                    (aera,fn,aps,_) = ps.split('-')
                else:
                    aera='*'
                    aps='*'
                pattern = '/'.join(['',dsn,'-'.join([aera,aps,'v*']),tier])
                wilds = getDatasets( pattern )
                print pattern,"->",len(wilds),"match(es)"
                for wild in [wildd['dataset'] for wildd in wilds]:
                    (_,_,mid,_) = wild.split('/')
                    v = int(mid.split('-')[-1].replace('v',''))
                    version = max(v,version)
            for output in outputs:
                (_,dsn,ps,tier) = output.split('/')
                if ps.count('-')==2:
                    (aera,aps,_) = ps.split('-')
                elif ps.count('-')==3:
                    (aera,fn,aps,_) = ps.split('-')
                else:
                    print "Cannot check output in reqmgr"
                    print output,"is what is in the request workload"
                    continue
                while True:
                    predicted = '/'.join(['',dsn,'-'.join([aera,aps,'v%d'%(version+1)]),tier])
                    print "checking against",predicted
                    conflicts = getWorkflowByOutput( self.url, predicted )
                    conflicts = filter(lambda wfn : wfn!=self.request['RequestName'], conflicts)
                    if len(conflicts):
                        print "There is an output conflict for",self.request['RequestName'],"with",conflicts
                        ## since we are not planned for pure extension and ever writing in the same dataset, go +1
                        version += 1
                    else:
                        break

        else:
            for output in  outputs:
                print output
                (_,dsn,ps,tier) = output.split('/')
                if ps.count("-") == 2:
                    (aera,aps,_) = ps.split('-')
                elif ps.count("-") == 3:
                    (aera,fn,aps,_) = ps.split('-')
                else:
                    ## cannot so anything
                    print "the processing string is mal-formated",ps
                    return None

                if aera == 'None' or aera == 'FAKE':
                    print "no era, using ",era
                    aera=era
                if aps == 'None':
                    print "no process string, using wild char"
                    aps='*'
                pattern = '/'.join(['',dsn,'-'.join([aera,aps,'v*']),tier])
                print "looking for",pattern
                wilds = getDatasets( pattern )
                print pattern,"->",len(wilds),"match(es)"
                for wild in [wildd['dataset'] for wildd in wilds]:
                    (_,_,mid,_) = wild.split('/')
                    v = int(mid.split('-')[-1].replace('v',''))
                    version = max(v,version)
            #print "version found so far",version
            for output in  outputs:
                print output
                (_,dsn,ps,tier) = output.split('/')
                if ps.count("-") == 2:
                    (aera,aps,_) = ps.split('-')
                elif ps.count("-") == 3:
                    (aera,fn,aps,_) = ps.split('-')
                else:
                    print "the processing string is mal-formated",ps
                    return None

                if aera == 'None' or aera == 'FAKE':
                    print "no era, using ",era
                    aera=era
                if aps == 'None':
                    print "no process string, cannot parse"
                    continue
                while True:
                    predicted = '/'.join(['',dsn,'-'.join([aera,aps,'v%d'%(version+1)]),tier])
                    conflicts = getWorkflowByOutput( self.url, predicted )
                    conflicts = filter(lambda wfn : wfn!=self.request['RequestName'], conflicts)
                    if len(conflicts):
                        print "There is an output conflict for",self.request['RequestName'],"with",conflicts
                        #return None
                        ## since we are not planned for pure extension and ever writing in the same dataset, go +1
                        version += 1
                    else:
                        break

        return version+1

def getFailedJobs(taskname, caller='getFailedJobs'):
    wfname=taskname.split('/')[1]
    print 'wfname=',wfname
    conn = make_x509_conn(reqmgr_url)
    try:
        r1=conn.request("GET",'/wmstatsserver/data/filtered_requests?RequestName=%s&mask=PrepID&mask=AgentJobInfo'%(wfname),headers={"Accept":"application/json"})
        r2=conn.getresponse()
        reading = json.loads(r2.read())
    except Exception as e:
        sendLog('componentInfo','not able to connect to wmstats server', level='critical')
        print str(e)
        return 0

    failed_jobs = 0

    for info in reading['result']:
        if info.get('AgentJobInfo', {}) is not None:
            for agentName in info.get('AgentJobInfo', {}):
                if taskname in info['AgentJobInfo'][agentName].get("tasks", {}):
                    taskInfo = info['AgentJobInfo'][agentName]["tasks"][taskname]
                    if "failure" in taskInfo.get("status", {}):
                        for failureType, numFailures in taskInfo["status"]["failure"].items():
                            failed_jobs += numFailures
    
    return failed_jobs