import sys
import urllib, urllib2
import logging
from dbs.apis.dbsClient import DbsApi
#import reqMgrClient
import httplib
import os
import json 
from collections import defaultdict
import random
from xml.dom.minidom import getDOMImplementation
import copy 
import pickle
import itertools
import time
import math 
import hashlib

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email.utils import make_msgid

dbs_url = os.getenv('UNIFIED_DBS3_READER' ,'https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
dbs_url_writer = os.getenv('UNIFIED_DBS3_WRITER','https://cmsweb.cern.ch/dbs/prod/global/DBSWriter')

phedex_url = os.getenv('UNIFIED_PHEDEX','cmsweb.cern.ch')
reqmgr_url = os.getenv('UNIFIED_REQMGR','cmsweb.cern.ch')
monitor_dir = os.getenv('UNIFIED_MON','/afs/cern.ch/user/c/cmst2/www/unified/')
base_dir =  os.getenv('UNIFIED_DIR','/afs/cern.ch/user/c/cmst2/Unified/')
cache_dir = '/afs/cern.ch/work/c/cmst2/unified/cache'

FORMAT = "%(module)s.%(funcName)s(%(lineno)s) => %(message)s (%(asctime)s)"
DATEFMT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format = FORMAT, datefmt = DATEFMT, level=logging.DEBUG)

def sendDashboard( subject, text, criticality='info', show=True):
    ### this sends something to the dashboard ES for error, info, messages
    pass
    
def sendLog( subject, text , wfi = None, show=True ,level='info'):
    try:
        try_sendLog( subject, text , wfi, show, level)
    except Exception as e:
        print "failed to send log to elastic search"
        print str(e)
        sendEmail('failed logging',subject+text+str(e))

def searchLog( q , actor=None, limit=50 ):
    conn = httplib.HTTPConnection( 'cms-elastic-fe.cern.ch:9200' )
    goodquery={
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "meta": "*%s*"%q
                            }
                        },
                    #{
                        #"term": {
                        #    "subject": "assignor"
                        #    }
                    #    }
                    ]
                }
            },
        "sort": [
            {
                "timestamp": "desc"
                }
            ],
        "_source": [
            "text",
            "subject",
            "date",
            "meta"
            ]
        }

    if actor:
        goodquery['query']['bool']['must'][0]['wildcard']['subject'] = actor

    conn.request("POST" , '/logs/_search?size=%d'%limit, json.dumps(goodquery))
    ## not it's just a matter of sending that query to ES.
    #lq = q.replace(':', '\:').replace('-','\\-')
    #conn.request("GET" , '/logs/_search?q=text:%s'% lq)

    response = conn.getresponse()
    data = response.read()
    o = json.loads( data )

    print o
    print o['hits']['total']
    return o['hits']['hits']

def try_sendLog( subject, text , wfi = None, show=True, level='info'):
    #import pdb
    #pdb.set_trace()
    conn = httplib.HTTPConnection( 'cms-elastic-fe.cern.ch:9200' )    

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
    conn.request("POST" , '/logs/log/', json.dumps(doc)) 
    response = conn.getresponse()
    data = response.read()
    try:
        res = json.loads( data ) 
        #print 'log:',res['_id'],"was created"
    except Exception as e:
        print "failed"
        print str(e)
        pass

def sendEmail( subject, text, sender=None, destination=None ):
    #print subject
    #print text
    #print sender
    #print destination
    
    if not destination:
        destination = ['vlimant@cern.ch','matteoc@fnal.gov','areinsvo@nd.edu']
    else:
        destination = list(set(destination+['vlimant@cern.ch','matteoc@fnal.gov','areinsvo@nd.edu']))
    if not sender:
        map_who = { 'vlimant' : 'vlimant@cern.ch',
                    'mcremone' : 'matteoc@fnal.gov' }
        user = os.getenv('USER')
        if user in map_who:
            sender = map_who[user]
        else:
            sender = 'vlimant@cern.ch'

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

def download_data(url = None, params = None, headers = None, logger = None):
    """
    Returns data got from server.
    params has to be a dictionary, which can contain 
    "key : [value1, value2,...], and that will be 
    converted to key=value1&key=value2&... 
    """
    if not logger:
        logger = logging
    try:
        if params:
            params = url_encode_params(params)
            url = "{url}?{params}".format(url = url, params = params)
        response = urllib2.urlopen(url)
        data = response.read()
        return data
    except urllib2.HTTPError as err:
        error = "{msg} (HTTP Error: {code})"
        logger.error(error.format(code = err.code, msg = err.msg))
        logger.error("URL called: {url}".format(url = url))
        return None
        
def download_file(url, params, path = None, logger = None):
    if not logger:
        logger = logging
    if params:
        params = url_encode_params(params)
        url = "{url}?{params}".format(url = url, params = params)
    logger.debug(url)
    try:
        filename, message = urllib.urlretrieve(url)
        return filename
    except urllib2.HTTPError as err:
        error = "{msg} (HTTP Error: {code})"
        logger.error(error.format(code = err.code, msg = err.msg))
        logger.error("URL called: {url}".format(url = url))
        return None

def GET(url, there, l=True):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",there)
    r2=conn.getresponse()
    if l:
        return json.loads(r2.read())
    else:
        return r2

def check_ggus( ticket ):
    conn  =  httplib.HTTPSConnection('ggus.eu', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/index.php?mode=ticket_info&ticket_id=%s&writeFormat=XML'%ticket)
    r2=conn.getresponse()
    print r2
    return False

def getSubscriptions(url, dataset):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/phedex/datasvc/json/prod/subscriptions?dataset='+dataset 
    r1=conn.request("GET", there)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']
    return items

def listRequests(url, dataset, site=None):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/phedex/datasvc/json/prod/requestlist?dataset='+dataset
    r1=conn.request("GET", there)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['request']
    res= defaultdict(list)
    for item in items:
        for node in item['node']:
            if site and node['name']!=site: continue
            if not item['id'] in res[node['name']]:
                res[node['name']].append(item['id'])
    for s in res:
        res[s] = sorted(res[s])
    return dict(res)

def listCustodial(url, site='T1_*MSS'):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/phedex/datasvc/json/prod/requestlist?node=%s&decision=pending'%site
    r1=conn.request("GET", there)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['request']
    res= defaultdict(list)
    for item in items:
        if item['type'] != 'xfer': continue
        for node in item['node']:
            if not item['id'] in res[node['name']]:
                res[node['name']].append(item['id'])
    for s in res:
        res[s] = sorted(res[s])
    return dict(res)

def listDelete(url, user, site=None):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/phedex/datasvc/json/prod/requestlist?type=delete&approval=pending&requested_by=%s'% user
    if site:
        there += 'node=%s'% ','.join(site)
    r1=conn.request("GET", there)

    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['request']
    #print json.dumps(items, indent=2)
    
    return list(itertools.chain.from_iterable([(subitem['name'],item['requested_by'],item['id']) for subitem in item['node'] if subitem['decision']=='pending' ] for item in items))

def listSubscriptions(url, dataset, within_sites=None):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset=%s'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['request']
    destinations ={}
    deletes = defaultdict(int)
    for item in items:
        for node in item['node']:
            site = node['name']
            if item['type'] == 'delete' and node['decision'] in [ 'approved','pending']:
                deletes[ site ] = max(deletes[ site ], node['time_decided'])

    for item in items:
        for node in item['node']:
            if item['type']!='xfer': continue
            site = node['name']            
            if within_sites and not site in within_sites: continue
            #print item
            if not 'MSS' in site:
                ## pending delete
                if site in deletes and not deletes[site]: continue
                ## delete after transfer
                #print  node['time_decided'],site
                if site in deletes and deletes[site] > node['time_decided']: continue

                destinations[site]=(item['id'], node['decision']=='approved')
                #print node['name'],node['decision']
                #print node
    #print destinations
    return destinations

"""
class newLockInfo:
    def __init__(self):
        self.db = json.loads(open('%s/globallocks.json'%monitor_dir).read())
        os.system('echo `date` > %s/globallocks.json.lock'%monitor_dir)

    def free(self):
        started = time.mktime(time.gmtime())
        max_wait = 50. #120*60. #2h
        sleep_time = 30
        locked = False
        while True:
            r = os.popen('curl -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/inActionLock.txt').read()
            if not ('Not Found' in r):
                sendLog('LockInfo','%s DDM lock is present\n%s'%(time.asctime(time.gmtime()),r),level='warning')
                locked = True
                now = time.mktime(time.gmtime())
                if (now-started) > max_wait: break
                else:
                    print "pausing"
                    time.sleep(sleep_time)
            else:
                locked = False
                break

        return (not locked)

    def __del__(self):
        open('%s/globallocks.json.new'%monitor_dir,'w').write(json.dumps( sorted(list(set(self.db))) , indent=2 ))
        os.system('mv %s/globallocks.json.new %s/globallocks.json'%(monitor_dir,monitor_dir))
        os.system('rm -f %s/globallocks.json.lock'%monitor_dir)

    def lock(self, dataset):
        # just put the 
        if dataset in self.db:
            print "\t",dataset,"was already locked"
        else:
            self.db.append(dataset)
        sendLog('lockInfo',"[new lock] "+dataset+" to be locked")

    def release(self, dataset):
        print "[new lock] should never release datasets"
        #return
        if not dataset in self.db:
            print "\t",dataset,"was not locked already"
        else:
            print "taking",dataset,"out of the lock"
            self.db.remove( dataset )
        

"""


class lockInfo:
    def __init__(self, andwrite=True):
        self.lockfilename = 'globallocks' ## official name
        self.writeondelete = andwrite
        if self.writeondelete:
            os.system('echo `date` > %s/globallocks.json.lock'%monitor_dir)

    def free(self):
        started = time.mktime(time.gmtime())
        max_wait = 50. #120*60. #2h
        sleep_time = 30
        locked = False
        while True:
            r = os.popen('curl -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/inActionLock.txt').read()
            if not ('Not Found' in r):
                sendLog('LockInfo','DDM lock is present\n%s'%(r),level='warning')
                locked = True
                now = time.mktime(time.gmtime())
                if (now-started) > max_wait: break
                else:
                    print "pausing"
                    time.sleep(sleep_time)
            else:
                locked = False
                break

        return (not locked)

    def __del__(self):
        try:
            from assignSession import session, Lock
            out = []
            detailed_out = {}
            all_locks = session.query(Lock).all()
            print len(all_locks),"existing locks"
            for lock in all_locks:
                if lock.lock:
                    out.append( lock.item )
                    detailed_out[lock.item] = { 'date' : lock.time,
                                                'reason' : lock.reason
                                                }
                else:
                    #print "poping",lock.item
                    pass
                    ## let's not do that for now
                    #session.delete( lock )
                    #session.commit()                    

            #print "writing to json"
            if self.writeondelete:
                print "writing",len( out ),"locks to the json interface"
                open('%s/%s.json.new'%(monitor_dir,self.lockfilename),'w').write( json.dumps( sorted(out) , indent=2))
                os.system('mv %s/%s.json.new %s/%s.json'%(monitor_dir,self.lockfilename,monitor_dir,self.lockfilename))
                open('%s/%s.detailed.json.new'%(monitor_dir,self.lockfilename),'w').write( json.dumps( detailed_out , indent=2))
                os.system('mv %s/%s.detailed.json.new %s/%s.detailed.json'%(monitor_dir,self.lockfilename,monitor_dir,self.lockfilename))
                os.system('rm -f %s/globallocks.json.lock'%monitor_dir)
        except Exception as e:
            print "Failed writing locks"
            print str(e)
        
    def release(self, item ):
        try:
            self._release(item)
        except Exception as e:
            print "failed to release"
            print str(e)
            
    def _release(self, item ):
        #from dataLock import locksession, Lock
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
        #from dataLock import locksession, Lock
        from assignSession import session, Lock
        l = session.query(Lock).filter(Lock.item == item).first()
        do_com = False
        if not l:
            print "in lock, making a new object for",item
            l = Lock(lock=False)
            #l.site = site
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
        if reason!=l.reason:
            l.reason = reason
            do_com =True
            message+=" because of %s"%reason
        if do_com:
            sendLog('lockInfo',message)
            l.time = now
            session.commit()

    def lock(self, item, site='', reason='staging'):
        try:
            self._lock( item, site, reason)
        except Exception as e:
            ## to be removed once we have a fully functional lock db
            print "could not lock",item,"at",site
            print str(e)
            
    
    def items(self, locked=True):
        #from dataLock import locksession, Lock
        from assignSession import session, Lock
        ret = sorted([ l.item for l in session.query(Lock).all() if l.lock==locked])
        return ret

    def tell(self, comment):
        #from dataLock import locksession, Lock
        from assignSession import session, Lock
        print "---",comment,"---"
        for l in session.query(Lock).all():
            print l.item,l.lock
        print "------"+"-"*len(comment)


class unifiedConfiguration:
    def __init__(self):
        self.configs = json.loads(open('%s/WmAgentScripts/unifiedConfiguration.json'%base_dir).read())
        
    def get(self, parameter):
        if parameter in self.configs:
            return self.configs[parameter]['value']
        else:
            print parameter,'is not defined in global configuration'
            print ','.join(self.configs.keys()),'possible'
            sys.exit(124)

def checkDownTime():
    conn  =  httplib.HTTPSConnection(reqmgr_url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/jbadillo_BTV-RunIISpring15MiniAODv2-00011_00081_v0__151030_162715_5312')
    r2=conn.getresponse()
    r = r2.read()
    if r2.status ==503:#if 'The site you requested is not unavailable' in r:
        return True
    else:
        return False

class componentInfo:
    def __init__(self, block=True, mcm=False,soft=None, keep_trying=False):
        self.mcm = mcm
        self.soft = soft
        self.block = block
        self.status ={
            'reqmgr' : False,
            'mcm' : False,
            'dbs' : False,
            'phedex' : False
            }
        self.code = 0
        self.keep_trying = keep_trying

    def check(self):
        while True:
            try:
                print "checking reqmgr"
                if 'testbed' in reqmgr_url:
                    wfi = workflowInfo(reqmgr_url,'sryu_B2G-Summer12DR53X-00743_v4_v2_150126_223017_1156')
                else:
                    wfi = workflowInfo(reqmgr_url,'pdmvserv_task_B2G-RunIIWinter15wmLHE-00067__v1_T_150505_082426_497')
                    
                self.status['reqmgr'] = True
                break
            except Exception as e:
                self.tell('reqmgr')
                if self.keep_trying:
                    time.sleep(30)
                    continue
                import traceback
                print traceback.format_exc()
                print reqmgr_url,"unreachable"
                print str(e)
                if self.block and not (self.soft and 'reqmgr' in self.soft):
                    self.code = 123
                    return False
                break

        from McMClient import McMClient

        if self.mcm:
            while True:
                try:
                    mcmC = McMClient(dev=False)
                    print "checking mcm"
                    test = mcmC.getA('requests',page=0)
                    time.sleep(1)
                    if not test:
                        raise Exception("mcm is corrupted")
                    else:
                        self.status['mcm'] = True
                        break
                except Exception as e:
                    self.tell('mcm')
                    if self.keep_trying:
                        time.sleep(30)
                        continue
                    print "mcm unreachable"
                    print str(e)
                    if self.block and not (self.soft and 'mcm' in self.soft):
                        self.code = 125
                        return False
                    break
        while True:
            try:
                print "checking dbs"
                dbsapi = DbsApi(url=dbs_url)
                if 'testbed' in dbs_url:
                    blocks = dbsapi.listBlockSummaries( dataset = '/QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph/Summer12pLHE-DMWM_Validation_DONOTDELETE_Alan_TEST-v1/GEN', detail=True)
                else:
                    blocks = dbsapi.listBlockSummaries( dataset = '/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM', detail=True)
                    if not blocks:
                        raise Exception("dbs corrupted")
                    else:
                        self.status['dbs'] = True
                        break
            except Exception as e:
                self.tell('dbs')
                if self.keep_trying:
                    time.sleep(30)
                    continue
                print "dbs unreachable"
                print str(e)
                if self.block and not (self.soft and 'dbs' in self.soft):
                    self.code = 127
                    return False
                break
        while True:
            try:
                print "checking phedex"
                if 'testbed' in dbs_url:
                    cust = findCustodialLocation(phedex_url,'/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM')
                else:
                    cust = findCustodialLocation(phedex_url,'/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM')
                    self.status['phedex'] = True
                    break
            except Exception as e:
                self.tell('phedex')
                if self.keep_trying:
                    time.sleep(30)
                    continue
                print "phedex unreachable"
                print str(e)
                if self.block and not (self.soft and 'phedex' in self.soft):
                    self.code = 128
                    return False
                break

        print json.dumps( self.status, indent=2)
        return True

    def tell(self, c):
        sendLog('componentInfo',"The %s component is unreachable."% c, level='critical')
        #sendEmail("%s Component Down"%c,"The component is down, just annoying you with this","vlimant@cern.ch",['vlimant@cern.ch','matteoc@fnal.gov'])

class campaignInfo:
    def __init__(self):
        #this contains accessor to aggreed campaigns, with maybe specific parameters
        self.campaigns = json.loads(open('%s/WmAgentScripts/campaigns.json'%base_dir).read())
        #SI = siteInfo()
        SI = global_SI()
        for c in self.campaigns:
            if 'parameters' in self.campaigns[c]:
                if 'SiteBlacklist' in self.campaigns[c]['parameters']:
                    for black in copy.deepcopy(self.campaigns[c]['parameters']['SiteBlacklist']):
                        if black.endswith('*'):
                            self.campaigns[c]['parameters']['SiteBlacklist'].remove( black )
                            reg = black[0:-1]
                            self.campaigns[c]['parameters']['SiteBlacklist'].extend( [site for site in (SI.all_sites) if site.startswith(reg)] )
                            #print self.campaigns[c]['parameters']['SiteBlacklist']
                            
    def go(self, c, s=None):
        if c in self.campaigns and self.campaigns[c]['go']:
            if 'labels' in self.campaigns[c]:
                if s!=None:
                    return (s in self.campaigns[c]['labels']) or any([l in s for l in self.campaigns[c]['labels']])
                else:
                    print "Not allowed to go for",c,s
                    return False
            else:
                return True
        else:
            print "Not allowed to go for",c
            return False
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

def notRunningBefore( component, time_out = 60*5 ):
    s = 10
    while True:
        if time_out<0:
            print "Could not wait any longer for %s to finish"% component
            return False
        process_check = filter(None,os.popen('ps -f -e | grep %s.py | grep -v grep  |grep python'%component).read().split('\n'))
        if len(process_check):
            ## there is still the component running. wait
            time.sleep(s)
            time_out-=s
            continue
        break
    return True


def duplicateLock(component=None, silent=False):
    if not component:
        ## get the caller
        component = sys._getframe(1).f_code.co_name

    ## check that no other instances of assignor is running
    process_check = filter(None,os.popen('ps -f -e | grep %s.py | grep -v grep  |grep python'%component).read().split('\n'))
    if len(process_check)>1:
        ## another component is running on the machine : stop
        if not silent:
            sendEmail('overlapping %s'%component,'There are %s instances running %s'%(len(process_check), '\n'.join(process_check)))
            print "quitting because of overlapping processes"
        return True
    return False

    
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

def genericGet( base, url, load=True, headers=None):
    if not headers: headers={"Accept":"*/*"}
    conn  =  httplib.HTTPSConnection( base,
                                      #cert_file = os.getenv('X509_USER_PROXY'),
                                      cert_file = '/data/certs/servicecert.pem',
                                      #key_file = os.getenv('X509_USER_PROXY'),
                                      key_file = '/data/certs/servicekey.pem',
                                      )
    r1=conn.request("GET",url, headers =headers)
    r2=conn.getresponse()
    if load:
        result = json.loads(r2.read())    
    else:
        result = r2.read() 
    return result

class docCache:
    def __init__(self):
        self.cache = {}
        def default_expiration():
            ## a random time between 20 min and 30 min.
            return 20*60+random.random()*10*60
        self.cache['ssb_106'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl -s --retry 5 "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=106&batch=1&lastdata=1"').read())['csvdata'],
            'cachefile' : None,
            'default' : []
            }
        self.cache['ssb_107'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl -s --retry 5 "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=107&batch=1&lastdata=1"').read())['csvdata'],
            'cachefile' : None,
            'default' : []
            }
        self.cache['ssb_108'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl -s --retry 5 "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=108&batch=1&lastdata=1"').read())['csvdata'],
            'cachefile' : None,
            'default' : []
            }
        self.cache['ssb_109'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl -s --retry 5 "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=109&batch=1&lastdata=1"').read())['csvdata'],
            'cachefile' : None,
            'default' : []
            }
        self.cache['ssb_136'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl -s --retry 5 "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=136&batch=1&lastdata=1"').read())['csvdata'],
            'cachefile' : None,
            'default' : []
            }
        self.cache['ssb_158'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl -s --retry 5 "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=158&batch=1&lastdata=1"').read())['csvdata'],
            'cachefile' : None,
            'default' : []
            }
        self.cache['ssb_237'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl -s --retry 5 "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=237&batch=1&lastdata=1"').read())['csvdata'],
            'cachefile' : None,
            'default' : []
            }
        self.cache['ssb_159'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl -s --retry 5 "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=159&batch=1&lastdata=1"').read())['csvdata'],
            'cachefile' : None,
            'default' : []
            }
        self.cache['ssb_160'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl -s --retry 5 "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=160&batch=1&lastdata=1"').read())['csvdata'],
            'cachefile' : None,
            'default' : []
            }
        self.cache['gwmsmon_totals'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl --retry 5 -s http://cms-gwmsmon.cern.ch/poolview/json/totals').read()),
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
            'getter' : lambda : json.loads(os.popen('curl --retry 5 -s http://cms-gwmsmon.cern.ch/prodview//json/site_summary').read()),
            'cachefile' : None,
            'default' : {}
            }
        self.cache['gwmsmon_site_summary' ] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl --retry 5 -s http://cms-gwmsmon.cern.ch/totalview//json/site_summary').read()),
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
        self.cache['T1_DE_KIT_MSS_usage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodeUsage(phedex_url,'T1_DE_KIT_MSS'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['T1_US_FNAL_MSS_usage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodeUsage(phedex_url,'T1_US_FNAL_MSS'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['T1_ES_PIC_MSS_usage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodeUsage(phedex_url,'T1_ES_PIC_MSS'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['T1_UK_RAL_MSS_usage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodeUsage(phedex_url,'T1_UK_RAL_MSS'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['T1_IT_CNAF_MSS_usage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodeUsage(phedex_url,'T1_IT_CNAF_MSS'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['T1_FR_CCIN2P3_MSS_usage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodeUsage(phedex_url,'T1_FR_CCIN2P3_MSS'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['T1_RU_JINR_MSS_usage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodeUsage(phedex_url,'T1_RU_JINR_MSS'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['T0_CH_CERN_MSS_usage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodeUsage(phedex_url,'T0_CH_CERN_MSS'),
            'cachefile' : None,
            'default' : ""
            }

        self.cache['stuck_cat1'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads( os.popen('curl -s --retry 5 https://test-cmstransfererrors.web.cern.ch/test-CMSTransferErrors/stuck_1.json').read()),
            'cachefile' : None,
            'default' : {}
            }
        for cat in ['1','2','m1','m3','m4','m5','m6']:
            self.cache['stuck_cat%s'%cat] = {
                'data' : None,
                'timestamp' : time.mktime( time.gmtime()),
                'expiration' : default_expiration(),
                'getter' : lambda : json.loads( os.popen('curl -s --retry 5 https://test-cmstransfererrors.web.cern.ch/test-CMSTransferErrors/stuck_%s.json'%cat).read()),
                'cachefile' : None,
                'default' : {}
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

        self.cache['mss_usage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads( os.popen('curl -s --retry 5 http://cmsmonitoring.web.cern.ch/cmsmonitoring/storageoverview/latest/StorageOverview.json').read()),
            'cachefile' : None,
            'default' : {}
            }
        self.cache['hlt_cloud'] = { 
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads( os.popen('curl -s --retry 1 --connect-timeout 5 http://137.138.184.204/cache-manager/images/cloudStatus.json').read()),
            'cachefile' : None,
            'default' : {}
            }
        #create the cache files from the labels
        for src in self.cache:
            self.cache[src]['cachefile'] = '.'+src+'.cache.json'
            

    def get(self, label, fresh=False):
        now = time.mktime( time.gmtime())
        if label in self.cache:
            cache = self.cache[label]
            get_back = False
            try:
                if not cache['data']:
                    #check the file version
                    if os.path.isfile(cache['cachefile']):
                        try:
                            print "load",label,"from file",cache['cachefile']
                            f_cache = json.loads(open(cache['cachefile']).read())
                            cache['data' ] = f_cache['data']
                            cache['timestamp' ] = f_cache['timestamp']
                        except Exception as e:
                            print "Failed to read local cache"
                            print str(e)
                            get_back = True
                    else: get_back = True        
                    if get_back:
                        print "no file cache for", label,"getting fresh"
                        cache['data'] = cache['getter']()
                        cache['timestamp'] = now
                        open(cache['cachefile'],'w').write( json.dumps({'data': cache['data'], 'timestamp' : cache['timestamp']}, indent=2) )
                    
                ## check the time stamp
                if cache['expiration']+cache['timestamp'] < now or fresh:
                    print "getting fresh",label
                    cache['data'] = cache['getter']()
                    cache['timestamp'] = now
                    open(cache['cachefile'],'w').write( json.dumps({'data': cache['data'], 'timestamp' : cache['timestamp']}, indent=2) )

                return cache['data']
            except Exception as e: 
                sendLog('doccache','Failed to get %s\n%s'%(label,str(e)), level='critical')
                print "failed to get",label
                print str(e)
                if os.path.isfile(cache['cachefile']):
                    print "load",label,"from file",cache['cachefile']
                    f_cache = json.loads(open(cache['cachefile']).read())
                    cache['data' ] = f_cache['data']
                    cache['timestamp' ] = f_cache['timestamp']
                    return cache['data']
                else:
                    return copy.deepcopy(cache['default'])

def getNodes(url, kind):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/nodes')
    r2=conn.getresponse()
    result = json.loads(r2.read())
    return [node['name'] for node in result['phedex']['node'] if node['kind']==kind]

def getNodeUsage(url, node):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/nodeusage?node=%s'%node)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    if len(result['phedex']['node']):
        s= max([sum([node[k] for k in node.keys() if k.endswith('_node_bytes')]) for node in result['phedex']['node']])
        return int(s / 1023.**4) #in TB
    else:
        return None

dataCache = docCache()

def getAllStuckDataset():
    all_stuck = set()
    for cat in ['1','2','m1','m3','m4','m5','m6']:
        info = dataCache.get('stuck_cat%s'%cat)
        all_stuck.update( info.keys() )
    return all_stuck


class DSS:
    def __init__(self):
        try:
            self.bdb = json.loads(open('bdss.json').read())
        except:
            print "no bank of dataset size. starting fresh"
            self.bdb = {}

    def _get(self, dataset ):
        if not dataset in self.bdb:
            print "fetching info of",dataset
            self.bdb[dataset] = getDatasetBlockSize( dataset )

    def get(self, dataset ):
        return self.get_size( dataset )

    def get_size(self, dataset):
        self._get( dataset )
        return sum( self.bdb[dataset].values() )

    def get_block_size(self, dataset):
        self._get( dataset )
        return sum( self.bdb[dataset].values() ), copy.deepcopy( self.bdb[dataset] )

    def __del__(self):
        ## pop when too many entries
        if len(self.bdb) > 1000:
            keys = self.bdb.keys()
            random.shuffle(keys)
            for k in keys[:1000]:
                self.bdb.pop(k)
        try:
            open('bdss.json','w').write( json.dumps( self.bdb ))
        except:
            print "no access to bdss.json"
                                    


class siteInfo:
    def __init__(self):
        
        UC = unifiedConfiguration()

        self.sites_ready_in_agent = set()

        try:
            agents = getAllAgents( reqmgr_url )
            for team,agents in agents.items():
                #print team
                if team !='production': continue
                for agent in agents:
                    if agent['status'] != 'ok': 
                        print agent['status']
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

            #data = dataCache.get('ssb_158') ## 158 is the site readyness metric
            data = dataCache.get('ssb_237') ## 237 is the site readyness metric
            for siteInfo in data:
                #print siteInfo['Status']
                if not siteInfo['Tier'] in [0,1,2]: continue ## ban de-facto all T3
                self.all_sites.append( siteInfo['VOName'] )
                if siteInfo['VOName'] in self.sites_banned: continue
                if self.sites_ready_in_agent and siteInfo['VOName'] in self.sites_ready_in_agent:
                    self.sites_ready.append( siteInfo['VOName'] )
                elif self.sites_ready_in_agent and not siteInfo['VOName'] in self.sites_ready_in_agent:
                    self.sites_not_ready.append( siteInfo['VOName'] )
                elif siteInfo['Status'] == 'enabled': 
                    self.sites_ready.append( siteInfo['VOName'] )
                else:
                    self.sites_not_ready.append( siteInfo['VOName'] )

            
        except Exception as e:
            print "issue with getting SSB readiness"
            print str(e)
            sendEmail('bad sites configuration','falling to get any sites')
            sys.exit(-9)



        self.sites_auto_approve = UC.get('sites_auto_approve')

        self.sites_eos = [ s for s in self.sites_ready if s in ['T2_CH_CERN','T2_CH_CERN_AI','T2_CH_CERN_T0','T2_CH_CERN_HLT'] ]
        self.sites_T3s = [ s for s in self.sites_ready if s.startswith('T3_')]
        self.sites_T2s = [ s for s in self.sites_ready if s.startswith('T2_')]
        self.sites_T1s = [ s for s in self.sites_ready if (s.startswith('T1_') or s.startswith('T0_'))] ## put the T0 in the T1 : who cares
        self.sites_AAA = list(set(self.sites_ready) - set(['T2_CH_CERN_HLT']))
        ## could this be an SSB metric ?
        self.sites_with_goodIO = UC.get('sites_with_goodIO')
        #restrict to those that are actually ON
        self.sites_with_goodIO = [s for s in self.sites_with_goodIO if s in self.sites_ready]
        ## those of the above that can be actively targetted for transfers
        allowed_T2_for_transfer = ["T2_DE_RWTH","T2_DE_DESY", 
                                          #not inquired# "T2_ES_CIEMAT",
                                          #no space# ##"T2_FR_GRIF_IRFU", #not inquired# ##"T2_FR_GRIF_LLR", #not inquired"## "T2_FR_IPHC",##not inquired"## "T2_FR_CCIN2P3",
                                          "T2_IT_Legnaro", "T2_IT_Pisa", "T2_IT_Rome", "T2_IT_Bari",
                                          "T2_UK_London_Brunel", "T2_UK_London_IC", "T2_UK_SGrid_RALPP",
                                          "T2_US_Nebraska","T2_US_Wisconsin","T2_US_Purdue","T2_US_Caltech", "T2_US_Florida", "T2_US_UCSD", "T2_US_MIT",
                                          "T2_BE_IIHE",
                                          "T2_EE_Estonia",
                                          "T2_CH_CERN", 

                                   'T2_RU_INR',
                                   'T2_UA_KIPT'
                                          ]
        # restrict to those actually ON
        allowed_T2_for_transfer = [s for s in allowed_T2_for_transfer if s in self.sites_ready]

        ## first round of determining the sites that veto transfer
        self.sites_veto_transfer = [site for site in self.sites_with_goodIO if not site in allowed_T2_for_transfer]

        self.storage = defaultdict(int)
        self.disk = defaultdict(int)
        self.free_disk = defaultdict(int)
        self.quota = defaultdict(int)
        self.locked = defaultdict(int)
        self.cpu_pledges = defaultdict(int)
        self.addHocStorage = {
            'T2_CH_CERN_T0': 'T2_CH_CERN',
            'T2_CH_CERN_HLT' : 'T2_CH_CERN',
            'T2_CH_CERN_AI' : 'T2_CH_CERN'
            }
        ## list here the site which can accomodate high memory requests
        self.sites_memory = {}

        self.sites_mcore_ready = []
        mcore_mask = dataCache.get('mcore_ready')
        if mcore_mask:
            self.sites_mcore_ready = [s for s in mcore_mask['sites_for_mcore'] if s in self.sites_ready]
        else:
            #sendEmail("no mcore sites","that is suspicious!")
            pass

        for s in self.all_sites:
            ## will get it later from SSB
            self.cpu_pledges[s]=1
            ## will get is later from SSB
            self.disk[ self.CE_to_SE(s)]=0
            #if s == 'T0_CH_CERN':
            #    self.disk[ self.CE_to_SE(s)]=200 ## temporary override

        tapes = getNodes(phedex_url, 'MSS')
        for mss in tapes:
            if mss in self.sites_banned: continue # not using these tapes for MC familly
            self.storage[mss] = 0

        ## and get SSB sync
        self.fetch_ssb_info(talk=False)
        

        mss_usage = dataCache.get('mss_usage')
        sites_space_override = UC.get('sites_space_override')
        use_field = 'Usable'
        for mss in self.storage:
            #used = dataCache.get(mss+'_usage')
            #print mss,'used',used
            #if used == None: self.storage[mss] = 0
            #else:  self.storage[mss] = max(0, self.storage[mss]-used)
            if not mss in mss_usage['Tape'][use_field]: 
                self.storage[mss] = 0 
            else: 
                self.storage[mss]  = mss_usage['Tape'][use_field][mss]

            if mss in sites_space_override:
                self.storage[mss] = sites_space_override[mss]


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

    def usage(self,site):
        try:
            info = json.loads( os.popen('curl -s "http://dashb-cms-job.cern.ch/dashboard/request.py/jobsummary-plot-or-table2?site=%s&check=submitted&sortby=activity&prettyprint"' % site ).read() )
            return info
        except:
            return {}
        
    def availableSlots(self, sites=None):
        s=0
        for site in self.cpu_pledges:
            if sites and not site in sites: continue
            s+=self.cpu_pledges[site]
        return s

    def getRemainingDatasets(self, site):
        start_reading=False
        datasets=[]
        s=0
        for line in os.popen('curl -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/%s/RemainingDatasets.txt'%site).read().split("\n"):
            if 'DDM Partition: DataOps' in line:
                start_reading=True
                continue

            if start_reading:
                splt=line.split()
                if len(splt)==5 and splt[-1].count('/')==3:
                    (_,size,_,_,dataset) = splt
                    size= float(size)
                    #print dataset
                    s+=size
                    datasets.append( (size,dataset) )
            if start_reading and 'DDM Partition' in line:
                break

        #print s
        return datasets

    def fetch_glidein_info(self, talk=True):
        self.sites_memory = dataCache.get('gwmsmon_totals')
        for site in self.sites_memory.keys():
            if not site in self.sites_ready:
                self.sites_memory.pop( site )
        
        for_max_running = dataCache.get('gwmsmon_site_summary')
        for site in self.cpu_pledges:
            if not site in for_max_running: continue
            #new_max = int(for_max_running[site]['MaxWasRunning'] * 0.70) ## put a fudge factor
            new_max = int(for_max_running[site]['MaxWasCpusUse'] * 0.70) ## put a fudge factor
            if new_max > self.cpu_pledges[site]:
                #print "could raise",site,"from",self.cpu_pledges[site],"to",for_max_running[site]['MaxWasRunning']
                #print "raising",site,"from",self.cpu_pledges[site],"to",new_max
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
    
    def sites_low_pressure(self, ratio):
        sites = [site for site,(matching,running,_) in self.sites_pressure.items() if (running==0 or (matching/float(running))< ratio) and site in self.sites_ready]
        return sites

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

    def restrictByMemory( self, maxMem, allowed_sites):
        allowed = self.sitesByMemory(maxMem)
        if allowed!=None:
            return list(set(allowed_sites) & set(allowed))
        return allowed_sites

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

            ## consider quota to be 80% of what's available
            available = int(float(quota)*buffer_level) - int(locked)

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
            'PledgeTape' : 107,
            'realCPU' : 136,
            'prodCPU' : 159,
            'CPUbound' : 160,
            'FreeDisk' : 106,
            #'UsedTape' : 108,
            #'FreeTape' : 109
            }
        
        all_data = {}
        for name,column in columns.items():
            if talk: print name,column
            try:
                #data = json.loads(os.popen('curl -s "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=%s&batch=1&lastdata=1"'%column).read())
                #all_data[name] =  data['csvdata']
                all_data[name] =  dataCache.get('ssb_%d'% column) #data['csvdata']
            except:
                print "cannot get info from ssb for",name
        _info_by_site = {}
        for info in all_data:
            for item in all_data[info]:
                site = item['VOName']
                if site.startswith('T3'): continue
                value = item['Value']
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

            if 'FreeDisk' in info and info['FreeDisk']:
                if site in self.disk:
                    if self.disk[site] < info['FreeDisk']:
                        if talk: print site,"could use",info['FreeDisk'],"instead of",self.disk[site],"for disk"
                        self.disk[site] = int(info['FreeDisk'])
                else:
                    if not ssite in self.disk:
                        if talk: print "setting",info['FreeDisk']," disk for",ssite
                        self.disk[ssite] = int(info['FreeDisk'])

            if 'FreeDisk' in info and site!=ssite and info['FreeDisk']:
                if ssite in self.disk:
                    if self.disk[ssite] < info['FreeDisk']:
                        if talk: print ssite,"could use",info['FreeDisk'],"instead of",self.disk[ssite],"for disk"
                        self.disk[ssite] = int(info['FreeDisk'])
                else:
                    if talk: print "setting",info['FreeDisk']," disk for",ssite
                    self.disk[ssite] = int(info['FreeDisk'])

            #if 'FreeTape' in info and 'UsedTape' in info and tsite in self.storage and info['FreeTape']:
            #    if info['UsedTape'] and self.storage[tsite] < info['FreeTape']:
            #        if talk: print tsite,"could use",info['FreeTape'],"instead of",self.storage[tsite],"for tape"
            #        self.storage[tsite] = int(info['FreeTape'])
            #if 'PledgeTape' in info and tsite in self.storage:
            #     self.storage[tsite] = int(info['PledgeTape'])


    def types(self):
        return ['sites_with_goodIO','sites_T1s','sites_T2s','sites_mcore_ready']#,'sites_veto_transfer']#,'sites_auto_approve']

    def CE_to_SE(self, ce):
        if (ce.startswith('T1') or ce.startswith('T0')) and not ce.endswith('_Disk'):
            return ce+'_Disk'
        else:

            if ce in self.addHocStorage:
                return self.addHocStorage[ce]
            else:
                return ce

    def SE_to_CE(self, se):
        if se.endswith('_Disk'):
            return se.replace('_Disk','')
        elif se.endswith('_MSS'):
            return se.replace('_MSS','')
        else:
            return se

    def pick_SE(self, sites=None, size=None): ## size needs to be in TB
        if size:
            return self._pick(sites, dict([(se,free) for (se,free) in self.storage.items() if free>size]))
        else:
            return self._pick(sites, self.storage)

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
        rnd = random.random() * sum(ws)
        #print ws
        for i, w in enumerate(ws):
            rnd -= w
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

def global_SI():
    if not global_SI.instance:
        global_SI.instance = siteInfo()
    return global_SI.instance
global_SI.instance = None



class closeoutInfo:
    def __init__(self):
        try:
            self.record = json.loads(open('closedout.json').read())
        except:
            print "No closed-out record, starting fresh"
            self.record = {}

    def table_header(self):
        text = '<table border=1><thead><tr><th>workflow</th><th>OutputDataSet</th><th>%Compl</th><th>acdc</th><th>Dupl</th><th>LSsize</th><th>Scubscr</th><th>dbsF</th><th>dbsIF</th><th>\
phdF</th><th>Updated</th><th>Priority</th></tr></thead>'
        return text

    def one_line(self, wf, wfo, count):
        if count%2:            color='lightblue'
        else:            color='white'
        text=""
        tpid = self.record[wf]['prepid']
        pid = tpid.replace('task_','')

        ## return the corresponding html
        order = ['percentage','acdc','duplicate','correctLumis','missingSubs','dbsFiles','dbsInvFiles','phedexFiles','updated']
        wf_and_anchor = '<a id="%s">%s</a>'%(wf,wf)
        for out in self.record[wf]['datasets']:
            text += '<tr bgcolor=%s>'%color
            text += '<td>%s<br>'%wf_and_anchor
            text += '<a href="https://%s/reqmgr2/fetch?rid=%s" target="_blank">dts</a>'%(reqmgr_url, wf)
            text += ', <a href="https://%s/reqmgr2/data/request/%s" target="_blank">wfc</a>'%(reqmgr_url, wf)
            text += ', <a href="https://cms-logbook.cern.ch/elog/Workflow+processing/?mode=full&reverse=0&reverse=1&npp=20&subtext=%s&sall=q" target="_blank">elog</a>'%(pid)
            text += ', <a href=https://%s/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s>perf</a>'%(reqmgr_url, wf)
            text += ', <a href=assistance.html#%s>%s</a>'%(wf,wfo.status)
            text += '<br>'
            text += '<a href="http://dabercro.web.cern.ch/dabercro/unified/showlog/?search=%s" target="_blank">history</a>'%(pid)
            text += ', <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s>%s</a>'%(tpid,tpid)
            text += ', <a href=report/%s>report</a>'%(wf)
            if 'ReReco' in tpid:
                text += ', <a href=https://cmst2.web.cern.ch/cmst2/unified/datalumi/lumi.%s.html>lumis</a>'%(tpid)
            text += ', <a href="https://its.cern.ch/jira/issues/?jql=text~%s AND project = CMSCOMPPR" target="_blank">jira</a>'%(pid)
            text += '</td>'
            
            text+='<td>%s</td>'% out
            lines = []
            for f in order:
                if f in self.record[wf]['datasets'][out]:
                    value = self.record[wf]['datasets'][out][f]
                else:
                    value = "-NA-"
                if f =='acdc':
                    text+='<td><a href=https://%s/reqmgr2/data/request?prep_id=%s&detail=false>%s</a></td>'%(reqmgr_url, tpid , value)
                else:
                    text+='<td>%s</td>'% value
            #text+='<td>%s</td>'%self.record[wf]['closeOutWorkflow']
            text+='<td>%s</td>'%self.record[wf]['priority']
            text+='</tr>'
            wf_and_anchor = wf

        return text

    def html(self):
        self.summary()
        self.assistance()

    def summary(self):
        os.system('cp closedout.json closedout.json.last')
        
        html = open('%s/closeout.html'%monitor_dir,'w')
        html.write('<html>')
        html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/ target=_blank> logs</a> <br><br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))

        html.write( self.table_header() )

        from assignSession import session, Workflow
        for (count,wf) in enumerate(sorted(self.record.keys())):
            wfo = session.query(Workflow).filter(Workflow.name == wf).first()
            if not wfo: continue
            if not (wfo.status == 'away' or wfo.status.startswith('assistance')):
                print "Taking",wf,"out of the close-out record"
                self.record.pop(wf)
                continue
            html.write( self.one_line( wf, wfo , count) )

        html.write('</table>')
        html.write('<br>'*100) ## so that the anchor works ok
        html.write('bottom of page</html>')

        open('closedout.json','w').write( json.dumps( self.record , indent=2 ) )

    def assistance(self):
        from assignSession import session, Workflow
        wfs = session.query(Workflow).filter(Workflow.status.startswith('assistance')).all()

        short_html = open('%s/assistance_summary.html'%monitor_dir,'w')
        html = open('%s/assistance.html'%monitor_dir,'w')
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
    
        #short_html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/last.log target=_blank> log</a> <a href=logs/recoveror/last.log target=_blank> postlog</a> <br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))
        #html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/last.log target=_blank> log</a> <a href=logs/recoveror/last.log target=_blank> postlog</a><br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))
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
<li> <b>recovered</b> : there is at least one inactive ACDC for the workflow <font color=green>(Automatic)</font></li>
<li> <b>recovery</b> : the final statistics of the sample is not passing the requirements <font color=green>(Automatic)</font> </li>
<li> <b>over100</b> : the final statistics is over 100%% <font color=red>(Operator)</font></li>
<li> <b>biglumi</b> : the maximum size of the lumisection in one of the output has been exceeded <font color=red>(Operator)</font></li>
<li> <b>filemismatch</b> : there is a mismatch in the number of files in DBS and Phedex <font color=red>(Operator)</font></li>
<li> <b>duplicates</b> : duplicated lumisection have been found and need to be invalidated <font color=red>(Operator)</font></li>
<li> <b>manual</b> : no automatic recovery was possible <font color=red>(Operator)</font></li>
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
            lines = []
            short_lines = []
            prio_ordered_wf = []
            for (count,wfo) in enumerate(assist[status]):
                if not wfo.name in self.record:
                    continue
                prio = self.record[wfo.name]['priority']
                prio_ordered_wf.append( (prio, wfo) )
            ## sort by priority
            prio_ordered_wf.sort( key = lambda item:item[0] ,reverse=True )
            ## and use only the wfo
            prio_ordered_wf = [ item[1] for item in prio_ordered_wf ]
            for (count,wfo) in enumerate(prio_ordered_wf):
                if count%2:            color='lightblue'
                else:            color='white'
                if not wfo.name in self.record: 
                    print "wtf with",wfo.name
                    #html.write( self.no_record( wfo.name, wfo, count))
                    continue     
                prio = self.record[wfo.name]['priority']
                #lines.append( ( prio ,self.one_line( wfo.name, wfo, count) ) )
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

                    #short_lines.append(line)
                    short_html.write(line)


            html.write("</table><br><br>")
            short_html.write("</table><br><br>")

        short_html.write("<br>"*100)
        short_html.write("bottom of page</html>")    
        html.write("<br>"*100)
        html.write("bottom of page</html>")    



        

def getSiteWhiteList( inputs , pickone=False):
    print "deprecated"
    sys.exit(-1)
    return []
    SI = global_SI()
    (lheinput,primary,parent,secondary) = inputs
    sites_allowed=[]
    if lheinput:
        sites_allowed = ['T2_CH_CERN'] ## and that's it
    elif secondary:
        sites_allowed = list(set(SI.sites_T1s + SI.sites_with_goodIO))
    elif primary:
        sites_allowed =list(set( SI.sites_T1s + SI.sites_T2s ))
    else:
        # no input at all
        sites_allowed =list(set( SI.sites_T2s + SI.sites_T1s))

    if pickone:
        sites_allowed = [SI.pick_CE( sites_allowed )]

    return sites_allowed

def reduceSiteWhiteList( sites_allowed, CI, SI):
    c_sites_allowed = CI.get(wfh.request['Campaign'], 'SiteWhitelist' , [])


def checkTransferApproval(url, phedexid):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?request='+str(phedexid))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['request']
    approved={}
    for item in items:
        for node in item['node']:
            approved[node['name']] = (node['decision']=='approved')
    return approved

def getDatasetFileFraction( dataset, files):
    dbsapi = DbsApi(url=dbs_url) 
    all_files = dbsapi.listFileArray( dataset= dataset,validFileOnly=1, detail=True)
    total = 0
    in_file = 0
    for f in all_files:
        total += f['event_count']
        if f['logical_file_name'] in files:
            in_file += f['event_count']
    fract=0
    if total:
        fract=float(in_file)/float(total)
    return fract, total, in_file
    

def getDatasetBlockFraction( dataset, blocks):
    dbsapi = DbsApi(url=dbs_url) 
    all_blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    total=0
    in_block=0
    for block in all_blocks:
        total += block['num_event']
        if block['block_name'] in blocks:
            in_block += block['num_event']

    fract=0
    if total:
        fract=float(in_block)/float(total)
    return fract, total, in_block
    
def findLateFiles(url, datasetname, going_to=None):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    u = '/phedex/datasvc/json/prod/filelatency?dataset=%s'% datasetname
    if going_to:
        u += '&to_node=%s'%going_to
    r1=conn.request("GET",u)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    ret = []
    for block in result['phedex']['block']:
        for destination in block['destination']:
            #if going_to and destination['name'] != going_to: continue
            for latency in destination['blocklatency']:
                for flatency in latency['filelatency']:
                    if not flatency['time_at_destination']:
                        print flatency['lfn'],"is not coming out from",flatency["from_node"],"to",destination['name'],"since", time.asctime(time.gmtime(flatency['time_first_attempt'])),"since last",time.asctime(time.gmtime(flatency['time_latest_attempt'])),"after",flatency['attempts'],"attempts"
                        ret.append({
                                'file' : flatency['lfn'],
                                'from' : flatency["from_node"],
                                'to' : destination['name'],
                                'since' : time.asctime(time.gmtime(flatency['time_first_attempt'])),
                                'sincetime' : flatency['time_first_attempt'],
                                'last' : time.asctime(time.gmtime(flatency['time_latest_attempt'])),
                                'lasttime' : flatency['time_latest_attempt'],
                                'delay' : flatency['time_latest_attempt']-flatency['time_first_attempt'],
                                'retries' : flatency['attempts']
                                })
                        if flatency['time_on_buffer']:
                            print "\t is on buffer, but not on tape"
    return ret

def findLostBlocks(url, datasetname):
    blocks,_ = findLostBlocksFiles(url , datasetname)
    return blocks

def findLostBlocksFiles(url, datasetname):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

    r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?block=%s%%23*&collapse=n'% datasetname)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    lost = []
    lost_files = []
    for dataset in result['phedex']['dataset']:
        for item in dataset['block']:
            exist=0
            for loc in item['subscription']:
                exist = max(exist, loc['percent_bytes'])
            if not exist:
                #print "We have lost:",item['name']
                #print json.dumps( item, indent=2)
                lost.append( item )
            elif exist !=100:
                print "we have lost files on",item['name']
                ## go deeper then
                r1=conn.request("GET",'/phedex/datasvc/json/prod/filereplicas?block=%s'%(item['name'].replace('#','%23')))
                r2=conn.getresponse() 
                sub_result=json.loads(r2.read())
                for block in sub_result['phedex']['block']:
                    for ph_file in block['file']:
                        #print ph_file
                        if len(ph_file['replica'])==0:
                            #print ph_file['name'],'has no replica'
                            lost_files.append( ph_file )
    return lost,lost_files

def checkTransferLag( url, xfer_id , datasets=None):
    try:
        v = try_checkTransferLag( url, xfer_id , datasets)
    except:
        try:
            time.sleep(1)
            v = try_checkTransferLag( url, xfer_id , datasets)
        except Exception as e:
            print "fatal execption in checkTransferLag\n","%s\n%s\n%s"%(xfer_id,datasets,str(e))
            #sendEmail('fatal execption in checkTransferLag',"%s\n%s\n%s"%(xfer_id,datasets,str(e)))
            sendLog('checkTransferLag','fatal execption in checkTransferLag for %s\n%s\n%s'%(xfer_id,datasets,str(e)), level='critical')
            v = {}
    return v

def try_checkTransferLag( url, xfer_id , datasets=None):
    ## xfer_id tells us what has to go where via subscriptions
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?request=%s'%(str(xfer_id)))
    #r2=conn.getresponse()
    #result = json.loads(r2.read())
    #timestamp = result['phedex']['request_timestamp']
    r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?request='+str(xfer_id))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    timecreate=min([r['time_create'] for r in result['phedex']['request']])
    subs_url = '/phedex/datasvc/json/prod/subscriptions?request=%s&create_since=%d'%(str(xfer_id),timecreate)
    subs_url+='&collapse=n'
    r1=conn.request("GET",subs_url)
    r2=conn.getresponse()
    result = json.loads(r2.read())

    now = time.mktime( time.gmtime() )
    #print result
    stuck = defaultdict(lambda : defaultdict(lambda : defaultdict(tuple)))
    if len(result['phedex']['dataset'])==0:
        print "trying with an earlier date than",timecreate,"for",xfer_id
        subs_url = '/phedex/datasvc/json/prod/subscriptions?request=%s&create_since=%d'%(str(xfer_id),0)
        subs_url+='&collapse=n' 
        r1=conn.request("GET",subs_url)
        r2=conn.getresponse()
        result = json.loads(r2.read())

    for item in  result['phedex']['dataset']:
        if 'subscription' not in item:            
            #print "sub"
            loop_on = list(itertools.chain.from_iterable([[(subitem['name'],i) for i in subitem['subscription']] for subitem in item['block']]))
        else:
            #print "no sub"
            loop_on = [(item['name'],i) for i in item['subscription']]
        #print loop_on
        for item,sub in loop_on:
            if datasets and not any([item.startswith(ds) for ds in datasets]): continue
            if sub['percent_files']!=100:
                destination = sub['node']
                time_then = sub['time_create']
                delay = (now - time_then)/(60.*60.*24.)
                delay_s = (now - time_then)
                print "\n",item,"is not complete at",destination,"since", delay ,"[d]"
                
                if '#' in item:
                    item_url = '/phedex/datasvc/json/prod/blockreplicas?block=%s'%( item.replace('#','%23') )
                else:
                    item_url = '/phedex/datasvc/json/prod/blockreplicas?dataset=%s'%( item )
                r1=conn.request("GET",item_url)
                r2=conn.getresponse()    
                item_result = json.loads(r2.read()) 

                for subitem in item_result['phedex']['block']:
                    dones = [replica['node'] for replica in subitem['replica'] if replica['complete'] == 'y']

                    block_size = max([replica['bytes'] for replica in subitem['replica']])
                    ssitems = [replica['bytes'] for replica in subitem['replica'] if replica['node']==destination]
                    destination_size = min(ssitems) if ssitems else 0
                    block_size_GB = block_size / (1024.**3)
                    destination_size_GB = destination_size / (1024.**3)
                    ### rate
                    rate = destination_size_GB / delay_s
                    if destination in dones: continue
                    if not dones: 
                        print "\t\t",subitem['name'],"lost"
                        stuck[ds][subitem['name']][destination] = (block_size,destination_size,delay,rate,dones)
                        continue
                    if delay>0: # more than 7 days is way to much !!
                        print subitem['name'],' of size',block_size_GB,'[GB] missing',block_size_GB-destination_size_GB,'[GB]'
                        print '\tis complete at',dones
                        print "\tincoming at",rate,"[GB/s]"
                    if rate < 10.:
                        ds = item.split('#')[0]
                        stuck[ds][subitem['name']][destination] = (block_size,destination_size,delay,rate,dones)
                        
    return stuck

def checkTransferStatus(url, xfer_id, nocollapse=False):
    try:
        v = try_checkTransferStatus(url, xfer_id, nocollapse)
    except:
        try:
            time.sleep(1)
            v = try_checkTransferStatus(url, xfer_id, nocollapse)
        except Exception as e:
            print str(e)
            #sendEmail('fatal exception in checkTransferStatus %s'%xfer_id, str(e))
            sendLog('checkTransferStatus','fatal exception in checkTransferStatus %s\n%s'%(xfer_id, str(e)), level='critical')
            v = {}
    return v
        

def try_checkTransferStatus(url, xfer_id, nocollapse=False):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

    r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?request='+str(xfer_id))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    timecreate=min([r['time_create'] for r in result['phedex']['request']])
    subs_url = '/phedex/datasvc/json/prod/subscriptions?request=%s&create_since=%d'%(str(xfer_id),timecreate)
    if nocollapse:
        subs_url+='&collapse=n'
    #print subs_url
    r1=conn.request("GET",subs_url)
    r2=conn.getresponse()
    result = json.loads(r2.read())

    #print result
    completions={}
    if len(result['phedex']['dataset'])==0:
        print "trying with an earlier date than",timecreate,"for",xfer_id
        subs_url = '/phedex/datasvc/json/prod/subscriptions?request=%s&create_since=%d'%(str(xfer_id),0)
        if nocollapse:
            subs_url+='&collapse=n'
        r1=conn.request("GET",subs_url)
        r2=conn.getresponse()
        result = json.loads(r2.read())

    #print json.dumps( result['phedex']['dataset'] , indent=2)
    for item in  result['phedex']['dataset']:
        completions[item['name']]={}
        if 'subscription' not in item:            
            loop_on = list(itertools.chain.from_iterable([subitem['subscription'] for subitem in item['block']]))
        else:
            loop_on = item['subscription']
        for sub in loop_on:
            if not sub['node'] in completions[item['name']]:
                completions[item['name']][sub['node']] = []
            if sub['percent_bytes']:
                #print sub
                #print sub['node'],sub['percent_files']
                completions[item['name']] [sub['node']].append(float(sub['percent_files']))
            else:
                completions[item['name']] [sub['node']].append(0.)
        
    for item in completions:
        for site in completions[item]:
            ## average it !
            completions[item][site] = sum(completions[item][site]) / len(completions[item][site])

    #print result
    """
    completions={}
    ## that level of completion is worthless !!!!
    # falling back to dbs
    print result
    if not len(result['phedex']['dataset']):
        print "no data at all"
        print result
    for item in  result['phedex']['dataset']:
        dsname = item['name']
        #print dsname
        completions[dsname]={}
        presence = getDatasetPresence(url, dsname)
        for sub in item['subscription']:
            #print sub
            site = sub['node']
            if site in presence:
                completions[dsname][site] = presence[site][1]
            else:
                completions[dsname][site] = 0.
    """

    #print completions
    return completions

def findCustodialCompletion(url, dataset):
    return findCustodialLocation(url, dataset, True)

def findCustodialLocation(url, dataset, with_completion=False):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset=%s'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    request=result['phedex']
    if 'block' not in request.keys():
        return [],None
    if len(request['block'])==0:
        return [],None
    cust=[]
    #veto = ["T0_CH_CERN_MSS"]
    veto = []
    blocks = set()
    cust_blocks = set()
    for block in request['block']:
        blocks.add( block['name'] )
        for replica in block['replica']:
            #print replica
            ## find a replica, custodial and COMPLETED !
            if replica['custodial']=="y" and (not with_completion or replica['complete']=="y"):
                if (replica['node'] in veto):
                    #print replica['node']
                    pass
                else:
                    cust.append(replica['node'])
                    cust_blocks.add( block['name'] )



    more_information = {"checked" : time.mktime( time.gmtime())}
    ## make sure all known blocks are complete at custodial
    if with_completion and len(blocks)!=len(cust_blocks):
        #print blocks
        #print cust_blocks

        print "Missing",len(blocks - cust_blocks),"blocks out of",len(blocks)
        more_information['nblocks'] = len(blocks)
        #more_information['blocks'] = list(blocks)
        more_information['nmissing'] = len(blocks - cust_blocks)
        more_information['missing'] = list(blocks - cust_blocks)
        if len(cust_blocks)!=0:
            print json.dumps(list(blocks - cust_blocks), indent=2)
        r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset=%s&node=T*MSS'%dataset)
        r2=conn.getresponse()
        result = json.loads(r2.read())
        request=result['phedex']['request']
        more_information['nodes']={}
        for nodes in request:
            created = nodes['time_create']
            for node in nodes['node']:
                decided = node['time_decided']
                print "request",nodes['id'],"to",node['name'],"is",node['decision'],
                if decided:
                    print "on",time.asctime( time.gmtime( decided))
                more_information['nodes'][node['name']] = { 'id': nodes['id'], 'created' : created, 'decided' : decided}


                print ". Created since",time.asctime( time.gmtime( created ))

        return [],more_information
    else:
        return list(set(cust)), more_information

def getDatasetFileLocations(url, dataset):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/filereplicas?dataset=%s'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['block']
    locations = defaultdict(set)
    for block in items:
        for f in block['file']:
            for r in f['replica']:
                #if r['
                locations[ f['name'] ] .add( r['node'] )
    return dict( locations )

def getDatasetFiles(url, dataset ,without_invalid=True ):
    dbsapi = DbsApi(url=dbs_url)
    files = dbsapi.listFileArray( dataset= dataset,validFileOnly=without_invalid, detail=True)
    dbs_filenames = [f['logical_file_name'] for f in files]
    
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

    r1=conn.request("GET",'/phedex/datasvc/json/prod/filereplicas?dataset=%s'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['block']
    phedex_filenames = []
    for block in items:
        for f in block['file']:
            phedex_filenames.append(f['name'])
    
    return dbs_filenames, phedex_filenames, list(set(dbs_filenames) - set(phedex_filenames)), list(set(phedex_filenames)-set(dbs_filenames))

def getDatasetBlocksFraction(url, dataset, complete='y', group=None, vetoes=None, sites=None, only_blocks=None):
    try:
        r = try_getDatasetBlocksFraction(url, dataset, complete,group,vetoes,sites,only_blocks)
    except:
        try:
            r = try_getDatasetBlocksFraction(url, dataset, complete,group,vetoes,sites,only_blocks)
        except Exception as e:
            #print sendEmail("exception in getDatasetBlocksFraction",str(e))
            sendLog('getDatasetBlocksFraction',"exception in getDatasetBlocksFraction for %s \n %s"%( dataset, str(e)), level='critical')
            r = 0.
    return r

def try_getDatasetBlocksFraction(url, dataset, complete='y', group=None, vetoes=None, sites=None, only_blocks=None):
    ###count how manytimes a dataset is replicated < 100%: not all blocks > 100% several copies exis
    if vetoes==None:
        vetoes = ['MSS','Buffer','Export']

    dbsapi = DbsApi(url=dbs_url)
    #all_blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    #all_block_names=set([block['block_name'] for block in all_blocks])
    files = dbsapi.listFileArray( dataset= dataset,validFileOnly=1, detail=True)
    all_block_names = list(set([f['block_name'] for f in files]))
    if only_blocks:
        all_block_names = [b for b in all_block_names if b in only_blocks]

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

    r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset=%s'%(dataset))
    r2=conn.getresponse()
    #retry since it does not look like its getting the right info in the first shot
    #print "retry"
    #time.sleep(2)
    #r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset=%s'%(dataset))
    #r2=conn.getresponse()

    result = json.loads(r2.read())
    items=result['phedex']['block']

    block_counts = {}
    #initialize
    for block in all_block_names:
        block_counts[block] = 0
    
    for item in items:
        for replica in item['replica']:
            if not any(replica['node'].endswith(v) for v in vetoes):
                if replica['group'] == None: replica['group']=""
                if complete and not replica['complete']==complete: continue
                if group!=None and not replica['group'].lower()==group.lower(): continue 
                if sites and not replica['node'] in sites:
                    #print "leaving",replica['node'],"out"
                    continue
                b = item['name']
                if not b in all_block_names: continue
                block_counts[ b ] +=1
    
    if not len(block_counts):
        print "no blocks for",dataset
        return 0
    first_order = float(len(block_counts) - block_counts.values().count(0)) / float(len(block_counts))
    if first_order <1.:
        print dataset,":not all",len(block_counts)," blocks are available, only",len(block_counts)-block_counts.values().count(0)
        return first_order
    else:
        second_order = sum(block_counts.values())/ float(len(block_counts))
        print dataset,":all",len(block_counts),"available",second_order,"times"
        return second_order
    
def getDatasetDestinations( url, dataset, only_blocks=None, group=None, vetoes=None, within_sites=None, complement=True):
    if vetoes==None:
        vetoes = ['MSS','Buffer','Export']
    #print "presence of",dataset
    dbsapi = DbsApi(url=dbs_url)
    all_blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    all_dbs_block_names=set([block['block_name'] for block in all_blocks])
    all_block_names=set([block['block_name'] for block in all_blocks])
    if only_blocks:
        all_block_names = filter( lambda b : b in only_blocks, all_block_names)
        full_size = sum([block['file_size'] for block in all_blocks if (block['block_name'] in only_blocks)])
        #print all_block_names
        #print [block['block_name'] for block in all_blocks if block['block_name'] in only_blocks]
    else:
        full_size = sum([block['file_size'] for block in all_blocks])

    if not full_size:
        print dataset,"is nowhere"
        return {}, all_block_names


    print len(all_block_names),"blocks"

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #if len(all_block_names)<5000: 
    items = []
    if not complement:
        print "global sub query"
        r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?block=%s%%23*&collapse=n'%(dataset))
        r2=conn.getresponse()
        result = json.loads(r2.read())
        if not len(result['phedex']['dataset']):
            return destinations, all_block_names

        items=result['phedex']['dataset'][0]['block']
        print "Got subscriptions for",dataset
        #print "failed once, let's go block by block: too lengthy"
        #for block in all_block_names:
        #    print "block sub query",block
        #    r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?block=%s&collapse=n'%(block.replace('#','%23')))
        #    r2=conn.getresponse()
        #    result = json.loads(r2.read())
        #    if len(result['phedex']['dataset']):
        #        items.extend( result['phedex']['dataset'][0]['block'] )
        
    destinations=defaultdict(set)


    ## the final answer is site : data_fraction_subscribed

    for item in items:
        if item['name'] not in all_block_names:
            if not only_blocks:
                print item['name'],'not yet injected in dbs, counting anyways'
                all_block_names.add( item['name'] )
            else:
                continue
        for sub in item['subscription']:
            if not any(sub['node'].endswith(v) for v in vetoes):
                if within_sites and not sub['node'] in within_sites: continue
                #if sub['group'] == None: sub['group']=""
                if sub['group'] == None: sub['group']="DataOps" ## assume "" group is dataops
                if group!=None and not sub['group'].lower()==group.lower(): continue 
                destinations[sub['node']].add( (item['name'], int(sub['percent_bytes']), sub['request']) )
    ## per site, a list of the blocks that are subscribed to the site
    ## transform into data_fraction or something valuable
            
    #complement with transfer request, approved, but without subscriptions yet
    if complement:
        time.sleep(1)
        print "Complementing the destinations with request with no subscriptions"
        print "we have",destinations.keys(),"for now"
        try:
            conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))  
            r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset=%s'%dataset)
            r2=conn.getresponse()
        except:
            print "\twaiting a bit for retry"
            time.sleep(1)
            conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))  
            r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset=%s'%dataset)
            r2=conn.getresponse()
        result = json.loads(r2.read())
        items=result['phedex']['request']
    else:
        items=[]

    deletes = {}
    for item in items:
        phedex_id = item['id']
        if item['type']!='delete': continue
        #if item['approval']!='approved': continue
        for node in item['node']:
            if node['decision'] != 'approved': continue
            if not node['name'] in deletes or deletes[node['name']]< int(node['time_decided']):
                ## add it if not or later delete
                deletes[node['name']] = int(node['time_decided'])


    times = {}
    for item in items:
        phedex_id = item['id']
        if item['type']=='delete': continue
        sites_destinations = []
        for req in item['node']:
            if within_sites and not req['name'] in within_sites: continue
            if req['decision'] != 'approved' : continue
            if not req['time_decided']: continue
            if req['name'] in deletes and int(req['time_decided'])< deletes[req['name']]:
                ## this request is void by now
                continue
            if not req['decision'] in ['approved']:
                continue
            if req['name'] in times:
                if int(req['time_decided']) > times[req['name']]: 
                    ## the node was already seen as a destination with an ealier time, and no delete in between
                    continue
            times[req['name']] = int(req['time_decided'])

            sites_destinations.append( req['name'] )
        sites_destinations = [site for site in sites_destinations if not any(site.endswith(v) for v in vetoes)]
        ## what are the sites for which we have missing information ?
        sites_missing_information = [site for site in sites_destinations if site not in destinations.keys()]
        print phedex_id,sites_destinations,"fetching for missing",sites_missing_information


        if len(sites_missing_information)==0: continue

        r3 = conn.request("GET",'/phedex/datasvc/json/prod/transferrequests?request=%s'%phedex_id)
        r4 = conn.getresponse()
        sub_result = json.loads(r4.read())
        sub_items = sub_result['phedex']['request']
        ## skip if we specified group
        for req in sub_items:
            if group!=None and not req['group'].lower()==group.lower(): continue
            for requested_dataset in req['data']['dbs']['dataset']:
                if requested_dataset['name'] != dataset: 
                    print requested_dataset,"not the same"
                    continue
                for site in sites_missing_information:
                    for b in all_block_names:
                        destinations[site].add( (b, 0, phedex_id) )
                    
            for b in req['data']['dbs']['block']:
                if not b['name'] in all_block_names: continue
                destinations[site].add((b['name'], 0, phedex_id) )
        #print "added?",(site in destinations.keys())
        if not site in destinations.keys():
            print json.dumps(sub_items, indent=2)
        
        
    for site in destinations:
        blocks = [b[0] for b in destinations[site]]
        blocks_and_id = dict([(b[0],b[2]) for b in destinations[site]])
        #print blocks_and_id
        completion = sum([b[1] for b in destinations[site]]) / float(len(destinations[site]))
        
        destinations[site] = { "blocks" : blocks_and_id, 
                               #"all_blocks" : list(all_block_names),
                               "data_fraction" : len(blocks) / float(len(all_block_names)) ,
                               "completion" : completion,
                               }

    """
    #print json.dumps( destinations, indent=2)
    for site in destinations:
        print site
        destinations[site]['overlap'] = dict([ (other_site, len(list(set( destinations[site]['blocks']) & set( destinations[other_site]['blocks']))) / float(len(destinations[other_site]['blocks']))) for other_site in destinations.keys()])
        #destinations[site]['overlap'].pop(site)

        destinations[site]['replication'] = sum([sum([destinations[other_site]['blocks'].count(block) for block in destinations[site]['blocks'] ]) / float(len(destinations[site]['blocks'])) for other_site in destinations.keys()])
    """
    
    return destinations, all_block_names

def getDatasetOnGoingDeletion( url, dataset ):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/deletions?dataset=%s&complete=n'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())['phedex']
    return result['dataset']
    
def getDatasetBlocks( dataset, runs=None, lumis=None):
    dbsapi = DbsApi(url=dbs_url)
    all_blocks = set()
    if runs:
        for r in runs:
            all_blocks.update([item['block_name'] for item in dbsapi.listBlocks(run_num=r, dataset= dataset) ])
    if lumis:
        #needs a series of convoluted calls
        #all_blocks.update([item['block_name'] for item in dbsapi.listBlocks( dataset = dataset )])
        pass
    if not runs and not lumis:
        all_blocks.update([item['block_name'] for item in dbsapi.listBlocks(dataset= dataset) ])
    
    return list( all_blocks )

def getUnsubscribedBlocks(url, site):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    urll = '/phedex/datasvc/json/prod/blockreplicas?node=%s&subscribed=n'%site
    r1=conn.request("GET",urll)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['block']
    collected = set()
    for item in items:
        collected.add( item['name'] )

    s = sum([i['bytes'] for i in items])
    print s
         
    return list(collected)


def getDatasetBlockAndSite( url, dataset, group=None,vetoes=None,complete=None):
    ret=10
    while ret>0:
        try:
            return try_getDatasetBlockAndSite( url, dataset, group, vetoes, complete)
        except:
            print dataset,"failed"
            ret-=1
    return None
def try_getDatasetBlockAndSite( url, dataset, group=None,vetoes=None,complete=None):
    if vetoes==None:
        vetoes = ['MSS','Buffer','Export']
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    urll = '/phedex/datasvc/json/prod/blockreplicas?dataset=%s'%(dataset)
    if complete:
        urll += '&complete=%s'%complete
    r1=conn.request("GET",urll)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['block']
    blocks_at_sites=defaultdict(set)
    for item in items:
        for replica in item['replica']:
            if complete and replica['complete'] != complete: continue
            if replica['group'] == None: replica['group']=""
            if group!=None and not replica['group'].lower()==group.lower(): continue
            if vetoes and any([veto in replica['node'] for veto in vetoes]): continue
            blocks_at_sites[replica['node']].add( item['name'] )
    #return dict([(site,list(blocks)) for site,blocks in blocks_at_sites.items()])
    return dict(blocks_at_sites)

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
    all_blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    #print json.dumps( all_blocks, indent=2)
    all_block_names=set([block['block_name'] for block in all_blocks])
    #print sorted(all_block_names)
    #print full_size
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
        #print "Mismatch in phedex/DBS blocks"
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


    presence={}
    for (site,blocks) in locations.items():
        site_size = sum([ block['file_size'] for block in all_blocks if (block['block_name'] in blocks and block['block_name'] in all_block_names)])
        #print site,blocks,all_block_names
        #presence[site] = (set(blocks).issubset(set(all_block_names)), site_size/float(full_size)*100.)
        presence[site] = (set(all_block_names).issubset(set(blocks)), site_size/float(full_size)*100.)
        if site =='T2_US_Nebraska' and False:
            print site,
            print set(all_block_names) - set(blocks)
            print '\n'.join( sorted( all_block_names ))
            print site
            print '\n'.join( sorted( blocks ))
    #print json.dumps( presence , indent=2)
    return presence

def getDatasetBlockSize(dataset):
    dbsapi = DbsApi(url=dbs_url)
    blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    return dict([(block['block_name'],block['file_size']/ (1024.**3)) for block in blocks ])

def getDatasetSize(dataset):
    dbsapi = DbsApi(url=dbs_url)
    blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    ## put everything in terms of GB
    return sum([block['file_size'] / (1024.**3) for block in blocks])

def getDatasetChops(dataset, chop_threshold =1000., talk=False, only_blocks=None):
    chop_threshold = float(chop_threshold)
    ## does a *flat* choping of the input in chunk of size less than chop threshold
    dbsapi = DbsApi(url=dbs_url)
    blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)

    ## restrict to these blocks only
    if only_blocks:
        blocks = [block for block in blocks if block['block_name'] in only_blocks]

    sum_all = 0

    ## put everything in terms of GB
    for block in blocks:
        block['file_size'] /= (1024.**3)

    for block in blocks:
        sum_all += block['file_size']

    items=[]
    sizes=[]
    if sum_all > chop_threshold:
        items.extend( [[block['block_name']] for block in filter(lambda b : b['file_size'] > chop_threshold, blocks)] )
        small_block = filter(lambda b : b['file_size'] <= chop_threshold, blocks)
        small_block.sort( lambda b1,b2 : cmp(b1['file_size'],b2['file_size']), reverse=True)

        while len(small_block):
            first,small_block = small_block[0],small_block[1:]
            items.append([first['block_name']])
            size_chunk = first['file_size']
            while size_chunk < chop_threshold and small_block:
                last,small_block = small_block[-1], small_block[:-1]
                size_chunk += last['file_size']
                items[-1].append( last['block_name'] )
            sizes.append( size_chunk )

                
            if talk:
                print len(items[-1]),"items below thresholds",size_chunk
                print items[-1]
    else:
        if talk:
            print "one big",sum_all
        if only_blocks:
            items = [[block['block_name'] for block in blocks]]
        else:
            items = [[dataset]] 
        sizes = [sum_all]
        if talk:
            print items
    ## a list of list of blocks or dataset
    print "Choped",dataset,"of size",sum_all,"GB (",chop_threshold,"GB) in",len(items),"pieces"
    return items,sizes

def distributeToSites( items, sites , n_copies, weights=None,sizes=None):
    ## assuming all items have equal size or priority of placement
    spreading = defaultdict(list)
    if not weights:
        for item in items:
            for site in random.sample(sites, n_copies):
                spreading[site].extend(item)
        return dict(spreading)
    else:
        ## pick the sites according to computing element plege
        SI = siteInfo()

        for iitem,item in enumerate(items):
            at=set()
            chunk_size = None
            if sizes:
                chunk_size = sizes[iitem]
            #print item,"requires",n_copies,"copies to",len(sites),"sites"
            if len(sites) <= n_copies:
                #more copies than sites
                at = set(sites)
            else:
                # pick at random according to weights
                for pick in range(n_copies):
                    picked_site = SI.pick_CE( list(set(sites)-at))
                    picked_site_se = SI.CE_to_SE( picked_site )
                    ## check on destination free space
                    if chunk_size:
                        overflow=10
                        while (SI.disk[picked_site_se]*1024.)< chunk_size and overflow>0:
                            overflow-=1
                            picked_site = SI.pick_CE( list(set(sites)-at))
                            picked_site_se = SI.CE_to_SE( picked_site )
                        if overflow<0:
                            print "We have run out of options to distribute chunks of data because of lack of space"
                            ## should I crash ?
                            print picked_site_se,"has",SI.disk[picked_site_se]*1024.,"GB free, while trying to put an item of size", chunk_size," that is",json.dumps( item, indent=2)
                            sendEmail('possibly going over quota','We have a potential over quota usage while distributing \n%s check transferor logs'%(json.dumps( item, indent=2)))
                    at.add( picked_site )
                #print list(at)
            for site in at:
                spreading[site].extend(item)                
        return dict(spreading)

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
    reply = dbsapi.listFiles(dataset=dataset, detail=True, run_num=run, validFileOnly=1)
    #print time.mktime(time.gmtime())-start,'[s]'
    files = [f['logical_file_name'] for f in reply if f['is_file_valid'] == 1]
    start = 0
    bucket = 100
    rreply = []
    while True:
        these = files[start:start+bucket]
        if len(these)==0: break
        rreply.extend( dbsapi.listFileLumiArray(logical_file_name=these,run_num=run))
        start+=bucket
        #print len(rreply)
    return rreply


def getDatasetLumis(dataset, runs=None, with_cache=False):
    dbsapi = DbsApi(url=dbs_url)
    c_name= '%s/.%s.lumis.json'%(cache_dir,dataset.replace('/','_'))
    if os.path.isfile(c_name) and with_cache:
        print "picking up from cache",c_name
        opened = json.loads(open(c_name).read())
        ## need to filter on the runs
        if runs:
            return dict([(k,v) for (k,v) in opened.items() if int(k) in runs])
        else:
            return opened


    full_lumi_json = defaultdict(set)
    d_runs = getDatasetRuns( dataset )
    #print len(runs),"runs"
    for run in d_runs:
        files = getFilesWithLumiInRun( dataset, run )
        #print run,len(files),"files"
        for f in files:
            full_lumi_json[run].update( f['lumi_section_num'] )

    ## convert
    lumi_json = {}
    for r in full_lumi_json: 
        full_lumi_json[r] = list(full_lumi_json[r])
        if runs and not r in runs: continue
        lumi_json[r] = list(full_lumi_json[r])
        
    open(c_name,'w').write( json.dumps( dict(full_lumi_json), indent=2))
    return dict(lumi_json)
            
def getDatasetEventsPerLumi(dataset):
    dbsapi = DbsApi(url=dbs_url)
    try:
        all_files = dbsapi.listFileSummaries( dataset = dataset , validFileOnly=1)
    except:
        print "We had to have a DBS listfilesummaries retry"
        time.sleep(1)
        all_files = dbsapi.listFileSummaries( dataset = dataset , validFileOnly=1)        
    try:
        average = sum([f['num_event']/float(f['num_lumi']) for f in all_files]) / float(len(all_files))
    except:
        average = 100
    return average
           
def setFileStatus(file_names, validate=True):
    dbswrite = DbsApi(url=dbs_url_writer)
    dbsapi = DbsApi(url=dbs_url)
    files = dbsapi.listFileArray(logical_file_name = file_names, detail=True)
    for fn in files:
        status = fn['is_file_valid']
        if status != validate:
            ## then change the status
            print "Turning",fn['logical_file_name'],"to",validate
            dbswrite.updateFileStatus( logical_file_name= fn['logical_file_name'], is_file_valid = int(validate) )


def setDatasetStatus(dataset, status):
    dbswrite = DbsApi(url=dbs_url_writer)

    new_status = getDatasetStatus( dataset )
    if new_status == None:
        ## means the dataset does not exist in the first place
        print "setting dataset status",status,"to inexistant dataset",dataset,"considered succeeding"
        return True

    max_try=3
    while new_status != status:
        dbswrite.updateDatasetType(dataset = dataset, dataset_access_type= status)
        new_status = getDatasetStatus( dataset )
        max_try-=1
        if max_try<0: return False
    return True
     
def getDatasetStatus(dataset):
        # initialize API to DBS3                                                                                                                                                                                                                                                     
        dbsapi = DbsApi(url=dbs_url)
        # retrieve dataset summary                                                                                                                                                                                                                                                   
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*',detail=True)
        if len(reply):
            return reply[0]['dataset_access_type']
        else:
            return None

def getDatasets(dataset):
    # initialize API to DBS3                                                                                                                                                                                                                                                    
    dbsapi = DbsApi(url=dbs_url)
    # retrieve dataset summary

    ## that does not work anymore
    #reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*')
    _,ds,p,t = dataset.split('/')
    a,s,v = p.split('-')
    reply = dbsapi.listDatasets(primary_ds_name = ds, 
                                #acquisition_era_name = a,
                                processed_ds_name = p,
                                data_tier_name = t,
                                dataset_access_type='*')
    return reply

def createXML(datasets):
    """
    From a list of datasets return an XML of the datasets in the format required by Phedex
    """
    # Create the minidom document
    impl=getDOMImplementation()
    doc=impl.createDocument(None, "data", None)
    result = doc.createElement("data")
    result.setAttribute('version', '2')
    # Create the <dbs> base element
    dbs = doc.createElement("dbs")
    dbs.setAttribute("name", dbs_url)
    result.appendChild(dbs)    
    #Create each of the <dataset> element
    for datasetname in datasets:
        dataset=doc.createElement("dataset")
        dataset.setAttribute("is-open","y")
        dataset.setAttribute("is-transient","y")
        dataset.setAttribute("name",datasetname)
        dbs.appendChild(dataset)
    return result.toprettyxml(indent="  ")

def phedexPost(url, request, params):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    encodedParams = urllib.urlencode(params, doseq=True)
    r1 = conn.request("POST", request, encodedParams)
    r2 = conn.getresponse()
    res = r2.read()
    try:
        result = json.loads(res)
    except:
        print "PHEDEX error",res
        return None
    conn.close()
    return result

def approveSubscription(url, phedexid, nodes=None , comments =None, decision = 'approve'):
    if comments==None:
        comments = 'auto-approve of production prestaging'
    if not nodes:
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?request='+str(phedexid))
        r2=conn.getresponse()
        result = json.loads(r2.read())
        items=result['phedex']['request']
        nodes=set()
        for item in items:
            for node in item['node']:
                nodes.add(node['name'])
        # find out from the request itself ?
        nodes = list(nodes)

        #nodes = ["T2_CH_CERN","T1_US_FNAL_Disk","T0_CH_CERN_MSS"]

    params = {
        'decision' : decision,
        'request' : phedexid,
#        'node' : nodes,
        'node' : ','.join(nodes),
        'comments' : comments
        }
    
    result = phedexPost(url, "/phedex/datasvc/json/prod/updaterequest", params)
    if not result:
        return False

    if 'already' in result:
        return True
    return result

def disapproveSubscription(url, phedexid, nodes=None , comments =None):
    if comments==None:
        comments = 'auto-approve of production prestaging'
    if not nodes:
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?request='+str(phedexid))
        r2=conn.getresponse()
        result = json.loads(r2.read())
        items=result['phedex']['request']
        nodes=set()
        for item in items:
            for node in item['node']:
                nodes.add(node['name'])
        # find out from the request itself ?
        nodes = list(nodes)

        #nodes = ["T2_CH_CERN","T1_US_FNAL_Disk","T0_CH_CERN_MSS"]

    params = {
        'decision' : 'disapprove',
        'request' : phedexid,
#        'node' : nodes,
        'node' : ','.join(nodes),
        'comments' : comments
        }
    
    result = phedexPost(url, "/phedex/datasvc/json/prod/updaterequest", params)
    if not result:
        return False

    if 'already' in result:
        return True
    return result

def makeDeleteRequest(url, site,datasets, comments):
    dataXML = createXML(datasets)
    params = { "node" : site,
               "data" : dataXML,
               "level" : "dataset",
               "rm_subscriptions":"y",
               #"group": "DataOps",
               #"priority": priority,
               #"request_only":"y" ,
               #"delete":"y",
               "comments":comments
               }
    print site
    response = phedexPost(url, "/phedex/datasvc/json/prod/delete", params)
    return response

def makeReplicaRequest(url, site,datasets, comments, priority='normal',custodial='n',approve=False,mail=True,group="DataOps"): # priority used to be normal
    dataXML = createXML(datasets)
    r_only = "n" if approve else "y"
    notice = "n" if mail else "y"
    params = { "node" : site,"data" : dataXML, "group": group, "priority": priority,
                 "custodial":custodial,"request_only":r_only ,"move":"n","no_mail":notice,"comments":comments}
    response = phedexPost(url, "/phedex/datasvc/json/prod/subscribe", params)
    return response

def makeMoveRequest(url, site,datasets, comments, priority='normal',custodial='n',group="DataOps"): # priority used to be normal
    dataXML = createXML(datasets)
    params = { "node" : site,"data" : dataXML, "group": group, "priority": priority,
                 "custodial":custodial,"request_only":"y" ,"move":"y","no_mail":"n","comments":comments}
    response = phedexPost(url, "/phedex/datasvc/json/prod/subscribe", params)
    return response

def updateSubscription(url, site, item, priority=None, user_group=None, suspend=None):
    params = { "node" : site }
    if '#' in item:
        params['block'] = item.replace('#','%23')
    else:
        params['dataset'] = item
        
    #params['block' if '#' in item else 'dataset'] = item
    if priority:   params['priority'] = priority
    if user_group: params['user_group'] = user_group
    if suspend!=None: params['suspend_until']  = suspend
    response = phedexPost(url, "/phedex/datasvc/json/prod/updatesubscription", params) 
    #print response
    return response

def getWorkLoad(url, wf ):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1= conn.request("GET",'/reqmgr2/data/request/'+wf, headers={"Accept":"*/*"})
    r2=conn.getresponse()
    data = json.loads(r2.read())
    return data['result'][0][wf]


#def getViewByInput( url, details=False):
#    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
#    there = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byinputdataset?'
#    if details:
#        there+='&include_docs=true'
#    r1=conn.request("GET",there)
#    r2=conn.getresponse()
#    data = json.loads(r2.read())
#    items = data['rows']
#    return items
#    if details:
#        return [item['doc'] for item in items]
#    else:
#        return [item['id'] for item in items]

#def getViewByOutput( url, details=False):
#    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
#    there = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byoutputdataset?'
#    if details:
#        there+='&include_docs=true'
#    r1=conn.request("GET",there)
#    r2=conn.getresponse()
#    data = json.loads(r2.read())
#    items = data['rows']
#    return items
#    if details:
#        return [item['doc'] for item in items]
#    else:
#        return [item['id'] for item in items]
        
def getWorkflowByInput( url, dataset , details=False):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byinputdataset?key="%s"'%(dataset)
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

def getWorkflowByOutput( url, dataset , details=False):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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

def getWorkflowByMCPileup( url, dataset , details=False):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bymcpileup?key="%s"'%(dataset)
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

def getWorkflowById( url, pid , details=False):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    


def forceComplete(url, wfi):
    import reqMgrClient
    familly = getWorkflowById( url, wfi.request['PrepID'] ,details=True)
    for member in familly:
        ### if member['RequestName'] == wl['RequestName']: continue ## set himself out
        if member['RequestDate'] < wfi.request['RequestDate']: continue
        if member['RequestStatus'] in ['None',None]: continue
        ## then set force complete all members
        if member['RequestStatus'] in ['running-opened','running-closed']:
            #sendEmail("force completing","%s is worth force completing\n%s"%( member['RequestName'] , percent_completions))
            print "setting",member['RequestName'],"force-complete"
            reqMgrClient.setWorkflowForceComplete(url, member['RequestName'])
        elif member['RequestStatus'] in ['acquired','assignment-approved']:
            print "rejecting",member['RequestName']
            reqMgrClient.invalidateWorkflow(url, member['RequestName'], current_status=member['RequestStatus'])

def getAllAgents(url):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    url = '/couchdb/wmstats/_design/WMStats/_view/agentInfo?stale=update_after'
    r1=conn.request("GET",url)
    r2=conn.getresponse()
    teams = defaultdict(list)
    for r in [i['value'] for i in json.loads( r2.read() )['rows']]:
        teams[r['agent_team']].append( r )
    return teams

def getWorkflows(url,status,user=None,details=False,rtype=None, priority=None):
    retries=10000
    wait=2
    while retries>0:
        try:
            return try_getWorkflows(url, status,user,details,rtype,priority)
        except Exception as e:
            print "getWorkflows retried"
            print str(e)
            time.sleep(wait)
            #wait+=2
            retries-=1
    raise Exception("getWorkflows failed 10 times")
    
def try_getWorkflows(url,status,user=None,details=False,rtype=None, priority=None):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

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


    #print len(workflows)
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

def getLFNbase(dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs_url)
        # retrieve file
        reply = dbsapi.listFiles(dataset=dataset)
        file = reply[0]['logical_file_name']
        return '/'.join(file.split('/')[:3])


def getListOfBlocks(inputdset,runwhitelist):
    dbsApi = DbsApi(url=dbs_url)
    blocks=dbsApi.listBlocks(dataset = inputdset, run_num = runwhitelist)
    
    block_list = []

    for block in blocks:
        block_list.append(block['block_name'])

    return block_list

def checkIfBlockIsSubscribedToASite(url,block,site):

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?block='+block.replace('#','%23'))

    r2=conn.getresponse()
    result = json.loads(r2.read())

    assert(len(result['phedex']['dataset']) == 1)

    for subscription in result['phedex']['dataset'][0]['subscription']:
        if subscription['node'] == site:
            return True

    if 'block' in result['phedex']['dataset'][0]:    

        assert(len(result['phedex']['dataset'][0]['block']) == 1)

        for subscription in result['phedex']['dataset'][0]['block'][0]['subscription']:
            if subscription['node'] == site:
                return True
        
    return False            


def checkIfDatasetIsSubscribedToASite(url,dataset,site):

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?dataset='+dataset)
    r2=conn.getresponse()
    result = json.loads(r2.read())

    if len(result['phedex']['dataset']) == 0:
        return False

    if len(result['phedex']['dataset']) != 1:
        os.system('echo '+dataset+' | mail -s \"utils.py error 1\" andrew.m.levin@vanderbilt.edu')
        sys.exit(1)

    for subscription in result['phedex']['dataset'][0]['subscription']:
        if subscription['node'] == site:
            return True
        
    return False            

def checkIfBlockIsAtASite(url,block,site):
    
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicasummary?block='+block.replace('#','%23'))
    r2=conn.getresponse()

    result = json.loads(r2.read())

    assert(len(result['phedex']['block']) == 1)

    for replica in result['phedex']['block'][0]['replica']:
        if replica['node'] == site and replica['complete'] == 'y':
            return True

    return False                


def getForceCompletes():
    overrides = {}
    for rider,email in [('vlimant','vlimant@cern.ch'),('jen_a','jen_a@fnal.gov'),('srimanob','srimanob@mail.cern.ch')]:
        rider_file = '/afs/cern.ch/user/%s/%s/public/ops/forcecomplete.json'%(rider[0],rider)
        if not os.path.isfile(rider_file):
            print "no file",rider_file
            continue
        try:
            overrides[rider] = json.loads(open( rider_file ).read() )
        except:
            print "cannot get force complete list from",rider
            sendEmail("malformated force complet file","%s is not json readable"%rider_file, destination=[email])
    return overrides


class workflowInfo:
    def __init__(self, url, workflow, spec=True, request=None,stats=False, wq=False, errors=False):
        self.logs = defaultdict(str)
        self.url = url
        self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
                    #r1=self.conn.request("GET",'/reqmgr2/data/request/'+workflow, headers={"Accept":"*/*"}) ## new
                    r2=self.conn.getresponse()
                    ret = json.loads(r2.read())
                    #ret = ret['result'][0][workflow]## new
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
        if not self.full_spec:
            self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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

            self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            r1=self.conn.request("GET",'/wmstatsserver/data/jobdetail/%s'%(self.request['RequestName']), headers={"Accept":"*/*"})
            r2=self.conn.getresponse()

            self.errors = json.loads(r2.read())['result'][0][self.request['RequestName']]
            try:
                open(f_cache,'w').write( json.dumps({'timestamp': time.mktime(time.gmtime()),
                                                     'data' : self.errors}))
            except Exception as e:
                print str(e)
            return self.errors
        except:
            print "Could not get wmstats errors for",self.request['RequestName']
            self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            return {}

    def getFullPicture(self, since=1, cache=0):
        by_site = self.getDashboard(since, sortby='site')
        picture = {}
        for site in by_site:
            print "dashboard for",site
            picture[site] = self.getDashboard(since, cache=cache, sortby='appexitcode', site=site)
        return picture

    def getDashboard(self, since=1, cache=0, **args):
        ### how do you encode the args in the file_cache ?
        hash = hashlib.sha224('since=%d'%(since)+str(args)).hexdigest()
        f_cache = '%s/%s.%s.dashb'% (cache_dir, self.request['RequestName'], hash)
        if cache:
            if os.path.isfile(f_cache):
                d_cache = json.loads(open(f_cache).read())
                now = time.mktime(time.gmtime())
                stamp = d_cache['timestamp']
                c_since = d_cache.get('since',0)
                c_arg = d_cache.get('args',{})
                if (now-stamp) < cache and since == c_since and all([ c_arg.get(k,None) == args[k] for k in args.keys() ]):
                    print "dashb taken from cache",f_cache
                    self.dashb = d_cache['data']
                    return self.dashb

        dconn = httplib.HTTPConnection('dashb-cms-job.cern.ch')

        dargs={
            'task' : 'wmagent_%s'%self.request['RequestName'],
            'user':'',
            'site':'',
            'submissiontool':'',
            'application':'',
            'activity':'',
            'status':'',
            'check':'submitted',
            'tier':'',
            'sortby':'site',
            'ce':'',
            'rb':'',
            'grid':'',
            'jobtype':'',
            'submissionui':'',
            'dataset':'',
            'submissiontype':'',
            'subtoolver':'',
            'genactivity':'',
            'outputse':'',
            'appexitcode':'',
            'accesstype':'',
            'inputse':'',
            'cores':'',
            'date1': time.strftime('%Y-%m-%d+%H:%M', time.gmtime(time.mktime(time.gmtime())-(since*24*60*60)) ),
            'date2': time.strftime('%Y-%m-%d+%H:%M', time.gmtime())
            }
            
        for key in args:
            if key in dargs:
                dargs[key] = args[key]

        url = '/dashboard/request.py/jobsummary-plot-or-table2?'+'&'.join( [ '%s=%s'%(k,v) for k,v in dargs.items()] )
        r1 = dconn.request('GET',url)
        r2 = dconn.getresponse()
        r = json.loads( r2.read())['summaries']
        ## transform into a dict
        self.dashb = dict([(d['name'],d) for d in r])
        try:
            open(f_cache,'w').write( json.dumps({'timestamp': time.mktime(time.gmtime()),
                                                 'since' : since,
                                                 'args' : args,
                                                 'data' : self.dashb}))
        except Exception as e:
            print str(e)
            pass
        return self.dashb

    def getWMStats(self ,cache=0):
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
            print str(e)

        return self.wmstats

    def getRecoveryBlocks(self):
        doc = self.getRecoveryDoc()
        all_files = set()    
        for d in doc:
            all_files.update( d['files'].keys())
        print len(all_files),"file in recovery"
        dbsapi = DbsApi(url=dbs_url)
        all_blocks = set()
        files_no_block = set()
        files_in_block = set()
        datasets = set()
        for f in all_files:
            try:
                r = dbsapi.listFileArray( logical_file_name = f, detail=True)
            except Exception as e:
                print "dbsapi.listFileArray failed on",f
                print str(e)
                continue

            if not r:
                files_no_block.add( f)
            else:
                files_in_block.add( f )
                all_blocks.update( [df['block_name'] for df in r ])
        dataset_blocks = set()
        for dataset in set([block.split('#')[0] for block in all_blocks]):
            print dataset
            dataset_blocks.update( getDatasetBlocks( dataset ) )
                     
        return dataset_blocks,all_blocks,files_in_block,files_no_block

    def getRecoveryDoc(self):
        if self.recovery_doc != None:
            return self.recovery_doc
        try:
            r1=self.conn.request("GET",'/couchdb/acdcserver/_design/ACDC/_view/byCollectionName?key="%s"&include_docs=true&reduce=false'% self.request['RequestName'])
            r2=self.conn.getresponse()
            rows = json.loads(r2.read())['rows']
            self.recovery_doc = [r['doc'] for r in rows]
        except:
            self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            print "failed to get the acdc document for",self.request['RequestName']
            self.recovery_doc = None
        return self.recovery_doc

    def getRecoveryInfo(self):
        self.getRecoveryDoc()

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

    
    def getWorkQueueElements(self):
        wq = self.getWorkQueue()
        wqes = [w[w['type']] for w in wq]
        return wqes

    def getWorkQueue(self):
        if not self.workqueue:
            try:
                r1=self.conn.request("GET",'/couchdb/workqueue/_design/WorkQueue/_view/elementsByParent?key="%s"&include_docs=true'% self.request['RequestName'])
                r2=self.conn.getresponse()
            except:
                try:
                    time.sleep(1) ## time-out
                    r1=self.conn.request("GET",'/couchdb/workqueue/_design/WorkQueue/_view/elementsByParent?key="%s"&include_docs=true'% self.request['RequestName'])
                    r2=self.conn.getresponse()
                except:
                    self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                    print "failed to get work queue for",self.request['RequestName']
                    self.workqueue = []
                    return self.workqueue
            self.workqueue = list([d['doc'] for d in json.loads(r2.read())['rows']])
        return self.workqueue
    

    def getJobs(self):
        agents = self.getActiveAgents()  
        agents = map(lambda s : s.split('/')[-1].split(':')[0], agents)

        class agentCom:
            def __init__(self, a):
                self.a = a
            def get(self, wfn):
                print "connecing to",self.a
                data = os.popen('ssh %s python %s/WmAgentScripts/local.py -w %s'%( self.a, base_dir, wfn)).read()
                return json.loads( data )['rows']
        print "Participating",','.join( agents )
        collect={}
        n=0
        for agent in agents:
            print agent
            ac = agentCom(agent)
            d= ac.get( self.request['RequestName'] )
            collect[agent] = [i['value'] for i in d]
            an = len( collect[agent] )
            n += an
            print an,"job(s) found in ",agent
        print n,"job(s) found in ",len(agents),"agents"
        return collect

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

    def getGQLocations(self):
        wq = self.getWorkQueue()
        wqes = [w[w['type']] for w in wq]
        ins=defaultdict(list)
        for wqe in wqes:
            for i in wqe['Inputs']:
                ins[i] = list(set(ins[i] + wqe['Inputs'][i]))
        return ins

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

        self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=self.conn.request("GET",'/couchdb/workloadsummary/'+self.request['RequestName'], headers={"Accept":"application/json"} )
        r2=self.conn.getresponse()

        self.summary = json.loads(r2.read())
        return self.summary


    def getErrors(self):
        all_errors = {}
        summary = self.getSummary()
        if summary and 'errors' in summary:
            all_errors = summary['errors']
            for task,errors in all_errors.items():
                print "\tTask",task 
                ## filtering of tasks we do not care about
                if 'Clean' in task: continue
                all_codes = []
                for name, codes in errors.items():
                    if type(codes)==int: continue
                    all_codes.extend( [(int(code),info['jobs'],name,list(set([e['type'] for e in info['errors']])),list(set([e['details'] for e in info['errors']])) ) for code,info in codes.items()] )

                all_codes.sort(key=lambda i:i[1], reverse=True)
                sum_failed = sum([l[1] for l in all_codes])
                for errorCode,njobs,name,types,details in all_codes:
                    rate = 100*njobs/float(sum_failed)
                    #print ("\t\t %10d (%6s%%) failures with error code %10d (%"+str(max_legend)+"s) at stage %s")%(njobs, "%4.2f"%rate, errorCode, legend, name)                                                                                
                    print ("\t\t %10d (%6s%%) failures with error code %10d (%30s) at stage %s")%(njobs, "%4.2f"%rate, errorCode, ','.join(types), name)
                    
                    added_in_recover=False
                    if errorCode in error_codes_to_recover:
                        ## the error code is registered                                                                                                                                                                                     
                        for case in error_codes_to_recover[errorCode]:
                            match = case['details']
                            matched= (match==None)
                            if not matched:
                                matched=False
                                for detail in details:
                                    if match in detail:
                                        print "[recover] Could find keyword",match,"in"
                                        print 50*"#"
                                        print detail
                                        print 50*"#"
                                        matched = True
                                        break
                            if matched and rate > case['rate']:
                                print "\t\t => we should be able to recover that", case['legend']
                                task_to_recover[task].append( (code,case) )
                                added_in_recover=True
                                message_to_user = ""
                            else:
                                print "\t\t recoverable but not frequent enough, needs",case['rate']

                    if errorCode in error_codes_to_block:
                        for case in error_codes_to_block[errorCode]:
                            match = case['details']
                            matched= (match==None)
                            if not matched:
                                matched=False
                                for detail in details:
                                    if match in detail:
                                        print "[block] Could find keyword",match,"in"
                                        print 50*"#"
                                        print detail
                                        print 50*"#"
                                        matched = True
                                        break
                            if matched and rate > case['rate']:
                                print "\t\t => that error means no ACDC on that workflow", case['legend']
                                if not options.go:
                                    message_to_ops += "%s has an error %s blocking an ACDC.\n%s\n "%( wfo.name, errorCode, '#'*50 )
                                    recover = False
                                    added_in_recover=False
                                
                    if errorCode in error_codes_to_notify and not added_in_recover:
                        print "\t\t => we should notify people on this"
                        message_to_user += "%s has an error %s in processing.\n%s\n" %( wfo.name, errorCode, '#'*50 )


        else:
            return None


    def getGlideMon(self):
        try:
            gmon = json.loads(os.popen('curl -s http://cms-gwmsmon.cern.ch/prodview/json/%s/summary'%self.request['RequestName']).read())
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
                    tpe =task['TimePerEvent']
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
    
    #def getNCopies(self, CPUh=None, m = 2, M = 6, w = 50000, C0 = 100000):
    def getNCopies(self, CPUh=None, m = 2, M = 3, w = 50000, C0 = 100000):
        def sigmoid(x):      
            return 1 / (1 + math.exp(-x)) 
        if CPUh==None:
            CPUh = self.getComputingTime()
        f = sigmoid(-C0/w)
        D = (M-m) / (1-f)
        O = (f*M - m)/(f-1) 
        #print O
        #print D
        return int(O + D * sigmoid( (CPUh - C0)/w)), CPUh

    def availableSlots(self):
        av = 0
        SI = global_SI()
        if 'SiteWhitelist' in self.request:
            return SI.availableSlots( self.request['SiteWhitelist'] )
        else:
            allowed = getSiteWhiteList( self.getIO() )
            return SI.availableSlots( allowed )

    def getSystemTime(self):
        ct = self.getComputingTime()
        resource = self.availableSlots()
        if resource:
            return ct / resource
        else:
            print "cannot compute system time for",self.request['RequestName']
            return 0

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

                        #print parent['splittingTask'],"is parent of",t
                        #print p_size,c_size
                        if not min_child_job_per_event or min_child_job_per_event > c_size:
                            min_child_job_per_event = c_size
                else:
                    root_job_per_event = c_size
                    
                if c_size and p_size:
                    blow_up = float(p_size)/ c_size
                    #print "parent jobs",p_size,"compared to my size",c_size
                    #print blow_up
                    if blow_up > max_blow_up:
                        max_blow_up = blow_up
            return (min_child_job_per_event, root_job_per_event, max_blow_up)    
        return (1.,1.,1.)
    def getSiteWhiteList( self, pickone=False, verbose=True):
        ### this is not used yet, but should replace most
        SI = global_SI()
        (lheinput,primary,parent,secondary) = self.getIO()
        sites_allowed=[]
        if lheinput:
            sites_allowed = ['T2_CH_CERN'] ## and that's it                                                                                                                                   
        elif secondary:
            sites_allowed = list(set(SI.sites_T1s + SI.sites_with_goodIO))
        elif primary:
            sites_allowed =list(set( SI.sites_T1s + SI.sites_T2s ))
        else:
            # no input at all
            ## all site should contribute
            sites_allowed =list(set( SI.sites_T2s + SI.sites_T1s))
            ### hack if we have urgency to kick gen-sim away
            #ust2s = set([site for site in SI.sites_T2s if site.startswith('T2_US')])
            #allmcores = set(SI.sites_mcore_ready)
            #sites_allowed =list(set( SI.sites_T2s ) - ust2s) ## remove all US
            #sites_allowed = list(set( SI.sites_T2s ) - allmcores) ## remove all multicore ready
        if pickone:
            sites_allowed = [SI.pick_CE( sites_allowed )]
            
        # do further restrictions based on memory
        # do further restrictions based on blow-up factor
        (min_child_job_per_event, root_job_per_event, max_blow_up) = self.getBlowupFactors()
        if max_blow_up > 5.:
            ## then restrict to only sites with >4k slots
            if verbose:
                print "restricting site white list because of blow-up factor",min_child_job_per_event, root_job_per_event, max_blow_up
            sites_allowed = list(set(sites_allowed) & set([site for site in sites_allowed if SI.cpu_pledges[site] > 4000]))


        CI = campaignInfo()
        for campaign in self.getCampaigns():
            c_sites_allowed = CI.get(campaign, 'SiteWhitelist' , [])
            if c_sites_allowed:
                if verbose:
                    print "Using site whitelist restriction by campaign,",campaign,"configuration",sorted(c_sites_allowed)
                sites_allowed = list(set(sites_allowed) & set(c_sites_allowed))
                if not sites_allowed:
                    sites_allowed = list(c_sites_allowed)

        c_black_list = CI.get(self.request['Campaign'], 'SiteBlacklist', [])
        c_black_list.extend( CI.parameters(self.request['Campaign']).get('SiteBlacklist', []))
        if c_black_list:
            if verbose:
                print "Reducing the whitelist due to black list in campaign configuration"
                print "Removing",c_black_list
            sites_allowed = list(set(sites_allowed) - set(c_black_list))

        #ncores = self.request.get('Multicore',1)
        ncores = self.getMulticore()
        memory_allowed = SI.sitesByMemory( float(self.request['Memory']) , maxCore=ncores)
        if memory_allowed!=None:
            if verbose:
                print "sites allowing",self.request['Memory'],"MB and",ncores,"core are",memory_allowed
            ## mask to sites ready for mcore
            if  ncores>1:
                memory_allowed = list(set(memory_allowed) & set(SI.sites_mcore_ready))
            sites_allowed = list(set(sites_allowed) & set(memory_allowed))

            


        return (lheinput,primary,parent,secondary,sites_allowed)

    def checkWorkflowSplitting( self ):
        if self.request['RequestType']=='TaskChain':
            (min_child_job_per_event, root_job_per_event, max_blow_up) = self.getBlowupFactors()
            if min_child_job_per_event and max_blow_up>2.:
                print min_child_job_per_event,"should be the best non exploding split"
                print "to be set instead of",root_job_per_event
                setting_to = min_child_job_per_event*1.5
                print "using",setting_to
                print "Not setting anything yet, just informing about this"
                return True
                #return {'EventsPerJob': setting_to }
            return True

        ncores = self.getMulticore()
        ## this isn't functioning for taskchain BTW
        if 'InputDataset' in self.request:
            average = getDatasetEventsPerLumi(self.request['InputDataset'])
            timing = self.request['TimePerEvent']

            ## need to divide by the number of cores in the job
            average /= ncores 
            ## if we can stay within 48 with one lumi. do it
            timeout = 48 *60.*60. #self.request['OpenRunningTimeout']
            if (average * timing) < timeout:
                ## we are within overboard with one lumi only
                return True

            spl = self.getSplittings()[0]
            algo = spl['splittingAlgo']
            if algo == 'EventAwareLumiBased':
                events_per_job = spl['avg_events_per_job']
                if average > events_per_job:
                    ## need to do something
                    print "This is going to fail",average,"in and requiring",events_per_job
                    return {'SplittingAlgorithm': 'EventBased'}
        return True

    def getSchema(self):
        #new_schema = copy.deepcopy( self.get_spec().request.schema.dictionary_())
        
        self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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

    def getAllTasks(self, select=None):
        all_tasks = []
        for task in self._tasks():
            ts = getattr(self.get_spec().tasks, task)
            all_tasks.extend( self._taskDescending( ts, select ) )
        return all_tasks

    def getSplittings(self):

        spl =[]
        for task in self.getWorkTasks():
            ts = task.input.splitting
            spl.append( { "splittingAlgo" : ts.algorithm,
                          "splittingTask" : task.pathName,
                          } )
            get_those = ['events_per_lumi','events_per_job','lumis_per_job','halt_job_on_file_boundaries','max_events_per_lumi','halt_job_on_file_boundaries_event_aware']#,'couchdDB']#,'couchURL']#,'filesetName']
            #print ts.__dict__.keys()
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

        ## this below is a functioning interface for retrieving the splitting from reqmgr2 used to make the call to reqmgr2 splitting change


        r1=self.conn.request("GET",'/reqmgr2/data/splitting/%s'%self.request['RequestName'], headers={"Accept":"application/json"} )
        r2=self.conn.getresponse()
        translate = { 
            #'EventAwareLumiBased' : [('events_per_job','avg_events_per_job')]
            }
        include = {
            #'EventAwareLumiBased' : { 'halt_job_on_file_boundaries_event_aware' : 'True' },
            #'LumiBased' : { 'halt_job_on_file_boundaries' : 'True'}
            }
        result = json.loads( r2.read() )['result']
        splittings = []
        for spl in result:
            if not spl['taskType'] in ['Production','Processing','Skim'] : continue
            splittings.append( spl )
            ## add default value which were not returned in the first place
            if spl['splitAlgo'] in include:
                for k,v in include[spl['splitAlgo']].items():
                    #splittings[-1][k] = v 
                    pass
            if spl['splitAlgo'] in translate:
                for (src,des) in translate[spl['splitAlgo']]:
                    splittings[-1][des] = splittings[-1].pop(src)

            continue
            #spl.append( { "splittingAlgo" : ts.algorithm,
            #              "splittingTask" : task.pathName,
            #              } )
            #get_those = ['events_per_lumi','events_per_job','lumis_per_job','halt_job_on_file_boundaries','max_events_per_lumi','halt_job_on_file_boundaries_event_aware']#,'couchdDB']#,'couchURL']#,'filesetName']
            
            #if ts.algorithm in include:
            #    for k,v in include[ts.algorithm].items():
            #        spl[-1][k] = v

            #for get in get_those:
            #    if hasattr(ts,get):
            #        set_to = get
            #        if ts.algorithm in translate:
            #            for (src,des) in translate[ts.algorithm]:
            #                if src==get:
            #                    set_to = des
            #                    break
            #        spl[-1][set_to] = getattr(ts,get)

        return splittings

    def getCurrentStatus(self):
        return self.request['RequestStatus']

    def getRequestNumEvents(self):
        if 'RequestNumEvents' in self.request and int(self.request['RequestNumEvents']):
            return int(self.request['RequestNumEvents'])
        else:
            return int(self.request['Task1']['RequestNumEvents'])

    def getPriority(self):
        return self.request['RequestPriority']

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

        return bwl
    def getLumiWhiteList(self):
        lwl=[]
        if 'Chain' in self.request['RequestType']:
            lwl_t = self._collectinchain('LumiWhitelist')
            for task in lwl_t:
                lwl.extend(lwl_t[task])
        else:
            if 'LumiWhitelist' in self.request:
                lwl.extend(self.request['LumiWhitelist'])
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
        return lwl

    def getBlocks(self):
        blocks = set()
        (_,primary,_,_) = self.getIO()

        blocks.update( self.getBlockWhitelist() )
        run_list = self.getRunWhiteList()
        if run_list:
            for dataset in primary:
                blocks.update( getDatasetBlocks( dataset, runs=run_list ) )
        lumi_list = self.getLumiWhiteList()
        if lumi_list:
            for dataset in primary:
                blocks.update( getDatasetBlocks( dataset, lumis= self.request['LumiList'] ) )
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
            #elif 'InputDatasets' in blob: primary = set(filter(None,blob['InputDatasets']))
            if primary and 'IncludeParent' in blob and blob['IncludeParent']:
                parent = findParent( primary )
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

    def getCampaigns(self):
        if 'Chain' in self.request['RequestType']:
            return self._collectinchain('AcquisitionEra').values()
        else:
            return [self.request['Campaign']]

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
            acqEra = self._collectinchain('AcquisitionEra', func=invertDigits)
        else:
            acqEra = invertDigits(self.request['Campaign'])
        return acqEra

    def processingString(self):
        if 'Chain' in self.request['RequestType']:
            return self._collectinchain('ProcessingString')
        else:
            return self.request['ProcessingString']
        

            
    def go(self,log=False):
        CI = campaignInfo()
        pss = self.processingString()
        aes = self.acquisitionEra()

        if type(pss) == dict:
            pas = [(aes[t],pss[t]) for t in pss]
        else:
            pas = [(aes,pss)]
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
        #print outputs
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
            #print "version found so far",version
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
                    conflicts = getWorkflowByOutput( self.url, predicted )
                    conflicts = filter(lambda wfn : wfn!=self.request['RequestName'], conflicts)
                    if len(conflicts):
                        print "There is an output conflict for",self.request['RequestName'],"with",conflicts
                        #return None
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

