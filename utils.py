import sys
import urllib, urllib2
import logging
from dbs.apis.dbsClient import DbsApi
#import reqMgrClient
import httplib
import os
import socket
import json
import collections
from collections import defaultdict
import random
from xml.dom.minidom import getDOMImplementation
import copy
import pickle
import itertools
import time
import math
import hashlib
import threading
import glob

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email.utils import make_msgid

dbs_url = os.getenv('UNIFIED_DBS3_READER' ,'https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
dbs_url_writer = os.getenv('UNIFIED_DBS3_WRITER','https://cmsweb.cern.ch/dbs/prod/global/DBSWriter')

phedex_url = os.getenv('UNIFIED_PHEDEX','cmsweb.cern.ch')
reqmgr_url = os.getenv('UNIFIED_REQMGR','cmsweb.cern.ch')
monitor_dir = os.getenv('UNIFIED_MON','/data/unified/www/')
#monitor_eos_dir = "/eos/project/c/cms-unified-logs/www/"
monitor_eos_dir = '/eos/cms/store/unified/www/'
monitor_dir = monitor_eos_dir
monitor_pub_dir = os.getenv('UNIFIED_MON','/data/unified/www/public/')
#monitor_pub_eos_dir = "/eos/project/c/cms-unified-logs/www/public/"
monitor_pub_eos_dir = "/eos/cms/store/unified/www/public/"
monitor_pub_dir = monitor_pub_eos_dir
base_dir =  os.getenv('UNIFIED_DIR','/data/unified/')
#base_eos_dir = "/eos/project/c/cms-unified-logs/"
base_eos_dir = "/eos/cms/store/unified/"


#unified_url = os.getenv('UNIFIED_URL','https://vocms049.cern.ch/unified/')
unified_url = os.getenv('UNIFIED_URL','https://cms-unified.web.cern.ch/cms-unified/')
unified_url_eos = "https://cms-unified.web.cern.ch/cms-unified/"
unified_url = unified_url_eos
url_eos = unified_url_eos
#unified_pub_url = os.getenv('UNIFIED_URL','https://vocms049.cern.ch/unified/public/')
unified_pub_url = os.getenv('UNIFIED_URL','https://cms-unified.web.cern.ch/cms-unified/public/')
cache_dir = '/data/unified-cache/'
mongo_db_url = 'vocms0274.cern.ch'

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
        #elif isinstance(v, list):
        #    d[k]=d.get(k,[])
        #    d[k].extend( v )
        #elif isinstance(v, set):
        #    d[k]=d.get(k,set())
        #    d[k].update( v )
        else:
            d[k] = v
    return d

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
    return _searchLog(q, actor, limit, conn, prefix = '/logs')

def new_searchLog( q, actor=None, limit=50 ):
    conn = httplib.HTTPSConnection( 'es-unified.cern.ch' )
    return _searchLog(q, actor, limit,conn, prefix = '/es/unified-logs/log', h = es_header())

def _searchLog( q, actor, limit, conn, prefix, h = None):

    goodquery={
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "meta": "*%s*"%q
                            }
                        },
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
            "meta",
            #"_id"
            ]
        }


    goodquery={"query": {"bool": {"must": [{"wildcard": {"meta": "*%s*"%q}}]}}, "sort": [{"timestamp": "desc"}], "_source": ["text", "subject", "date", "meta"]}

    if actor:
        #goodquery['query']['bool']['must'][0]['wildcard']['subject'] = actor
        goodquery['query']['bool']['filter'] = { "term" : { "subject" : actor}}

    turl = prefix+'/_search?size=%d'%limit
    print turl
    conn.request("GET" , turl, json.dumps(goodquery) ,headers = h if h else {})
    ## not it's just a matter of sending that query to ES.
    #lq = q.replace(':', '\:').replace('-','\\-')
    #conn.request("GET" , '/logs/_search?q=text:%s'% lq)

    response = conn.getresponse()
    data = response.read()
    o = json.loads( data )

    #print o
    #print o['hits']['total']
    hits =  o['hits']['hits']
    #if actor:
    #    hits = [h for h in hits if h['_source']['subject']==actor]
    return hits

def es_header():
    entrypointname,password = open('Unified/secret_es.txt').readline().split(':')
    import base64
    auth = base64.encodestring(('%s:%s' % (entrypointname, password)).replace('\n', '')).replace('\n', '')
    header = { "Authorization":  "Basic %s"% auth}
    return header

def migrate_ES():
    o_conn = httplib.HTTPConnection( 'cms-elastic-fe.cern.ch:9200' )
    n_conn = httplib.HTTPSConnection( 'es-unified.cern.ch' )

    N = 1000
    f = 0
    total_send = 0
    while False:#True:
        arxn = '/data/es-archive/query_%s_%s.json'%( N, f)
        do_q = True
        total = None
        if os.path.isfile( arxn):
            try:
                #with  open(arxn) as l:
                #    total = json.loads( l.read())['hits']['total']
                #    l.close()
                #    #print total,"is the value of total in cache"
                print arxn,"already there"
                do_q = False
            except:
                pass

        if do_q:
            print "querying"
            o_conn.request('GET','/logs/log/_search?size=%d&from=%d&sort=timestamp:desc&q=timestamp:(<=1511959960)'%( N, f))
            response =o_conn.getresponse()
            data = response.read()

            arx = open(arxn, 'w')
            arx.write( data )
            arx.close()

        if f%(10*N)==0:
            print f,"out of",total
        f+=N

    #return

    ## get all existing ES since a certain date
    ## find out the min time and max time from the new instance
    o_conn.request('GET','/logs/log/_search?size=1&sort=timestamp:desc')
    response =o_conn.getresponse()
    data = json.loads(response.read())
    o_max_date = data['hits']['hits'][0]['_source']['timestamp']

    o_conn.request('GET','/logs/log/_search?size=1&sort=timestamp:asc')
    response =o_conn.getresponse()
    data = json.loads(response.read())
    o_min_date = data['hits']['hits'][0]['_source']['timestamp']

    n_conn.request('GET','/es/unified-logs/log/_search?size=1&sort=timestamp:desc', headers= es_header())
    response =n_conn.getresponse()
    data = json.loads(response.read())
    n_max_date = data['hits']['hits'][0]['_source']['timestamp']

    #print n_max_date
    ## the hard migration happened at time 1512603883.0 ##everything after that is in the new DB

    ## last lower boundary at time of sync 1511952228
    ## current last document age 1511959960
    #n_max_date = 1511959960-1

    n_conn.request('GET','/es/unified-logs/log/_search?size=1&sort=timestamp:asc', headers= es_header())
    response =n_conn.getresponse()
    data = json.loads(response.read())
    n_min_date = data['hits']['hits'][0]['_source']['timestamp']


    ## we should search from now to n_max_date and from n_min_date to 0
    #query = 'q=timestamp:(>=%d OR <=%d)'%( n_max_date, n_min_date)
    #query = 'q=timestamp:(>=%d)'%( n_max_date )
    #query = 'q=timestamp:(<=%d)'%( n_min_date ) ## pick up anything that is older than the oldest one in the new instance ## does not seem to function
    #query = 'q=timestamp:(<=%d)'%( 1511959960 ) ## anything older than a reference time
    query = 'q=timestamp:(<=%d)'%( 1512603883.0 ) ## anything older than a reference time when unified was off and switched over

    print query


    docs = []
    N = 500
    f = 0
    total_send = 0
    while True:
        o_conn.request('GET','/logs/log/_search?size=%d&from=%d&sort=timestamp:desc&%s'%( N, f, query))
        f+=N
        response =o_conn.getresponse()
        data = response.read()
        d = json.loads( data )



        total = d['hits']['total']
        print total,"documents to send over under",query
        docs = d['hits']['hits']
        if not docs: break
        ## copy them over to the new ES
        for doc in docs:
            print "to be send"
            print doc['_id']

            n_conn.request("GET", '/es/unified-logs/log/%s'% doc['_id'], headers = es_header())
            response = n_conn.getresponse()
            data = response.read()
            if not json.loads( data)['found']:
                send_doc = doc['_source']
                encodedParams = urllib.urlencode( send_doc )
                n_conn.request("POST" , '/es/unified-logs/log/%s'% doc['_id'], json.dumps(send_doc), headers = es_header())
                response = n_conn.getresponse()
                data = response.read()
                try:
                    res = json.loads( data )
                    total_send+=1
                except:
                    print "failed to upload", data
            else:
                print doc['_id'],"already existing"

    print total_send,"send"

def new_sendLog( subject, text , wfi = None, show=True, level='info'):
    conn = httplib.HTTPSConnection( 'es-unified.cern.ch' )

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
    #conn.request('GET', "/es/unified-logs", headers = es_header())
    #response = conn.getresponse()
    #data = response.read()
    #print data
    #return

    _try_sendLog( subject, text, wfi, show, level, conn = conn, prefix='/es/unified-logs', h = es_header())

def try_sendLog( subject, text , wfi = None, show=True, level='info'):
    #conn = httplib.HTTPConnection( 'cms-elastic-fe.cern.ch:9200' )
    #_try_sendLog(subject, text , wfi, show, level, conn = conn)

    ## send it to the new instance too. Without showing it
    re_conn = httplib.HTTPSConnection( 'es-unified.cern.ch' )
    _try_sendLog( subject, text, wfi, show, level, conn = re_conn, prefix='/es/unified-logs', h = es_header())


def _try_sendLog( subject, text , wfi = None, show=True, level='info', conn= None, prefix= '/logs', h =None):
    #conn = httplib.HTTPConnection( 'cms-elastic-fe.cern.ch:9200' )

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
    conn.request("POST" , prefix+'/log/', json.dumps(doc), headers = h if h else {})
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
    UC = unifiedConfiguration()

    email_destination = UC.get("email_destination")
    if not destination:
        destination = email_destination
    else:
        destination = list(set(destination+email_destination))
    if not sender:
        map_who = { #'vlimant' : 'vlimant@cern.ch',
                    'mcremone' : 'matteoc@fnal.gov',
                    'qnguyen' : 'thong.nguyen@cern.ch'
                    }
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
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn = make_x509_conn(url)
    r1=conn.request("GET",there)
    r2=conn.getresponse()
    if l:
        return json.loads(r2.read())
    else:
        return r2

def check_ggus( ticket ):
    conn = make_x509_conn('ggus.eu')
    #conn  =  httplib.HTTPSConnection('ggus.eu', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/index.php?mode=ticket_info&ticket_id=%s&writeFormat=XML'%ticket)
    r2=conn.getresponse()
    print r2
    return False

def getSubscriptions(url, dataset):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/phedex/datasvc/json/prod/subscriptions?dataset='+dataset
    r1=conn.request("GET", there)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']
    return items

def listRequests(url, dataset, site=None):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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

def pass_to_dynamo( items, N ,sites = None, group = None ):
    check_N_times = 1
    while True:
        check_N_times-=1
        try:
            return _pass_to_dynamo( items, N, sites, group)
        except Exception as e:
            if check_N_times<=0:
                print "Failed to pass %s to dynamo"% items
                print str(e)
                return False

def _pass_to_dynamo( items, N ,sites = None, group = None ):
    start = time.mktime(time.gmtime())
    if sites == None or sites == []:
        sites = ['T2_*','T1_*_Disk']
    if type(items)==str:
        items = items.split(',')
    conn = make_x509_conn('dynamo.mit.edu')
    par = {'item' : items, 'site': sites, 'n':N}
    if group:
        par.update( {'group' : group })
    #params = urllib.urlencode(par)
    print par
    #print params
    par.update({'cache':'y'})
    conn.request("POST","/registry/request/copy", json.dumps(par))
    response = conn.getresponse()
    data = response.read()
    #print data
    stop = time.mktime(time.gmtime())
    print stop-start,"[s] to hand over to dynamo of",items
    try:
        res = json.loads( data )
        #print json.dumps( res, indent=2)
        return (res['result'] == "OK")
    except Exception as e:
        #if data.replace('\n','') == '':
        #    print "consider blank as OK"
        #    return True
        print "Failed _pass_to_dynamo"
        print "---"
        print data
        print "---"
        print str(e)
        return False

class UnifiedLock:
    def __init__(self, acquire=True):
        self.owner = "%s-%s"%(socket.gethostname(), os.getpid())
        #self.owner = owner
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
        now = time.mktime(time.gmtime())
        from assignSession import session, LockOfLock
        for ll in session.query(LockOfLock).all():
            ## one needs to go and check the process on the corresponding machine
            pass
        session.commit()

    def __del__(self):
        self.release()

    def release(self):
        from assignSession import session, LockOfLock
        for ll in session.query(LockOfLock).filter(LockOfLock.owner == self.owner).all():
            ll.lock = False
            ll.endtime = time.mktime( time.gmtime())
        session.commit()



class DynamoLock:
    def __init__(self, owner=None, wait=True, timeout=None, acquire=True):
        self.owner = owner
        self.go = False
        self.wait = wait
        self.timeout = timeout
        if acquire: self.acquire()

    def acquire(self):
        wait = 30
        waited = 0 
        while True:
            self.go = not self.check()
            if not self.go and self.wait:
                waited += wait
                if self.timeout and waited > self.timeout:
                    break
                time.sleep(wait)
                print "wait on dynamo"
            break

        print "dynamo lock acquired",self.go
        #self.go = lock_DDM(owner=self.owner, wait=self.wait, timeout=self.timeout)

    def free(self):
        return self.go

    def check(self):
        retry = 3
        while retry:
            try:
                return self._check()
            except:
                print "Failed to check on dynamo",retry
                retry-=1
                time.sleep(5)
        return True
                
            
    def _check(self):
        conn = make_x509_conn('dynamo.mit.edu')
        r1 = conn.request("GET",'/data/applock/check?app=detox')
        r2 = conn.getresponse()
        r = json.loads(r2.read())
        if (r['result'] == 'OK' and r['message'] == 'Locked'):
            print "waiting on dynamo",r
            locked = True
        else:
            locked = False
        return locked

    def deadlock(self):
        from assignSession import session, LockOfLock
        Ulocks = session.query(LockOfLock).filter(LockOfLock.lock == True).all()
        if not Ulocks:
            ## noone on this end is currently supposed to handshake with dynamo
            # does not work out 
            self.full_release()

    def __del__(self):
        if self.go: self.release()

    def release(self):
        #unlock_DDM(self.owner)
        pass

    def full_release(self):
        ## release as many times as necessary to get it free
        while self.check():
            self.release()

def unlock_DDM(owner=None):
    try:
        return _lock_DDM(owner=owner, lock=False, wait=None, timeout=None)
    except Exception as e:
        print "Failure in unlocking DDM"
        print str(e)
        return False

def lock_DDM(owner=None, wait=True, timeout=None):
    try:
        return _lock_DDM(owner=owner, lock=True, wait=wait, timeout=timeout)
    except Exception as e:
        print "Failure in locking DDM"
        print str(e)
        return False


def _lock_DDM(owner=None, lock=True, wait=True, timeout=None):
    print "deprecated"
    sys.exit(5)
    return
    conn = make_x509_conn('dynamo.mit.edu')
    go = False
    waited = 0
    sleep = 30
    service = 'unified' ## could be replaced with some owner
    if owner: service+= '-'+owner
    if lock:
        while True:
            conn.request("POST","/registry/applock/lock?service=%s&app=detox"% service )
            response = conn.getresponse()
            data = response.read()
            res = json.loads( data )
            if res['result'].lower() == 'wait':
                time.sleep( sleep )
                waited += sleep
            elif res['result'].lower() == 'ok':
                print "we locked dynamo for",service
                go = True
                break
            else:
                go = False
                print res
                break
            if timeout and waited>timeout:
                print "locking dynamo has timedout for",service
                go = False
    else:
        conn.request("POST","/registry/applock/unlock?service=%s&app=detox"% service)
        response = conn.getresponse()
        data = response.read()
        res = json.loads( data )
        if res['result'].lower() == 'ok':
            print "we unlocked dynamo for",service
            go = True
        else:
            print 'possible deadlock on',service
            print res
            go = True

    return go



class lockInfo:
    def __init__(self, andwrite=True):
        self.owner = "%s-%s"%(socket.gethostname(), os.getpid())
        self.ddmlock = DynamoLock( owner = None, timeout = 10*60)
        self.unifiedlock = UnifiedLock()

    def free(self):
        return self.ddmlock.free()
        
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
        if not item:
            sendEmail('lockInfo', "trying to lock item %s" % item)
            
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


def mongo_client():
    import pymongo,ssl
    return pymongo.MongoClient('mongodb://%s/?ssl=true'%mongo_db_url, ssl_cert_reqs=ssl.CERT_NONE)

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

            
class replacedBlocks:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.replacedBlocks

    def add(self, blocks):
        for block in blocks:
            self.db.update_one({'name' : block},
                               {'$set' : {'name' : block,
                                          'time' : time.mktime( time.gmtime() )
                                      }},
                               upsert=True)
            
    def test(self, block):
        ## return "already replaced"
        b = self.db.find_one({'name' : block})
        return True if b else False
    
class transferDataset:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.transferDataset
        self.added = set()

        ## one time sync
        #for k,v in json.loads(eosRead('/eos/cms/store/unified/datasets_by_phid.json')).items():
        #    self.add(int(k),v)
            
    def add(self, phedexid, datasets):
        self.added.add( phedexid )
        self.db.update({'phedexid' :phedexid},
                       {'$set' : { 
                           'phedexid' :phedexid,
                           'datasets' : datasets}},
                       upsert = True)

    def content(self):
        r = {}
        for t in self.db.find():
            r[t['phedexid']] = t['datasets']
        return r

    def __del__(self):
        if self.added:
            phids = [t['phedexid'] for t in self.db.find()]
            for phid in phids:
                if not phid in self.added:
                    while self.db.find_one({'phedexid' : phid}):
                        self.db.delete_one({'phedexid' : phid})
                        
        
class transferStatuses:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.cachedTransferStatuses

    def pop(self, phedexid):
        self.db.delete_one({'phedexid' : phedexid})

    def add(self, phedexid, status):
        to_insert = copy.deepcopy( status )
        to_insert['phedexid'] = phedexid
        self.db.update_one( {'phedexid' : phedexid},
                            {"$set": to_insert },
                            upsert=True
        )

    def all(self):
        all = self.db.find()
        if all:
            return [d['phedexid'] for d in all ]
        else:
            return []

    def content(self):
        rd = {}
        for d in self.db.find():
            d.pop('_id')
            ii = d.pop('phedexid')
            rd[ii] = dict(d)
        return rd

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
        ## anything older than then => delete
        for o in self.db.find():
            if o['start'] < then:
                print "removing start/stop from",o['_id'],time.asctime(time.localtime( o['start'])), o['component'], o['start']
                self.db.delete_one({'_id' : o['_id']})
                
      


class unifiedConfiguration:
    def __init__(self):
        self.configs = json.loads(open('unifiedConfiguration.json').read()) ## switch to None once you want to read it from mongodb
        if self.configs is None:
            try:
                self.client = mongo_client()
                self.db = self.client.unified.unifiedConfiguration
                quest = self.db.find_one()
            except:
                print "could not reach pymongo"
                self.configs = json.loads(open('unifiedConfiguration.json').read())

    def get(self, parameter):
        if self.configs:
            if parameter in self.configs:
                return self.configs[parameter]['value']
            else:
                print parameter,'is not defined in global configuration'
                print ','.join(self.configs.keys()),'possible'
                sys.exit(124)
        else:
            found = self.db.find_one({"name": parameter})
            if found:
                found.pop("_id")
                found.pop("name")
                return found
            else:
                availables = [o['name'] for o in self.db.find_one()]
                print parameter,'is not defined in mongo configuration'
                print ','.join(availables),'possible'
                sys.exit(124)


def checkDownTime():
    conn = make_x509_conn()
    #conn  =  httplib.HTTPSConnection(reqmgr_url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/jbadillo_BTV-RunIISpring15MiniAODv2-00011_00081_v0__151030_162715_5312')
    r2=conn.getresponse()
    r = r2.read()
    if r2.status ==503:#if 'The site you requested is not unavailable' in r:
        return True
    else:
        return False

def checkMemory():
    ## credits http://fa.bianp.net/blog/2013/different-ways-to-get-memory-consumption-or-lessons-learned-from-memory_profiler/
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
                alarm =  "Timeout in checking the sanity of components %d > %d "%(now-check_start,self.check_timeout)
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
            self.soft = ['mcm','wtc', 'mongo'] ##components that are not mandatory
        else:
            self.soft = soft
        self.block = block
        self.status ={
            'reqmgr' : False,
            'mcm' : False,
            'dbs' : False,
            'phedex' : False,
            'cmsr' : False,
            'wtc' : False,
            'eos' : False,
            'mongo' : False
            }
        self.code = 0
        self.keep_trying = keep_trying
        self.go = False

    def run(self):
        self.go = self.check()
        print "componentCheck finished"

    def check_cmsr(self):
        from assignSession import session, Workflow
        all_info = session.query(Workflow).filter(Workflow.name.contains('1')).all()

    def check_reqmgr(self):
        if 'testbed' in reqmgr_url:
            wfi = getWorkLoad(reqmgr_url,'sryu_B2G-Summer12DR53X-00743_v4_v2_150126_223017_1156')
        else:
            wfi = getWorkLoad(reqmgr_url,'pdmvserv_task_B2G-RunIIWinter15wmLHE-00067__v1_T_150505_082426_497')       
        name = wfi['RequestName']
        tests = getWorkflows(reqmgr_url, 'assignment-approved')

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

    def check_phedex(self):
        if 'testbed' in dbs_url:
            cust = findCustodialLocation(phedex_url,'/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM')
        else:
            cust = findCustodialLocation(phedex_url,'/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM')

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
            r = os.system('rm -f %s'% eosfile)
            if not r == 0:
                raise Exception("failed to I/O on eos")

    def check_mongo(self):
        db = agentInfoDB()
        infos = [a['status'] for a in db.find()]
        
    def check(self):
        ecode = 120
        for component in sorted(self.status):
            ecode+=1
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
        #sendEmail("%s Component Down"%c,"The component is down, just annoying you with this","vlimant@cern.ch",['vlimant@cern.ch','matteoc@fnal.gov'])

def eosRead(filename,trials=5):
    filename = filename.replace('//','/')
    if not filename.startswith('/eos/'):
        print filename,"is not an eos path in eosRead"
        #sys.exit(2)
        #return open(filename).read()
    T=0
    while T<trials:
        T+=1
        try:
            return open(filename).read()
        except Exception as e:
            print "failed to read",filename,"from eos"
            time.sleep(2)
            cache = (cache_dir+'/'+filename.replace('/','_')).replace('//','/')
            r = os.system('cp %s %s'%( filename, cache ))
            if r==0:
                return open(cache).read()
    print "unable to read from eos"
    #sys.exit(2)
    return None
        
class eosFile(object):
    def __init__(self, filename, opt='w'):
        if not filename.startswith('/eos/'):
            print filename,"is not an eos path"
            sys.exit(2)
        self.opt = opt
        self.eos_filename = filename.replace('//','/')
        self.cache_filename = (cache_dir+'/'+filename.replace('/','_')).replace('//','/')
        self.cache = open(self.cache_filename, self.opt)

    def write(self, something):
        self.cache.write( something )
        return self

    def close(self):
        self.cache.close()
        bail_and_email = True
        while True:
            try:
                print "moving",self.cache_filename,"to",self.eos_filename
                r = os.system("cp %s %s"%( self.cache_filename, self.eos_filename))
                if r==0: return True
                print "not able to copy to eos",self.eos_filename,"with code",r
                if bail_and_email:
                    h = socket.gethostname()
                    print 'eos is acting up on %s on %s. not able to copy %s to eos code %s'%( h, time.asctime(), self.eos_filename, r)
                    #sendEmail('eosFile','eos is acting up on %s on %s. not able to copy %s to eos code %s'%( h, time.asctime(), self.eos_filename, r))
                    break

            except Exception as e:
                print "Failed to copy",self.eos_filename,"with",str(e)
                if bail_and_email:
                    h = socket.gethostname()
                    print 'eos is acting up on %s on %s. not able to copy %s to eos \n%s'%( h, time.asctime(), self.eos_filename, stre(e))
                    #sendEmail('eosFile','eos is acting up on %s on %s. not able to copy %s to eos \n%s'%( h, time.asctime(), self.eos_filename, stre(e)))
                    break
                else:
                    time.sleep(30)
        return False

class relvalInfo:
    def __init__(self):
        pass
    def content(self):
        ## dump the campaign dict out
        pass

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

        # one time conversion of the file on eos to mongodb
        #for rv,rvc in json.loads(eosRead('%s/campaigns.relval.json'%base_eos_dir)).items():
        #    self.add( rv, rvc, c_type='relval')

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
        #sendEmail('campaignInfo','removing %s from configuration'% item_name, destination=['vlimant@cern.ch'])
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
                    secs.add( sec )
        return sorted(secs)

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
        #print [l.get('component') for l in locks]
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
        #self.all_locks()
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
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    url = '/wmstatsserver/data/requestcache'
    r1=conn.request("GET",url,headers={"Accept":"application/json"})
    r2=conn.getresponse()
    return json.loads(r2.read())['result'][0]



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

class ThreadHandler(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.threads = args.get('threads', [])
        self.n_threads = args.get('n_threads', 10)
        self.r_threads = []
        self.sleepy = args.get('sleepy',10)
        self.timeout = args.get('timeout',None)
        self.verbose = args.get('verbose',False)
        self.label = args.get('label', 'ThreadHandler')

    def run(self):
        self._run()
        self.threads = self.r_threads

    def _run(self):
        random.shuffle(self.threads)
        ntotal=len(self.threads)
        print "Processing",ntotal,"threads with",self.n_threads,"max concurrent and timeout",self.timeout,'[min]'
        start_now = time.mktime(time.gmtime())
        self.r_threads = []
        

        bug_every=max(len(self.threads) / 10., 100.) ## 10 steps of eta verbosity
        next_ping = int(len(self.threads)/bug_every)
        while self.threads:
            if self.timeout and (time.mktime(time.gmtime()) - start_now) > (self.timeout*60.):
                print '[%s]'%self.label,"Stopping to start threads because the time out is over",time.asctime(time.gmtime())
                for t in self.r_threads:
                    while t.is_alive():
                        time.sleep(5)
                ## transfer all to running
                while self.threads:
                    self.r_threads.append( self.threads.pop(-1))
                ## then we have to kill all threads
                for t in self.r_threads:
                    pass
                return

                
            running = sum([t.is_alive() for t in self.r_threads])
            if self.n_threads==None or running < self.n_threads:
                startme = self.n_threads-running if self.n_threads else len(self.threads)
                if self.verbose or int(len(self.threads)/bug_every)<next_ping:
                    next_ping =int(len(self.threads)/bug_every)
                    now= time.mktime(time.gmtime())
                    spend = (now - start_now)
                    n_done = ntotal-len(self.threads)
                    print '[%s]'%self.label,"Starting",startme,"new threads",len(self.threads),"remaining", time.asctime()
                    if n_done:
                        eta = (spend / n_done) * len(self.threads)
                        print "Will finish in ~%.2f [s]"%(eta)
                if startme > self.n_threads/5.:
                    self.sleepy/=2.
                for it in range(startme):
                    if self.threads:
                        self.r_threads.append( self.threads.pop(-1))
                        self.r_threads[-1].start()
                        ## just wait a sec
                        time.sleep(5)
            time.sleep(self.sleepy)
        ##then wait for completion
        while sum([t.is_alive() for t in self.r_threads]):
            if self.timeout and (time.mktime(time.gmtime()) - start_now) > (self.timeout*60.):
                print '[%s]'%self.label,"Stopping to wait for threads because the time out is over",time.asctime(time.gmtime())
                return
            time.sleep(self.sleepy)
        



class UnifiedBuster(threading.Thread):
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.master = args['master'] ## an locking iterator
        self.log = ""
        self.iterate_me = args["iterate_me"]

    def run(self):
        #pick an item from the master
        while True:
            self.master.lock()
            try:
                item = next(self.iterate_me)
            except StopIteration:
                print "finished"
                self.master.unlock()
                break
            self.master.unlock()

            ## run something on this, giving the output to the master
            try:
                self.operate( item )
            except Exception as e:
                print "Failed in thread"
                print str(e)
                break


    def operate(self, item):
        print "to be overloaded every where"
        pass


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
        self.cache['gwmsmon_pool'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl --retry 5 -s http://cms-gwmsmon.cern.ch/poolview/json/summary').read()),
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
        self.cache['gwmsmon_prod_maxused' ] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : json.loads(os.popen('curl --retry 5 -s http://cms-gwmsmon.cern.ch/prodview//json/maxusedcpus').read()),
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
            'getter' : lambda : getNodesQueue('cmsweb-testbed.cern.ch'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['site_storage'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getSiteStorage('cmsweb.cern.ch'),
            'cachefile' : None,
            'default' : ""
            }
        self.cache['phedex_nodes'] = {
            'data' : None,
            'timestamp' : time.mktime( time.gmtime()),
            'expiration' : default_expiration(),
            'getter' : lambda : getNodesId('cmsweb.cern.ch'),
            'cachefile' : None,
            'default' : ""
            }
        for cat in ['1','2','m1','m3','m4','m5','m6']:
            self.cache['stuck_cat%s'%cat] = {
                'data' : None,
                'timestamp' : time.mktime( time.gmtime()),
                'expiration' : default_expiration(),
                #'getter' : lambda : json.loads( os.popen('curl -s --retry 5 https://test-cmstransfererrors.web.cern.ch/test-CMSTransferErrors/stuck_%s.json'%cat).read()),
                #'getter' : lambda : json.loads( os.popen('curl -s --retry 5 https://cms-stucktransfers.web.cern.ch/cms-stucktransfers/stuck_%s.json'%cat).read()),
                'getter' : lambda : json.loads( os.popen('curl -s --retry 5 http://snarayan.web.cern.ch/snarayan/TransferErrors/stuck_%s.json'%cat).read()),
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
    tries = 5 
    while tries>0:
        tries-=1
        try:
            return _getNodes(url, kind)
        except Exception as e:
            pass
    print str(e)

def _getNodes(url, kind):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/nodes')
    r2=conn.getresponse()
    result = json.loads(r2.read())
    return [node['name'] for node in result['phedex']['node'] if node['kind']==kind]

def getNodeUsage(url, node):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/nodeusage?node=%s'%node)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    if len(result['phedex']['node']):
        s= max([sum([node[k] for k in node.keys() if k.endswith('_node_bytes')]) for node in result['phedex']['node']])
        return int(s / 1023.**4) #in TB
    else:
        return None

def getNodeQueue(url, node):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/nodeusagehistory?node=%s'%node)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    print result
    missing = 0
    if len(result['phedex']['node']):
        for node in result['phedex']['node']:
            for usage in node['usage']:
                missing += int(usage['miss_bytes'] / 1023.**4) #in TB
        return missing
    return None

def getSiteStorage(url):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/sitedb/data/prod/data-processing', headers={"Accept":"*/*"})
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

    def get(self, dataset , blocks=None):
        return self.get_size( dataset , blocks=blocks)

    def get_size(self, dataset, blocks=None):
        self._get( dataset )
        if blocks:
            ## sum over the specified blocks
            return sum( [self.bdb[dataset].get(b,0.) for b in blocks ])
        else:
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
    def __init__(self, override_good = None):

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
                if not siteInfo['Tier'] in [0,1,2,3]: continue
                self.all_sites.append( siteInfo['VOName'] )
                override = (override_good and siteInfo['VOName'] in override_good)
                if siteInfo['VOName'] in self.sites_banned and not override:
                    continue
                if (self.sites_ready_in_agent and siteInfo['VOName'] in self.sites_ready_in_agent) or override:
                    self.sites_ready.append( siteInfo['VOName'] )
                elif self.sites_ready_in_agent and not siteInfo['VOName'] in self.sites_ready_in_agent:
                    self.sites_not_ready.append( siteInfo['VOName'] )
                elif siteInfo['Status'] == 'enabled':
                    self.sites_ready.append( siteInfo['VOName'] )
                else:
                    self.sites_not_ready.append( siteInfo['VOName'] )

            ##over-ride those since they are only handled through jobrouting
            add_as_ready = [
                'T3_US_OSG',
                'T3_US_Colorado',
                'T3_CH_CERN_HelixNebula',
                'T3_CH_CERN_HelixNebula_REHA',
                'T3_US_NERSC',
                'T3_US_TACC',
                'T3_US_PSC'
                            ]
            for aar in add_as_ready:
                if not aar in self.sites_ready:
                    self.sites_ready.append(aar)
                if not aar in self.all_sites:
                    self.all_sites.append(aar)



        except Exception as e:
            print "issue with getting SSB readiness"
            print str(e)
            sendEmail('bad sites configuration','falling to get any sites')
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
                           'T3_US_Colorado'
        ]
        add_on_aaa = list(set(add_on_good_aaa + add_on_aaa))
        self.sites_AAA = list(set(self.sites_AAA + add_on_aaa ))

        ## could this be an SSB metric ?
        self.sites_with_goodIO = UC.get('sites_with_goodIO')
        #restrict to those that are actually ON
        self.sites_with_goodIO = [s for s in self.sites_with_goodIO if s in self.sites_ready]
        ## those of the above that can be actively targetted for transfers
        #allowed_T2_for_transfer = ["T2_DE_RWTH","T2_DE_DESY",
                                          #not inquired# "T2_ES_CIEMAT",
                                          #no space# ##"T2_FR_GRIF_IRFU", #not inquired# ##"T2_FR_GRIF_LLR", #not inquired"## "T2_FR_IPHC",##not inquired"## "T2_FR_CCIN2P3",
        #                                  "T2_IT_Legnaro", "T2_IT_Pisa", "T2_IT_Rome", "T2_IT_Bari",
        #                                  "T2_UK_London_Brunel", "T2_UK_London_IC", "T2_UK_SGrid_RALPP",
        #                                  "T2_US_Nebraska","T2_US_Wisconsin","T2_US_Purdue","T2_US_Caltech", "T2_US_Florida", "T2_US_UCSD", "T2_US_MIT",
        #                                  "T2_BE_IIHE",
        #                                  "T2_EE_Estonia",
        #                                  "T2_CH_CERN", "T2_CH_CERN_HLT",

        #                           'T2_RU_INR',
        #                           'T2_UA_KIPT'
        #                                  ]

        # restrict to those actually ON
        #allowed_T2_for_transfer = [s for s in allowed_T2_for_transfer if s in self.sites_ready]

        ## first round of determining the sites that veto transfer
        #self.sites_veto_transfer = [site for site in self.sites_with_goodIO if not site in allowed_T2_for_transfer]
        self.sites_veto_transfer = []  ## do not prevent any transfer by default

        ## new site lists for better matching
        self.sites_with_goodAAA = self.sites_with_goodIO + add_on_good_aaa
        self.sites_with_goodAAA = list(set([ s for s in self.sites_with_goodAAA if s in self.sites_ready]))


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
            #print phn,psn,"have a special setting"
            self.addHocStorage[psn] = phn

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
                self.storage[mss]  = max(0,mss_usage['Tape'][use_field][mss])

            if mss in sites_space_override:
                self.storage[mss] = sites_space_override[mss]


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

    def sites_low_pressure(self, ratio):
        sites = [site for site,(matching,running,_) in self.sites_pressure.items() if (running==0 or (matching/float(running))< ratio) and site in self.sites_ready]
        return sites

    def sitesByArch(self, arch ):
        if not self.sites_memory:
            print "not enough information about glidein mon"
            return None
        rel6=(arch.startswith('slc6'))
        rel7=(arch.startswith('slc7'))
        allowed = set()
        for site,slots in self.sites_memory.items():
            for slot in slots:
                if slot['OS'] == 'any' or slot['SINGULARITY'] == True:
                    ## should match anything
                    allowed.add( site )
                elif rel7 and slot['OS'] == 'rhel7':
                    allowed.add( site )
                elif rel6 and slot['OS'] == 'rhel6':
                    allowed.add( site )

        return list(allowed)

    def sitesByArchs(self,archs):
        by_archs = {}
        for arch in set(archs):
            by_archs[arch] = self.sitesByArch(arch)
        final = None
        for arch,sites in by_archs.items():
            if final is None:
                final = set(sites)
            else:
                final = final & set(sites)

        return list(final)

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
            queued = self.queue.get(site,0)
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

class remainingDatasetInfo:
    def __init__(self):
        self.client = mongo_client()
        self.db = self.client.unified.remainingDatasetInfo
    
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

def isHEPCloudReady(url, limit=20):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr2/data/request?mask=RequestStatus&status=assigned&status=acquired&status=running-open&status=running-closed&team=hepcloud', headers={"Accept":"*/*"})
    r2=conn.getresponse()
    d = json.loads(r2.read())['result']
    ok=limit
    if len(d):
        ok = max(0,limit-len(d[0].keys()))
    return ok


def global_SI(a=None):
    if a or not global_SI.instance:
        print "making a new instance of siteInfo",a
        global_SI.instance = siteInfo(a)
    return global_SI.instance
global_SI.instance = None



class closeoutInfo:
    def __init__(self):
        self.owner = "%s-%s"%( socket.gethostname(), os.getpid())
        self.removed_keys = set()
        self.client = mongo_client()
        self.db = self.client.unified.closeoutInfo
        self.record = {}
        #print len([o for o in self.db.find()])

        ### one time sync
        #olddb = json.loads(eosRead('/eos/cms/store/unified/closedout.json'))
        #for k,v in olddb.items():
        #    self.update(k,v)

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
        text = '<table border=1><thead><tr><th>workflow</th><th>OutputDataSet</th><th>%Compl</th><th>acdc</th><th>Dupl</th><th>LSsize</th><th>Scubscr</th><th>dbsF</th><th>dbsIF</th><th>\
phdF</th><th>Updated</th><th>Priority</th></tr></thead>'
        return text

    def one_line(self, wf, wfo, count):
        if count%2:            color='lightblue'
        else:            color='white'
        text=""
        _ = self.get( wf ) ## cache the value
        tpid = self.record[wf]['prepid']
        pid = tpid.replace('task_','')

        ## return the corresponding html
        order = ['percentage','acdc','duplicate','correctLumis','missingSubs','dbsFiles','dbsInvFiles','phedexFiles']#,'updated']
        wf_and_anchor = '<a id="%s">%s</a>'%(wf,wf)
        n_out = len(self.record[wf]['datasets'])
        o_and_c = [(out,self.record[wf]['datasets'][out].get('percentage')) for out in self.record[wf]['datasets'] ]
        o_and_c.sort( key = lambda o : o[1])
        #for io,out in enumerate(self.record[wf]['datasets']):
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
            #text+='<td>%s</td>'% out
            lines = []
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
                else:
                    text+='<td>%s</td>'% value
            u_text = '<td rowspan="%d">%s</td>'%( n_out, self.record[wf]['datasets'][out]['updated'])
            if io==0: text+=u_text
            p_text = '<td rowspan="%d">%s</td>'%( n_out, self.record[wf]['priority'])
            if io==0: text+=p_text
            ###text+='<td>%s</td>'%self.record[wf]['priority']
            text+='</tr>'
            wf_and_anchor = wf

        return text

    def html(self):
        self.summary()
        self.assistance()

    def summary(self):


        #html = open('%s/closeout.html'%monitor_dir,'w')
        html = eosFile('%s/closeout.html'%monitor_dir,'w')
        html.write('<html>')
        html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/ target=_blank> logs</a> <br><br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))

        html.write( self.table_header() )

        from assignSession import session, Workflow
        #for (count,wf) in enumerate(sorted(self.record.keys())):
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
        os.system('touch %s'%my_file)
        #out = open(my_file, 'w')
        #out.write( json.dumps( self.record , indent=2 ) )
        #out.write( my_file )
        #out.close()

        ## write the information out to disk
        os.system('cp %s/closedout.json %s/closedout.json.last'%(base_eos_dir, base_eos_dir))

        ## merge the content
        try:
            old = json.loads(open('%s/closedout.json'%base_eos_dir).read())
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

        out = open('%s/closedout.json'%base_eos_dir,'w')
        out.write( json.dumps( self.record , indent=2 ) )
        out.close()
        time.sleep(100)

        os.system('rm -f %s'% my_file )

    def assistance(self):
        from assignSession import session, Workflow
        wfs = session.query(Workflow).filter(Workflow.status.startswith('assistance')).all()

        #short_html = open('%s/assistance_summary.html'%monitor_dir,'w')
        #html = open('%s/assistance.html'%monitor_dir,'w')
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
<li> <b>announce</b> : the final statistics of the sample is enough to announce the outputs <font color=green>(Automatic)</font> </li>
<li> <b>announced</b> : the final statistics of the sample is enough and the outputs are announced <font color=green>(Automatic)</font> </li>
<li> <b>over100</b> : the final statistics is over 100%% <font color=red>(Operator)</font></li>
<li> <b>biglumi</b> : the maximum size of the lumisection in one of the output has been exceeded <font color=red>(Operator)</font></li>
<li> <b>bigoutput</b> : the maximum size for an output dataset to go to tape was exceeded (<font color=blue>Requester</font>/<font color=red>Operator)</font></li>
<li> <b>filemismatch</b> : there is a mismatch in the number of files in DBS and Phedex <font color=red>(Operator)</font></li>
<li> <b>duplicates</b> : duplicated lumisection have been found and need to be invalidated <font color=green>(Automatic)</font></li>
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
            lines = []
            short_lines = []
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

        # close and put on eos
        html.close()
        short_html.close()


def checkTransferApproval(url, phedexid):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    try:
        return try_findLostBlocksFiles(url, datasetname)
    except:
        sendLog('findLostBlocksFiles','fatal execption in findLostBlocksFiles for %s, assuming no lost blocks and files'% datasetname, level='critical')
        return ([],[])

def try_findLostBlocksFiles(url, datasetname):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

    r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?block=%s%%23*&collapse=n'% datasetname)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    lost = []
    lost_files = []
    for dataset in result['phedex']['dataset']:
        for item in dataset['block']:
            if item['is_open'] == 'y' : continue # skip those
            exist=0
            for loc in item['subscription']:
                exist = max(exist, loc['percent_bytes'])
            if not exist:
                print "We have lost:",item['name']
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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    except Exception as e:
        print str(e)
        try:
            time.sleep(1)
            v = try_checkTransferStatus(url, xfer_id, nocollapse)
        except Exception as e:
            print str(e)
            #sendEmail('fatal exception in checkTransferStatus %s'%xfer_id, str(e))
            sendLog('checkTransferStatus','fatal exception in checkTransferStatus %s\n%s'%(xfer_id, str(e)), level='critical')
            v = {}
    return v


def getNodesId(url):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/nodes?')
    r2=conn.getresponse()
    result = json.loads(r2.read())
    nodes = {}
    for node in result['phedex']['node']:
        nodes[node['name']] = node['id']
    return nodes

def try_checkTransferStatus(url, xfer_id, nocollapse=False):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

    r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?request='+str(xfer_id))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    timecreate=min([r['time_create'] for r in result['phedex']['request']])
    phedex_nodes = dataCache.get('phedex_nodes')
    my_nodes = [ phedex_nodes[n['name']] for n in result['phedex']['request'][0]['node']]
    subs_url = '/phedex/datasvc/json/prod/subscriptions?request=%s&create_since=%d&%s'%(str(xfer_id),timecreate, '&'.join(['node=%s'%n for n in my_nodes]))
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
        subs_url = '/phedex/datasvc/json/prod/subscriptions?request=%s&create_since=%d&%s'%(str(xfer_id),0, '&'.join(['node=%s'%n for n in my_nodes]))
        if nocollapse:
            subs_url+='&collapse=n'
        r1=conn.request("GET",subs_url)
        r2=conn.getresponse()
        result = json.loads(r2.read())
        print result

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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    tries = 5 
    while tries>0:
        tries-=1
        try:
            return _getDatasetFiles(url, dataset ,without_invalid)
        except Exception as e:
            pass
    print str(e)

def _getDatasetFiles(url, dataset ,without_invalid=True ):
    dbsapi = DbsApi(url=dbs_url)
    files = dbsapi.listFileArray( dataset= dataset,validFileOnly=without_invalid, detail=True)
    dbs_filenames = [f['logical_file_name'] for f in files]

    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

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

    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

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



def getBetterDatasetDestinations( url, dataset, only_blocks=None, group=None, vetoes=None, within_sites=None, complement=True):
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
    else:
        full_size = sum([block['file_size'] for block in all_blocks])

    if not full_size:
        print dataset,"is nowhere"
        return {}, all_block_names

    print len(all_block_names),"blocks"

    items = None
    while items == None:
        try:
            conn = make_x509_conn(url)
            #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            url = '/phedex/datasvc/json/prod/requestlist?dataset=%s'%dataset
            if group:
                url+='group=%s'%group
            r1=conn.request("GET",url)
            r2=conn.getresponse()

            items=json.loads(r2.read())['phedex']['request']
        except Exception as e :
            print "\twaiting a bit for retry"
            print e
            time.sleep(1)


    deletes = {}
    now = time.mktime(time.gmtime())
    for item in items:
        phedex_id = item['id']
        if item['type']!='delete': continue
        #if item['approval']!='approved': continue
        for node in item['node']:
            stamp = int(node['time_decided']) if node['time_decided'] else now
            #if node['decision'] != 'approved': continue ## a delete is a delete
            if not node['name'] in deletes or deletes[node['name']]< stamp:
                ## add it if not or later delete
                deletes[node['name']] = stamp

    destinations = defaultdict(set)
    all_destinations = set()
    for item in items:
        phedex_id = item['id']
        if item['type']=='delete': continue
        for req in item['node']:
            if within_sites and not req['name'] in within_sites: continue
            if vetoes and any([req['name'].endswith(v) for v in vetoes]): continue
            if not req['time_decided']: continue
            if not req['decision'] in ['approved']: continue

            if req['name'] in deletes and int(req['time_decided'])< deletes[req['name']]:
                continue

            all_destinations.add( req['name'] )
    #print sorted( all_destinations)


    if only_blocks:
        ## do something special when there is a block whitelist
        for block in only_blocks:
            try:
                url='/phedex/datasvc/json/prod/subscriptions?block=%s&collapse=n'%( block.replace('#','%%23'))
                r1=conn.request("GET",url)
                r2=conn.getresponse()
                r = r2.read()
            except Exception as e:
                print str(e)
                raise Exception(e)
            result = json.loads(r)['phedex']['dataset']
            for ds in result:
                for block in ds['block']:
                    for sub in block['subscription']:
                        site = sub['node']
                        if within_sites and not site in within_sites: continue
                        if vetoes and any([site.endswith(v) for v in vetoes]): continue
                        sub['percent_bytes'] = sub['percent_bytes'] if sub['percent_bytes']!=None else 0
                        destinations[site].add( (block['name'], sub['percent_bytes'], sub['request']))
        pass

    else:
        ## check first by full dataset
        print "getting all d-sub for dataset",dataset
        r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?dataset=%s'%(dataset))
        r2=conn.getresponse()
        result = json.loads(r2.read())['phedex']['dataset']

        in_full = set()
        for ds in result:
            for sub in ds['subscription']:
                phedex_id = sub['request']
                site = sub['node']
                if within_sites and not site in within_sites: continue
                ## it is there and in full
                if vetoes and any([site.endswith(v) for v in vetoes]): continue
                for b in all_block_names:
                    sub['percent_bytes'] = sub['percent_bytes'] if sub['percent_bytes']!=None else 0
                    destinations[site].add( (b, sub['percent_bytes'], phedex_id) )
                in_full.add( site )

        #print sorted(all_destinations)
        print "in full at",sorted(in_full)
        ## then check block by block at the site not already OK.
        for site in (all_destinations-in_full):
            if not site.startswith('T'): continue
            print "getting all b-sub for dataset",dataset,'@',site
            try:
                url='/phedex/datasvc/json/prod/subscriptions?block=%s%%23*&node=%s&collapse=n'%(dataset, site)
                #print "querying all block at",site,dataset,url
                r1=conn.request("GET",url)
                r2=conn.getresponse()
                r = r2.read()
            except Exception as e :
                ## addhoc to try and move forward
                if False and dataset == '/Neutrino_E-10_gun/RunIISpring15PrePremix-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v2-v2/GEN-SIM-DIGI-RAW':
                    continue
                else:
                    raise Exception(e)

            #print r
            result = json.loads(r)['phedex']['dataset']
            for ds in result:
                for block in ds['block']:
                    for sub in block['subscription']:
                        print sub['percent_bytes']
                        sub['percent_bytes'] = sub['percent_bytes'] if sub['percent_bytes']!=None else 0
                        destinations[site].add( (block['name'], sub['percent_bytes'], sub['request']))


    #for site in destinations:
    #    destinations[site] = list( destinations[site])
    #print json.dumps( destinations, indent=2)

    re_destinations = {}
    for site in destinations:
        blocks = [b[0] for b in destinations[site]]
        blocks_and_id = dict([(b[0],b[2]) for b in destinations[site]])
        #print blocks_and_id
        completion = sum([b[1] for b in destinations[site]]) / float(len(destinations[site]))

        re_destinations[site] = { "blocks" : blocks_and_id,
                               #"all_blocks" : list(all_block_names),
                               "data_fraction" : len(blocks) / float(len(all_block_names)) ,
                               "completion" : completion,
                               }
    return re_destinations, all_block_names

def getOldDatasetDestinations( url, dataset, only_blocks=None, group=None, vetoes=None, within_sites=None, complement=True):
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

    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
            conn = make_x509_conn(url)
            #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset=%s'%dataset)
            r2=conn.getresponse()
        except:
            print "\twaiting a bit for retry"
            time.sleep(1)
            conn = make_x509_conn(url)
            #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
            if req['name']=='T2_CH_CERN': print "yes to cern"

            if within_sites and not req['name'] in within_sites: continue
            #if req['decision'] != 'approved' : continue
            if not req['time_decided']: continue
            if req['name'] in deletes and int(req['time_decided'])< deletes[req['name']]:
                ## this request is void by now
                continue
            if not req['decision'] in ['approved']:
                continue
            if req['name'] in times:
                #if req['name']=='T2_CH_CERN': print times[req['name']]
                if int(req['time_decided']) > times[req['name']]:
                    ## the node was already seen as a destination with an ealier time, and no delete in between
                    if req['name']=='T2_CH_CERN': print "uhm",phedex_id
                    continue
            if req['name']=='T2_CH_CERN': print phedex_id,"is being registered for CERN destination"
            times[req['name']] = int(req['time_decided'])


            sites_destinations.append( req['name'] )
        sites_destinations = [site for site in sites_destinations if not any(site.endswith(v) for v in vetoes)]
        ## what are the sites for which we have missing information ?
        sites_missing_information = [site for site in sites_destinations if site not in destinations.keys()]
        print phedex_id,sites_destinations,"fetching for missing",sites_missing_information

        addhoc = (dataset == '/Neutrino_E-10_gun/RunIISpring15PrePremix-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v2-v2/GEN-SIM-DIGI-RAW')
        if len(sites_missing_information)==0 and not addhoc: continue

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


def getDatasetDestinations( url, dataset, only_blocks=None, group=None, vetoes=None, within_sites=None, complement=True):
    try:
        return getBetterDatasetDestinations(url, dataset, only_blocks, group, vetoes, within_sites, complement)
    except Exception as e:
        print "failed once"
        print str(e)
        sendLog('getDatasetDestinations','Failed the new implementation', level='critical')
        try:
            return getOldDatasetDestinations(url, dataset, only_blocks, group, vetoes, within_sites, complement)
        except:
            print "failed twice, and crash"
            raise Exception("getDatasetDestinations crashed")

def getDatasetOnGoingDeletion( url, dataset ):
    tries = 5 
    while tries>0:
        tries-=1
        try:
            return _getDatasetOnGoingDeletion(url, dataset)
        except Exception as e:
            pass
    print str(e)

def _getDatasetOnGoingDeletion( url, dataset ):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/deletions?dataset=%s&complete=n'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())['phedex']
    return result['dataset']

def getDatasetBlocks( dataset, runs=None, lumis=None):
    dbsapi = DbsApi(url=dbs_url)
    all_blocks = set()
    if runs == []:
        for r in runs:
            all_blocks.update([item['block_name'] for item in dbsapi.listBlocks(run_num=r, dataset= dataset) ])
    if lumis:
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

        #needs a series of convoluted calls
        #all_blocks.update([item['block_name'] for item in dbsapi.listBlocks( dataset = dataset )])
        pass
    elif runs:
        for run in runs:
            #try:
            #    all_files = dbsapi.listFileArray( dataset = dataset, run_num=int(run), detail=True)
            #except Exception as e:
            #    print "Exception in listFileArray",str(e)
            #    all_files = []
            #all_blocks.update( [f['block_name'] for f in all_files])
            all_blocks.update([b['block_name'] for b in dbsapi.listBlocks(dataset = dataset, run_num = int(run))])

    if runs==None and lumis==None:
        all_blocks.update([item['block_name'] for item in dbsapi.listBlocks(dataset= dataset) ])

    return list( all_blocks )

def getUnsubscribedBlocks(url, site):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    
    #protect prune
    for s in blocks_at_sites:
        pbs = [ b for b in blocks_at_sites[s] if b.startswith(dataset)]
        if len(pbs) != len(blocks_at_sites[s]):
            sendEmail('getDatasetBlockAndSite','phedex is acting up %s %s'%(dataset, sorted(blocks_at_sites[s])), destination=['bmaier@mit.edu','natasha@fnal.gov','Dmytro.Kovalskyi@cern.ch'])
        blocks_at_sites[s] = set(pbs)

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


def getDatasetBlocksFromFiles( dataset, files):
    dbsapi = DbsApi(url=dbs_url)
    all_files = dbsapi.listFiles( dataset = dataset , detail=True, validFileOnly=1)
    collected_blocks = set()
    for fn in all_files:
        if fn['logical_file_name'] in files:
            collected_blocks.add( fn['block_name'] )
    return sorted(collected_blocks)

def tcWrapper( *argv, **args):
    label = argv[0]
    func = argv[1]
    argv = argv[1:]

def getDatasetBlockSize(dataset):
    count=5
    label = "getDatasetBlockSize"
    while count>0:
        try:
            return _getDatasetBlockSize(dataset)
        except Exception as e:
            print "[%d] Failed on %s with \n%s"%(count,label,str(e))
            time.sleep(5)
    raise Exception("Failed on %s with \n%s"%( label,str(e)))


def _getDatasetBlockSize(dataset):
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
        sizes.extend( [block['file_size'] for block in filter(lambda b : b['file_size'] > chop_threshold, blocks)] )
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
                print len(sizes)
                print len(items)
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
        SI = global_SI()

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
    try:
        reply = dbsapi.listFiles(dataset=dataset, detail=True, run_num=run, validFileOnly=1) if run!=1 else dbsapi.listFiles(dataset=dataset, detail=True,validFileOnly=1)
    except:
        try:
            reply = dbsapi.listFiles(dataset=dataset, detail=True, run_num=run, validFileOnly=1) if run!=1 else dbsapi.listFiles(dataset=dataset, detail=True,validFileOnly=1)
        except:
            sendLog('getFilesWithLumiInRun','Fatal exception in running dbsapi.listFiles for %s %s '% (dataset,run), level='critical')
            reply = []

    #print time.mktime(time.gmtime())-start,'[s]'
    files = [f['logical_file_name'] for f in reply if f['is_file_valid'] == 1]
    start = 0
    bucket = 100
    rreply = []
    while True:
        these = files[start:start+bucket]
        if len(these)==0: break
        rreply.extend( dbsapi.listFileLumiArray(logical_file_name=these,run_num=run) if run!=1 else dbsapi.listFileLumiArray(logical_file_name=these))
        start+=bucket
        #print len(rreply)
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


def getDatasetLumisAndFiles(dataset, runs=None, lumilist=None, with_cache=False,force=False):
    if runs and lumilist:
        print "should not be used that way"
        return {},{}
    lumis =set()
    if lumilist:
        for r in lumilist:
            lumis.update( [(r,l) for l in lumilist[r]])

    now = time.mktime(time.gmtime())
    dbsapi = DbsApi(url=dbs_url)
    c_name= '%s/.%s.lumis.json'%(cache_dir,dataset.replace('/','_'))
    #print os.path.isfile(c_name),with_cache
    if os.path.isfile(c_name):
        print "picking up from cache",c_name
        try:
            opened = json.loads(open(c_name).read())
        except:
            opened = {}
            with_cache = False

        if 'time' in opened:
            record_time = opened['time']
            if (now-record_time)<(0.5*60*60): ## 0 ?
                with_cache=True ## if the record is less than 1 hours, it will get it from cache
        else:
            with_cache = False ## force new caches
        if force: with_cache=False

        ## need to filter on the runs
        if with_cache and 'lumis' in opened and 'files' in opened:
            lumi_json = dict([(int(k),v) for (k,v) in opened['lumis'].items()])
            files_json = dict([(tuple(map(int,k.split(":"))),v) for (k,v) in opened['files'].items()])
            if runs:
                lumi_json = dict([(int(k),v) for (k,v) in opened['lumis'].items() if int(k) in runs])
                files_json = dict([(tuple(map(int,k.split(":"))),v) for (k,v) in opened['files'].items() if int(k.split(':')[0]) in runs])
            elif lumilist:
                runs = map(int(lumilist.keys()))
                lumi_json = dict([(int(k),v) for (k,v) in opened['lumis'].items() if int(k) in runs])
                files_json = dict([(tuple(map(int,k.split(":"))),v) for (k,v) in opened['files'].items() if map(int,k.split(":")) in lumis])

            print "return from cache"
            return lumi_json,files_json
        else:
            print "old cache. re-querying"
    print "querying getDatasetLumisAndFiles", dataset
    #print c_name
    full_lumi_json = defaultdict(set)
    files_per_lumi = defaultdict(set) ## the revers dictionnary of files by r:l
    d_runs = getDatasetRuns( dataset )
    #print len(runs),"runs"
    class getFilesWithLumiInRun_t(threading.Thread):
        def __init__(self, d,r):
            threading.Thread.__init__(self)
            self.d =d
            self.r =r
        def run(self):
            self.res = getFilesWithLumiInRun( self.d, self.r)

    threads = []
    for run in d_runs:
        threads.append( getFilesWithLumiInRun_t( dataset, run))
        threads[-1].start()
    while sum([t.is_alive() for t in threads]):
        pass
    for t in threads:
        if not hasattr(t,'res'):
            print "not good to not have a result from the thread"
            continue
        for f in t.res:
            full_lumi_json[t.r].update( f['lumi_section_num'] )
            for lumi in f['lumi_section_num']:
                files_per_lumi[(t.r,lumi)].add( f['logical_file_name'] )

    """
    for run in d_runs:
        files = getFilesWithLumiInRun( dataset, run )
        #print run,len(files),"files"
        for f in files:
            full_lumi_json[run].update( f['lumi_section_num'] )
            for lumi in f['lumi_section_num']:
                files_per_lumi[(run,lumi)].add( f['logical_file_name'] )

    """

    ## convert set->list and for a run list
    lumi_json = {}
    files_json = {}
    for r in full_lumi_json:
        full_lumi_json[r] = list(full_lumi_json[r])
        if runs and not r in runs: continue
        if lumilist:
            lumi_json[r] = list(full_lumi_json[r] & lumilist.get(r, set()))
        else:
            lumi_json[r] = list(full_lumi_json[r])
    for rl in files_per_lumi.keys():
        conv = list(files_per_lumi.pop(rl))
        if runs:
            if rl[0] in runs: files_json[rl] = conv
        elif lumis:
            if rl in lumis: files_json[rl] = conv
        else:
            files_json[rl] = conv
        files_per_lumi['%d:%d'%(rl)] = conv


    try:
        open(c_name,'w').write( json.dumps(
                {'lumis' : dict(full_lumi_json),
                 'files' : dict(files_per_lumi),
                 'time' : now}
                , indent=2))
    except:
        print "could not write the cache file out"

    return dict(lumi_json),dict(files_json)


def getDatasetLumis(dataset, runs=None, with_cache=False):
    l,f = getDatasetLumisAndFiles(dataset, runs=runs, lumilist=None, with_cache=with_cache)
    return l
"""
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
    files_per_lumi = defaultdict(set) ## the revers dictionnary of files by r:l
    d_runs = getDatasetRuns( dataset )
    #print len(runs),"runs"
    for run in d_runs:
        files = getFilesWithLumiInRun( dataset, run )
        #print run,len(files),"files"
        for f in files:
            full_lumi_json[run].update( f['lumi_section_num'] )
            for lumi in f['lumi_section_num']:
                files_per_lumi[(run,lumi)].add( f['logical_file_name'] )

    ## convert set->list and for a run list
    lumi_json = {}
    files_json = {}
    for r in full_lumi_json:
        full_lumi_json[r] = list(full_lumi_json[r])
        if runs and not r in runs: continue
        lumi_json[r] = list(full_lumi_json[r])
    for rl in files_per_lumi:
        if runs and rl[0] in runs:
            files_json[rl] = list(files_per_lumi[rl])
        files_per_lumi['%d:%d'%(rl)] = list(files_per_lumi.pop(rl))


    open(c_name,'w').write( json.dumps(
            cache_store = {'lumis' : dict(full_lumi_json),
                           'files' : dict(files_per_lumi)}
            , indent=2))
    #open(c_name,'w').write( json.dumps( dict(full_lumi_json), indent=2))

    return dict(lumi_json),dict(files_json)
"""
def getDatasetListOfFiles(dataset):
    dbsapi = DbsApi(url=dbs_url)
    all_files = dbsapi.listFileArray( dataset = dataset, detail=False)
    all_lfn = sorted([f['logical_file_name'] for f in all_files])
    return all_lfn

def getDatasetAllEventsPerLumi(dataset, fraction=1):
    dbsapi = DbsApi(url=dbs_url)
    all_files = dbsapi.listFileArray( dataset = dataset ,detail=True)
    if fraction!=1:
        ## truncate if need be
        random.shuffle( all_files )
        all_files = all_files[:int(fraction*len(all_files)+1)]

    result = []
    final = {}
    for f in all_files:
        final[f['logical_file_name']] = [f['event_count'],0]
    chunking = 900
    for chunk in [all_files[x:x+chunking] for x in xrange(0, len(all_files), chunking)]:
        ls = dbsapi.listFileLumiArray( logical_file_name = [f['logical_file_name'] for f in chunk])
        for l in ls:
            final[l['logical_file_name']][1]+= len(l['lumi_section_num'])



    return    [a/float(b) for (a,b) in final.values()]

def getDatasetEventsPerLumi(dataset):
    all_values = getDatasetAllEventsPerLumi(dataset)
    if all_values:
        return sum(all_values) / float(len(all_values))
    else:
        return 1.
    ## the thing below does not actually work
    #dbsapi = DbsApi(url=dbs_url)
    #try:
    #    all_files = dbsapi.listFileSummaries( dataset = dataset , validFileOnly=1)
    #except:
    #    print "We had to have a DBS listfilesummaries retry"
    #    time.sleep(1)
    #    all_files = dbsapi.listFileSummaries( dataset = dataset , validFileOnly=1)
    #try:
    #    average = sum([f['num_event']/float(f['num_lumi']) for f in all_files]) / float(len(all_files))
    #except:
    #    average = 100
    #return average

def invalidateFiles( files ):
    all_OK = True
    #conn = make_x509_conn('dynamo.mit.edu')
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

def checkParent( dataset ):
    dbsapi = DbsApi(url=dbs_url)
    dbswrite = DbsApi(url=dbs_url_writer)

    ## get the parent dataset
    parents = findParent(dataset)
    if len(parents)>1:
        print "this is clearly a pb"
        return False

    parents = parents[:1]
    p_f = {}
    for parent in parents:
        _,p_f[parent] = getDatasetLumisAndFiles(parent)
    ## the list of lumis - file
    _,f = getDatasetLumisAndFiles(dataset)
    checked = {}
    file_parent_fixing = defaultdict(set)
    print len(f),"lumis to check"
    icount=0
    for rl,fns in f.items():
        icount+=1
        if icount%50==0: print icount,'/',len(f)
        for fn in fns:
            ### get their parents
            file_parents = set()
            ret = checked.get(fn , dbsapi.listFileParents( logical_file_name = fn ))
            for r in ret:
                file_parents.update( r.get('parent_logical_file_name',[]))

            ## if none, make an alarm and fix it
            if not file_parents:
                print "from DBS",fn,"has no parent"
                for parent in parents:
                    file_parents.update(p_f[parent].get(rl, []))
                print "\t found",sorted(file_parents)
            if not file_parents:
                ## this is really an issue and new lumisections were made
                print rl,"is really problematic in",dataset
                print "not present in",parents
            else:
                ### dbs api to write the parent configuration
                file_parent_fixing[fn].update(file_parents)

    #for fn,file_parents in file_parent_fixing.items():
    #    print "Fixing DBS is underway for ",fn
    #    #print "Found",sorted(file_parents),"as parents"
    #    print "Found",len(file_parents),"as parents in",parents[0]
    #    ## that API does not exists
    #    dbswrite.updateFile( logical_file_name = fn,
    #                         dataset = dataset,
    #                         parent_logical_file_name = list(file_parents),
    #                         parent_dataset = parents[0]
    #                         )
    #    pass


def findParent( dataset ):
    dbsapi = DbsApi(url=dbs_url)
    print dataset,"for parent"
    ret = dbsapi.listDatasetParents( dataset= dataset)
    parents = [r.get('parent_dataset',None) for r in ret]
    return parents

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


def setDatasetStatus(dataset, status, withFiles=True):
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


def injectFile(url, info):
    ## do a sub parsing per site
    per_site = defaultdict( lambda : defaultdict( lambda: defaultdict( list)))
    for dataset in info:
        for block in info[dataset]:
            for file_o in info[dataset][block]:
                per_site[file_o.pop("site")][dataset][block].append( file_o )
    for site in per_site:
        x = createFileXML( per_site[site] )
        params = { "node" : site,
                   "data" : x }
        #print x
        #print params
        r = phedexPost(url, '/phedex/datasvc/json/prod/inject', params)
        print json.dumps( r , indent=2)


def createFileXML(dataset_block_file_locations):
    impl=getDOMImplementation()
    doc=impl.createDocument(None, "data", None)
    result = doc.createElement("data")
    result.setAttribute('version', '2')
    dbs = doc.createElement("dbs")
    dbs.setAttribute("name", dbs_url)
    result.appendChild(dbs)
    for dataset in dataset_block_file_locations:
        xdataset=doc.createElement("dataset")
        ### check these directly in phedex?
        xdataset.setAttribute("is-open","y")
        #xdataset.setAttribute("is-transient","y")
        xdataset.setAttribute("name",dataset)
        dbs.appendChild(xdataset)
        for block in dataset_block_file_locations[dataset]:
            xblock = doc.createElement("block")
            ## check these in phedex?
            xblock.setAttribute("is-open","y")
            xblock.setAttribute("name", block)
            xdataset.appendChild(xblock)
            for file_o in dataset_block_file_locations[dataset][block]:
                xfile = doc.createElement("file")
                for k,v in file_o.items():
                    ## should be name, bytes, checksum
                    xfile.setAttribute(k,str(v))
                    xblock.appendChild(xfile)
    return result.toprettyxml(indent="  ")


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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
        conn = make_x509_conn(url)
        #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
        conn = make_x509_conn(url)
        #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    try:
        return _getWorkLoad(url, wf )
    except:
        time.sleep(5)
        try:
            return _getWorkLoad(url, wf )
        except:
            print "failed twice to _getWorkLoad(url, wf )"
            return None

def _getWorkLoad(url, wf ):
    conn = make_x509_conn(url)
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

def getConfigurationFile(url , cacheid):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/couchdb/reqmgr_config_cache/%s/configFile'% cacheid
    r1=conn.request("GET",there)
    r2=conn.getresponse()
    return r2.read()

def getConfigurationLine(url, cacheid, token="# with command line"):
    cfg = getConfigurationFile(url, cacheid)
    for line in cfg.split('\n'):
        if line.startswith(token): return line
    return None

def getWorkflowByCampaign(url, campaign, details=False):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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


def getWorkflowByInput( url, dataset , details=False):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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


def getLatestMCPileup( url, statuses=None):
    if not statuses:
        statuses = ['assigned','acquired','running-open','running-closed','force-complete','completed','closed-out','announced',
                    #'normal-archived'
                    ]

    """
    ss = '&'.join(['status=%s'% s for s in statuses])
    print ss
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr2/data/request?mask=RequestDate&mask=MCPileup&%s'%ss, headers={"Accept":"application/json"})
    r2=conn.getresponse()
    data = json.loads(r2.read())
    reqs = data['result']
    """

    reqs = []
    for status in statuses:
        print status
        conn = make_x509_conn(url)
        #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr2/data/request?mask=RequestDate&mask=MCPileup&status=%s'%status, headers={"Accept":"application/json"})
        r2=conn.getresponse()
        data = json.loads(r2.read())

        reqs.extend( data['result'] )

    those = defaultdict(set)
    for req in reqs:
        for v in req.values():
            t = v.get('MCPileup',None)
            d = v.get('RequestDate',[])
            if t and len(d)==6:
                d =time.mktime(time.strptime("-".join(map(lambda n : "%02d"%int(n), d)), "%Y-%m-%d-%H-%M-%S"))
                for tt in t:
                    those[tt].add( d )
    ret = {}
    for dataset,ages in those.items():
        ret[dataset] = max(ages)
    return ret

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

def getWorkflowByMCPileup( url, dataset , details=False):
    retries=5
    while retries>0:
        retries-=1
        try:
            return _getWorkflowByMCPileup(url, dataset , details)
        except Exception as e:
            pass
    print str(e)
    
def _getWorkflowByMCPileup( url, dataset , details=False):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    import reqMgrClient
    familly = wfi.getFamilly( and_self=True, only_resub=only_resub)
    outs = set()
    check = []
    for fwl in familly:
        check.append(reqMgrClient.invalidateWorkflow(url, fwl['RequestName'], current_status=fwl['RequestStatus'], cascade=False))
        outs.update( fwl['OutputDatasets'] )
    if with_output:
        for dataset in outs:
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
        #print "got from pymongo",self.all_agents
        self.buckets = defaultdict(list)
        self.drained = set()
        self.wake_draining = False ## do not wake up agents that are on drain already
        self.release = defaultdict(set)
        self.m_release = defaultdict(set)
        self.ready = self.getStatus()
        if not self.ready:
            print "AgentInfo could not initialize properly"
        #print json.dumps(self.buckets, indent=2)
        print json.dumps(self.content(), indent=2)

    def content(self):
        ## same as 
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
        ## works for inserting
        put_info = self._getA(agent)
        put_info.update( info )
        put_info['name'] = agent
        self.db.update_one( {'name': agent},
                            {"$set": put_info},
                            upsert = True)

    def agentStatus(self, agent):
        return self._get(agent, 'status', 'N/A')
        #for status in self.buckets:
        #    if agent in self.buckets[status]:
        #        return status
        #return 'N/A'

    def checkTrello(self, sync_trello=None, sync_agents=None, acting=False):
        from TrelloClient import TrelloClient
        if not hasattr(self, 'tc'):
            self.tc = TrelloClient()
        tc = self.tc

        now,nows = self.getNow()

        for agent in self.all_agents:
            #print "checking on",agent
            astatus = self._get(agent,'status')
            ti = tc.getCard( cn = agent)
            lid = tc.lists.get( astatus )
            #print agent,lid,astatus,tc.lists
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

            #print agent,lid,clid



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
                    #st = 'draining'
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
            #sendEmail('agentInfo', msg)

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
                #if len(standbies)<=1 and (cp <= cpu_running*self.speed_draining_fraction) and (r <= t*self.speed_draining_fraction) and (cr <= cpu_running*self.speed_draining_fraction):
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
                #elif agent_name in self.release[oldest_release]:
                #elif not agent_name in self.release[top_release]:
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
                #sendEmail('agentInfo', msg)
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
                #sendEmail('agentInfo', msg)

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
            #sendLog('agentInfo',msg, level='critical')
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

def getAgentConfig(url, agent, keys):
    conn = make_x509_conn(url)
    go_url= '/reqmgr2/data/wmagentconfig/%s'% agent
    r1=conn.request("GET",go_url, headers={"Accept":"application/json"})
    r2=conn.getresponse()
    info = json.loads(r2.read())['result'][-1]
    return dict([(k,v) for k,v in info.items() if k in keys])

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

def addAgentNoRetry(url, agent, error_codes):
    info = getAgentConfig(url, agent, ['NoRetryExitCodes'])
    already = info['NoRetryExitCodes']
    print "Already in noretry",sorted( already )
    already.extend( error_codes )
    already = list(set(already))
    return sendAgentConfig(url, agent, {'NoRetryExitCodes' : already})

def setAgentDrain(url, agent, drain=True):
    #the agent name has to have .cern.ch and all
    info = getAgentConfig(url, agent, ['UserDrainMode'])
    draining = info['UserDrainMode']
    print agent,"is draining?",draining

    r = sendAgentConfig(url, agent, {"UserDrainMode":drain})
    return r

def setAgentOn(url, agent):
    return setAgentDrain(url, agent, drain=False)


def getAgentInfo(url, agent):
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    url= '/couchdb/wmstats/%s%%3A9999'%agent
    r1=conn.request("GET",url)
    r2=conn.getresponse()
    return json.loads(r2.read())["WMBS_INFO"]#["sitePendCountByPrio"]

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
    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

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

    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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

    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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

def getBlockLocations(url, dataset, group=None):
    conn = make_x509_conn(url)
    go= '/phedex/datasvc/json/prod/blockreplicas?dataset=%s'%(dataset)
    if group:
        go+='&group=%s'%group
    r1=conn.request("GET",go)
    r2=conn.getresponse()    
    result = json.loads(r2.read())['phedex']
    locations = defaultdict(set)
    for block in result['block']:
        for rep in block['replica']:
            locations[block['name']].add( rep['node'])
    return dict(locations)
    

def checkIfBlockIsAtASite(url,block,site):

    conn = make_x509_conn(url)
    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicasummary?block='+block.replace('#','%23'))
    r2=conn.getresponse()

    result = json.loads(r2.read())

    assert(len(result['phedex']['block']) == 1)

    for replica in result['phedex']['block'][0]['replica']:
        if replica['node'] == site and replica['complete'] == 'y':
            return True

    return False

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
    def sync(self):

        force = getForceCompletes()
        for user,items in force.items():
            for item in items:
                print user, item
                self.add( action='force', keyword=item, user = user)
                

        UC = unifiedConfiguration()
        actors = UC.get('allowed_bypass')
        
        for bypassor,email in actors:
            bypass_file = '/afs/cern.ch/user/%s/%s/public/ops/bypass.json'%(bypassor[0],bypassor)
            if not os.path.isfile(bypass_file):
                continue
            try:
                print "Can read bypass from", bypassor
                extending = json.loads(open(bypass_file).read())
                print bypassor,"is bypassing",json.dumps(sorted(extending))
                for ex in extending:
                    self.add( action = 'bypass' , keyword = ex, user = bypassor)
            except:
                pass
        
            holding_file = '/afs/cern.ch/user/%s/%s/public/ops/onhold.json'%(bypassor[0],bypassor)
            if not os.path.isfile(holding_file):
                continue
            try:
                extending = json.loads(open(holding_file).read())
                print bypassor,"is holding",json.dumps(sorted(extending))
                for ex in extending:
                    self.add( action = 'hold' , keyword = ex, user = bypassor)
            except:
                pass

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

def getForceCompletes():
    overrides = {}
    UC = unifiedConfiguration()
    actors = UC.get('allowed_bypass')
    for rider,email in actors:
        rider_file = '/afs/cern.ch/user/%s/%s/public/ops/forcecomplete.json'%(rider[0],rider)
        if not os.path.isfile(rider_file):
            continue
        try:
            extending = json.loads(open( rider_file ).read() )
            print rider,"is force-completing",sorted(extending)
            overrides[rider] = extending
        except:
            print "cannot get force complete list from",rider
            sendEmail("malformated force complet file","%s is not json readable"%rider_file, destination=[email])
    return overrides


class workflowInfo:
    def __init__(self, url, workflow, spec=True, request=None,stats=False, wq=False, errors=False):
        self.logs = defaultdict(str)
        self.url = url
        self.conn = make_x509_conn(self.url)
        #self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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




    #def getFamiolly(self, details=True, and_self=False):
    #    return self.getFamilly(details, and_self)

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

    def checkSettings(self):
        ## open a mystery file
        perfs = json.loads(open('%s/perf_per_config.json'% base_eos_dir))
        ## and check the observed value and the settings of the workflow

    def isRelval(self):
        if 'SubRequestType' in self.request and 'RelVal' in self.request['SubRequestType']:
            return True
        else:
            return False

    def isGoodForNERSC(self, no_step=False):
        nersc_archs=set(['slc6_amd64_gcc530','slc6_amd64_gcc630'])
        good = (self.request['RequestType'] == 'StepChain' or no_step)  and self.request['RequestPriority'] <= 85000 and len(set(self.request['ScramArch'])&nersc_archs)>=1
        io = _,prim,_,sec = self.getIO()
        if self.heavyRead(): good=False
        if prim: good = False
        #if sec: good = False
        ## should be of significant size. how do we check that ???
        #good = good &
        return good

    def isGoodToConvertToStepChain(self ,keywords=None, talk=False):
        ## only one value throughout the chain
        all_same_cores = len(set(self.getMulticores()))==1
        ##make sure not tow same data tier is produced
        all_tiers = map(lambda o : o.split('/')[-1], self.request['OutputDatasets'])
        #single_tiers = (len(all_tiers) == len(set(all_tiers)))
        single_tiers = True
        ## more than one task with output until https://github.com/dmwm/WMCore/issues/8269 gets solved
        #output_per_task = self.getOutputPerTask()
        #output_from_single_task = len(output_per_task.keys())==1
        output_from_single_task = True ## the parentage 
        ## more than one task to not convert single task in a step
        #more_than_one_task = wfi.request.get('TaskChain',0)>1
        more_than_one_task = True
        ## so that conversion happens only for a selected few
        found_in_transform_keywords = True
        wf = self.request['RequestName']
        if keywords:
            found_in_transform_keywords = any([keyword in wf for keyword in keywords])
        good = self.request['RequestType'] == 'TaskChain' and more_than_one_task and found_in_transform_keywords and single_tiers and all_same_cores and output_from_single_task
        if not good and talk:
            #print more_than_one_task
            #print found_in_transform_keywords
            #print "single_tiers
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
            #self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
            print "failed get dashboard"
            print str(e)
            pass
        return self.dashb

    def getWMStats(self ,cache=0):
        trials = 10
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
        return None

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

    def getRecoveryBlocks(self ,collection_name=None):
        doc = self.getRecoveryDoc(collection_name=collection_name)
        all_files = set()
        files_and_loc = defaultdict(set)
        for d in doc:
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
        for f in all_files:
            try:
                if not f.startswith('/store/unmerged/') and not f.startswith('MCFakeFile-'):
                    r = dbsapi.listFileArray( logical_file_name = f, detail=True)
                else:
                    r = []
            except Exception as e:
                print "dbsapi.listFileArray failed on",f
                print str(e)
                continue

            if not r:
                files_no_block.add( f)
            else:
                files_in_block.add( f )
                all_blocks.update( [df['block_name'] for df in r ])
                for df in r:
                    all_blocks_loc[df['block_name']] . update( files_and_loc.get( f, []))
        dataset_blocks = set()
        for dataset in set([block.split('#')[0] for block in all_blocks]):
            print dataset
            dataset_blocks.update( getDatasetBlocks( dataset ) )

        files_and_loc = dict([(k,list(v)) for (k,v) in files_and_loc.items() if k in files_no_block])
        return dataset_blocks,all_blocks_loc,files_in_block,files_and_loc#files_no_block

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
            #self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
            #print task,doc['files'].keys()
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
            #print pu['mc'][block]
            ret.update( pu['mc'][block]['PhEDExNodeNames'])
            for site in pu['mc'][block]['PhEDExNodeNames']:
                count_blocks[site]+=pu['mc'][block]['NumberOfEvents']
            if intersection:
                intersection = intersection & set(pu['mc'][block]['PhEDExNodeNames'])
            else:
                intersection = set(pu['mc'][block]['PhEDExNodeNames'])

        max_blocks = max( count_blocks.values())
        #print json.dumps(count_blocks, indent=2)
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
                    self.workqueue = list([d['doc'] for d in json.loads(r2.read())['rows']])
                except Exception as e:
                    self.conn = make_x509_conn(self.url)
                    time.sleep(1) ## time-out
                    print "Failed to get workqueue"
                    print str(e)
                    self.workqueue = []
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

        self.conn = make_x509_conn(self.url)
        #self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
    def heavyRead(self):
        ## this is an add-hoc way of doing this. True by default. False if "premix" appears in the output datasets or in the campaigns
        response = True
        if any(['premix' in c.lower() for c in self.getCampaigns()]):
            response = False
        if any(['premix' in o.lower() for o in self.request['OutputDatasets']]):
            response = False
        return response

    def getSiteWhiteList( self, pickone=False, verbose=True):
        ### this is not used yet, but should replace most
        SI = global_SI()
        (lheinput,primary,parent,secondary) = self.getIO()
        sites_allowed=[]
        if lheinput:
            sites_allowed = sorted(SI.sites_eos) #['T2_CH_CERN'] ## and that's it
        elif secondary:
            if self.heavyRead():
                sites_allowed = sorted(set(SI.sites_T0s + SI.sites_T1s + SI.sites_with_goodIO))
            else:
                sites_allowed = sorted(set(SI.sites_T0s + SI.sites_T1s + SI.sites_with_goodAAA))
        elif primary:
            sites_allowed =sorted(set(SI.sites_T0s + SI.sites_T1s + SI.sites_T2s))# + SI.sites_T3s))
        else:
            # no input at all
            ## all site should contribute
            sites_allowed =sorted(set( SI.sites_T0s + SI.sites_T2s + SI.sites_T1s))# + SI.sites_T3s ))
        if pickone:
            sites_allowed = sorted([SI.pick_CE( sites_allowed )])

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

        #ncores = self.request.get('Multicore',1)
        ncores = self.getMulticore()
        mem = self.getMemory()
        memory_allowed = SI.sitesByMemory( mem , maxCore=ncores)
        if memory_allowed!=None:
            if verbose:
                print "sites allowing",mem,"MB and",ncores,"core are",sorted(memory_allowed)
            ## mask to sites ready for mcore
            if  ncores>1:
                memory_allowed = list(set(memory_allowed) & set(SI.sites_mcore_ready))
            sites_allowed = list(set(sites_allowed) & set(memory_allowed))

        ## check on CC7 compatibility
        archs = self.getArchs()
        arch_allowed = SI.sitesByArchs( archs )
        if not arch_allowed is None:
            sites_allowed = list(set(arch_allowed) & set(sites_allowed))
            print "Reducing the whitelist to sites allowing",archs,":",sorted(arch_allowed)

        return (lheinput,primary,parent,secondary,sites_allowed)

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

                #print tname,ncores
                #print GB_space_limit
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
                    ## climb up all task to take the filter eff into account
                    efficiency_factor = 1.
                    while t and 'InputTask' in t:
                        t = find_task_dict( t['InputTask'] )
                        if 'FilterEfficiency' in t:
                            efficiency_factor *= t['FilterEfficiency']
                    events_per_lumi_at_this_task = events_per_lumi * efficiency_factor

                    #if (events_per_lumi_at_this_task > avg_events_per_job):
                    #    print "The default splitting will not work for subsequent steps",events_per_lumi,"[in the input dataset] amounts to",events_per_lumi_at_this_task,"[at this task]>",avg_events_per_job,"[splitting for the task]"
                    #    #max_events_per_lumi.append( int(avg_events_per_job*0.75) ) ##reducing
                    #    max_events_per_lumi.append( int(avg_events_per_job*0.75) ) ##reducing


                    if timeperevent:
                        job_timeout = 45. ## hours
                        job_target = 8. ## hours
                        time_per_input_lumi = events_per_lumi_at_this_task*timeperevent
                        if (time_per_input_lumi > (job_timeout*60*60)): ##45h
                            ## even for one lumisection, the job will time out.
                            print "The running time of task %s is expected to be too large even for one lumi section: %d x %.2f s = %.2f h > %d h"%( tname,
                                                                                                                                                     events_per_lumi_at_this_task, timeperevent,
                                                                                                                                                     time_per_input_lumi / (60.*60.),
                                                                                                                                                     job_timeout)
                            this_max_events_per_lumi = int( job_target*60.*60. / timeperevent)
                            max_events_per_lumi.append( this_max_events_per_lumi /efficiency_factor ) ## report here, so that if we can change it, we will change this
                        else:
                            pass


                    if sizeperevent:# and (avg_events_per_job * sizeperevent ) > (GB_space_limit*1024.**2):
                        size_per_input_lumi = events_per_lumi_at_this_task*sizeperevent
                        this_max_events_per_lumi = int( (GB_space_limit*1024.**2) / sizeperevent)
                        if (size_per_input_lumi > (GB_space_limit*1024.**2)):
                            ## derive a value for the lumisection
                            print "The output size task %s is expected to be too large : %.2f GB > %f GB even for one lumi (effective lumi size is ~%d), should go as low as %d"% ( tname ,
                                                                                                                                                size_per_input_lumi / (1024.**2 ),
                                                                                                                                                GB_space_limit,
                                                                                                                                                events_per_lumi_at_this_task,
                                                                                                                                                this_max_events_per_lumi)
			    sendLog('assignor', "The output size task %s is expected to be too large : %.2f GB > %f GB even for one lumi (effective lumi size is ~%d), should go as low as %d"% ( tname ,
                                                                                                                                                size_per_input_lumi / (1024.**2 ),
                                                                                                                                                GB_space_limit,
                                                                                                                                                events_per_lumi_at_this_task,
                                                                                                                                                this_max_events_per_lumi), level='critical')
                            max_events_per_lumi.append( this_max_events_per_lumi/efficiency_factor ) ## adding this to that later on we can check and adpat the split 0
                        elif (avg_events_per_job * sizeperevent ) > (GB_space_limit*1024.**2):
                            ## should still change the avg_events_per_job setting of that task
                            print "The output size of task %s is expected to be too large : %d x %.2f kB = %.2f GB > %f GB. Should set as low as %d "% ( tname ,
                                                                                                                                                         avg_events_per_job, sizeperevent,
                                                                                                                                                         avg_events_per_job * sizeperevent / (1024.**2 ),
                                                                                                                                                         GB_space_limit,
                                                                                                                                                         this_max_events_per_lumi)
			    sendLog('assignor', 'The output size of task %s is expected to be too large : %d x %.2f kB = %.2f GB > %f GB. Should set as low as %d'%( tname ,
                                                                                                                                                         avg_events_per_job, sizeperevent,
                                                                                                                                                         avg_events_per_job * sizeperevent / (1024.**2 ),
                                                                                                                                                         GB_space_limit,
                                                                                                                                                         this_max_events_per_lumi), level='critical')
                            modified_split_for_task = spl
                            modified_split_for_task['splitParams']['events_per_job'] = this_max_events_per_lumi
                            modified_splits.append( modified_split_for_task )
                            max_events_per_lumi.append( this_max_events_per_lumi/efficiency_factor ) ## adding this to that later on we can check and adpat the split 0

            if max_events_per_lumi:
                if events_per_lumi_inputs:
                    if min(max_events_per_lumi)<events_per_lumi_inputs:
                        ## there was an input dataset somewhere and we cannot break down that lumis, except by changing to EventBased
                        print "the smallest value of %s is still smaller than %s evt/lumi of the input dataset"%(max_events_per_lumi, events_per_lumi_inputs)
			sendLog('assignor', 'the smallest value of %s is still smaller than %s evt/lumi of the input dataset'%(max_events_per_lumi, events_per_lumi_inputs), level='critical')
                        hold = True
                    else:
                        #hold = True #to be removed
                        print "the smallest value of %s is ok compared to %s evt/lumi in the input"%(max_events_per_lumi, events_per_lumi_inputs)
                else:
                    root_split = splits[0]
                    current_split = root_split.get('splitParams',{}).get('events_per_lumi',None)
                    if current_split and current_split > min(max_events_per_lumi):
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
        #new_schema = copy.deepcopy( self.get_spec().request.schema.dictionary_())

        self.conn = make_x509_conn(self.url)
        #self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
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
            #print t._internal_name
            #print "what",t.subscriptions
            parse_what = t.subscriptions.outputModules if hasattr(t.subscriptions,'outputModules') else t.subscriptions.outputSubs
            if parse_what:
                for om in parse_what:
                    dsname = getattr(t.subscriptions, om).dataset
                    if dsname in all_outputs: ## do the intersection with real outputs
                        #print dsname
                        #print t._internal_name
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

    def getSplittingsNew(self ,strip=False):
        self.conn = make_x509_conn(self.url)
        #self.conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=self.conn.request("GET",'/reqmgr2/data/splitting/%s'%self.request['RequestName'], headers={"Accept":"application/json"} )
        r2=self.conn.getresponse()
        result = json.loads( r2.read() )['result']
        splittings = []
        for spl in result:
            if not spl['taskType'] in ['Production','Processing','Skim'] : continue
            if strip:
                for drop in ['algorithm','trustPUSitelists','trustSitelists','deterministicPileup','type','include_parents','lheInputFiles','runWhitelist','runBlacklist','collectionName','group','couchDB','couchURL','owner','initial_lfn_counter','filesetName', 'runs','lumis']:
                    if drop in spl['splitParams']:
                        spl['splitParams'].pop(drop)
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
            #acqEra = self._collectinchain('AcquisitionEra', func=invertDigits)
            acqEra = self._collectinchain('AcquisitionEra')
        else:
            #acqEra = invertDigits(self.request['AcquisitionEra'])
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
                    print "checking against",predicted
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

