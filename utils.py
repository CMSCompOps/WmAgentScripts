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

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email.utils import make_msgid


FORMAT = "%(module)s.%(funcName)s(%(lineno)s) => %(message)s (%(asctime)s)"
DATEFMT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format = FORMAT, datefmt = DATEFMT, level=logging.DEBUG)


def sendEmail( subject, text, sender=None, destination=None ):
    #print subject
    #print text
    #print sender
    #print destination
    
    if not destination:
        destination = ['vlimant@cern.ch','matteoc@fnal.gov']
    else:
        destination = list(set(destination+['vlimant@cern.ch','matteoc@fnal.gov']))
    if not sender:
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

import dataLock

class lockInfo:
    def __init__(self):
        pass

    def __del__(self):
        if random.random() < 0.5:
            print "Cleaning locks"
            #self.clean_block()
            self.clean_unlock()
            print "... cleaning locks done"

        jdump = {}
        for l in dataLock.locksession.query(dataLock.Lock).all():
            ## don't print lock=False ??
            #if not l.lock: continue 
            site= l.site
            if not site in jdump: jdump[site] = {}
            jdump[site][l.item] = { 
                'lock' : l.lock,
                'time' : l.time,
                'date' : time.asctime( time.gmtime(l.time) ),
                'reason' : l.reason
                }
        now = time.mktime(time.gmtime())
        jdump['lastupdate'] = now
        open('/afs/cern.ch/user/c/cmst2/www/unified/datalocks.json','w').write( json.dumps( jdump , indent=2))

    def _lock(self, item, site, reason):
        print "[lock] of %s at %s because of %s"%( item, site, reason )
        now = time.mktime(time.gmtime())
        l = dataLock.locksession.query(dataLock.Lock).filter(dataLock.Lock.site == site).filter(dataLock.Lock.item == item).first()
        if not l:
            l = dataLock.Lock(lock=False)
            l.site = site
            l.item = item
            l.is_block = '#' in item
            dataLock.locksession.add ( l )
        if l.lock:
            print l.item,item,"already locked at",site
        
        ## overwrite the lock 
        l.reason = reason
        l.time = now
        l.lock = True
        dataLock.locksession.commit()

    def lock(self, item, site, reason):
        try:
            self._lock( item, site, reason)
        except Exception as e:
            ## to be removed once we have a fully functional lock db
            print "could not lock",item,"at",site
            print str(e)
            
    def _release(self, item, site, reason='releasing'):
        print "[lock release] of %s at %s because of %s" %( item, site, reason)
        now = time.mktime(time.gmtime())
        # get the lock on the item itself
        l = dataLock.locksession.query(dataLock.Lock).filter(dataLock.Lock.site==site).filter(dataLock.Lock.item==item).first()
        if not l:
            print item,"was not locked at",site
            l = dataLock.Lock(lock=False)
            l.site = site
            l.item = item
            l.is_block = '#' in item
            dataLock.locksession.add ( l )
            dataLock.locksession.commit()
            
        #then unlock all of everything starting with item (itself for block, all block for dataset)
        for l in dataLock.locksession.query(dataLock.Lock).filter(dataLock.Lock.site==site).filter(dataLock.Lock.item.startswith(item)).all():
            l.time = now
            l.reason = reason
            l.lock = False
        dataLock.locksession.commit()

    def release_except(self, item, except_site, reason='releasing'):
        print "[lock release] of %s except at %s because of %s" %( item, except_site, reason)
        try:
            for l in dataLock.locksession.query(dataLock.Lock).filter(dataLock.Lock.item.startswith(item)).all():
                site = l.site
                if not site in except_site:
                    self._release(l.item, site, reason)
                else:
                    print "We are told to not release",item,"at site",site,"per request of",except_site
        except Exception as e:
            print "could not unlock",item,"everywhere but",except_site
            print str(e)

    def release_everywhere(self, item, reason='releasing'):
        print "[lock release] of %s everywhere because of %s" % ( item, reason )
        try:
            for l in dataLock.locksession.query(dataLock.Lock).filter(dataLock.Lock.item.startswith(item)).all():
                site = l.site
                self._release(item, site, reason)
        except Exception as e:
            print "could not unlock",item,"everywhere"
            print str(e)

    def release(self, item, site, reason='releasing'):
        print "[lock release] of %s at %s because of %s" %( item, site, reason)
        try:
            self._release(item, site, reason)
        except Exception as e:
            print "could not unlock",item,"at",site
            print str(e)

    def tell(self, comment):
        print "---",comment,"---"
        for l in dataLock.locksession.query(dataLock.Lock).all():
            print l.item,l.site,l.lock
        print "------"+"-"*len(comment)

    def clean_block(self, view=False):
        print "start cleaning lock info"
        ## go and remove all blocks for which the dataset is specified
        # if the dataset is locked, nothing matters for blocks -> remove
        # if the dataset is unlocked, it means we don't want to keep anything of it -> remove
        clean_timeout=0
        for l in dataLock.locksession.query(dataLock.Lock).filter(dataLock.Lock.lock==True).filter(dataLock.Lock.is_block==False).all():
            site = l.site
            item = l.item
            if view:  print item,"@",site
            ## get all the blocks at that site, for that dataset, under the same condition
            for block in dataLock.locksession.query(dataLock.Lock).filter(dataLock.Lock.site==site).filter(dataLock.Lock.item.startswith(item)).filter(dataLock.Lock.is_block==True).all():
                print "removing lock item for",block.item,"at",site,"due to the presence of overriding dataset information"
                dataLock.locksession.delete( block )
                clean_timeout+=1
                if view: print block.name
                if clean_timeout> 10:                    break
            dataLock.locksession.commit()
            if clean_timeout> 10:                break

    def clean_unlock(self, view=False):
        clean_timeout=0
        ## go and remove all items that have 'lock':False
        for l in dataLock.locksession.query(dataLock.Lock).filter(dataLock.Lock.lock==False).all():
            print "removing lock=false for",l.item,"at",l.site
            dataLock.locksession.delete( l )            
            clean_timeout+=1
            #if clean_timeout> 10:                break

        dataLock.locksession.commit()

class unifiedConfiguration:
    def __init__(self):
        self.configs = json.loads(open('/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/unifiedConfiguration.json').read())
        
    def get(self, parameter):
        if parameter in self.configs:
            return self.configs[parameter]['value']
        else:
            print parameter,'is not defined in global configuration'
            print ','.join(self.configs.keys()),'possible'
            sys.exit(124)

class componentInfo:
    def __init__(self, block=True, mcm=False,soft=None):
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
        #if not self.check():
        #    sys.exit( self.code)
        

    def check(self):
        try:
            print "checking reqmgr"
            wfi = workflowInfo('cmsweb.cern.ch','pdmvserv_task_B2G-RunIIWinter15wmLHE-00067__v1_T_150505_082426_497')
            self.status['reqmgr'] = True
        except Exception as e:
            self.tell('reqmgr')
            print "cmsweb.cern.ch unreachable"
            print str(e)
            if self.block and not (self.soft and 'reqmgr' in self.soft):
                self.code = 123
                return False

        from McMClient import McMClient

        if self.mcm:
            try:
                mcmC = McMClient(dev=False)
                print "checking mcm"
                test = mcmC.getA('requests',page=0)
                time.sleep(1)
                if not test: 
                    self.tell('mcm')
                    print "mcm corrupted"
                    if self.block and not (self.soft and 'mcm' in self.soft):
                        self.code = 124
                        return False
                else:
                    self.status['mcm'] = True
            except Exception as e:
                self.tell('mcm')
                print "mcm unreachable"
                print str(e)
                if self.block and not (self.soft and 'mcm' in self.soft):
                    self.code = 125
                    return False
        
        try:
            print "checking dbs"
            dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
            blocks = dbsapi.listBlockSummaries( dataset = '/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM', detail=True)
            if not blocks:
                self.tell('dbs')
                print "dbs corrupted"
                if self.block and not (self.soft and 'dbs' in self.soft):
                    self.code = 126
                    return False

        except Exception as e:
            self.tell('dbs')
            print "dbs unreachable"
            print str(e)
            if self.block and not (self.soft and 'dbs' in self.soft):
                self.code = 127
                return False

        try:
            print "checking phedex"
            cust = findCustodialLocation('cmsweb.cern.ch','/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM')
            
        except Exception as e:
            self.tell('phedex')
            print "phedex unreachable"
            print str(e)
            if self.block and not (self.soft and 'phedex' in self.soft):
                self.code = 128
                return False

        return True

    def tell(self, c):
        sendEmail("%s Component Down"%c,"The component is down, just annoying you with this","vlimant@cern.ch",['vlimant@cern.ch','matteoc@fnal.gov'])

class campaignInfo:
    def __init__(self):
        #this contains accessor to aggreed campaigns, with maybe specific parameters
        self.campaigns = json.loads(open('/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/campaigns.json').read())
        SI = siteInfo()
        for c in self.campaigns:
            if 'parameters' in self.campaigns[c]:
                if 'SiteBlacklist' in self.campaigns[c]['parameters']:
                    for black in copy.deepcopy(self.campaigns[c]['parameters']['SiteBlacklist']):
                        if black.endswith('*'):
                            self.campaigns[c]['parameters']['SiteBlacklist'].remove( black )
                            reg = black[0:-1]
                            self.campaigns[c]['parameters']['SiteBlacklist'].extend( [site for site in (SI.all_sites) if site.startswith(reg)] )
                            #print self.campaigns[c]['parameters']['SiteBlacklist']
                            
    def go(self, c):
        if c in self.campaigns and self.campaigns[c]['go']:
            return True
        else:
            print "Not allowed to go for",c
            return False
    def parameters(self, c):
        if c in self.campaigns and 'parameters' in self.campaigns[c]:
            return self.campaigns[c]['parameters']
        else:
            return {}

def duplicateLock(component=None):
    if not component:
        ## get the caller
        component = sys._getframe(1).f_code.co_name

    ## check that no other instances of assignor is running
    process_check = filter(None,os.popen('ps -f -e | grep %s.py | grep -v grep  |grep python'%component).read().split('\n'))
    if len(process_check)>1:
        ## another component is running on the machine : stop
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

class siteInfo:
    def __init__(self):

        try:
            ## get all sites from SSB readiness
            self.sites_ready = []
            self.all_sites = []
            column = 158
            data = json.loads(os.popen('curl -s "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=%s&batch=1&lastdata=1"'%column).read())['csvdata']
            for siteInfo in data:
                self.all_sites.append( siteInfo['VOName'] )
                if not siteInfo['Tier']: continue
                if not siteInfo['Status'] == 'on': continue
                self.sites_ready.append( siteInfo['VOName'] )
                
        except Exception as e:
            print "issue with getting SSB readiness"
            print str(e)



        if self.sites_ready:
            self.sites_T2s = [ s for s in self.sites_ready if s.startswith('T2_')]
            self.sites_T1s = [ s for s in self.sites_ready if s.startswith('T1_')]
            self.sites_with_goodIO = [ "T2_DE_DESY","T2_DE_RWTH","T2_ES_CIEMAT","T2_FR_IPHC",
                                       "T2_IT_Bari","T2_IT_Legnaro","T2_IT_Pisa","T2_IT_Rome",
                                       "T2_UK_London_Brunel","T2_UK_London_IC","T2_US_Caltech","T2_US_MIT",
                                       "T2_US_Nebraska","T2_US_Purdue","T2_US_UCSD","T2_US_Wisconsin","T2_US_Florida"]
            self.sites_with_goodIO = [s for s in self.sites_with_goodIO if s in self.sites_ready]
            allowed_T2_for_transfer = ["T2_US_Nebraska","T2_US_Wisconsin","T2_US_Purdue","T2_US_Caltech","T2_DE_RWTH","T2_DE_DESY", "T2_US_Florida", "T2_IT_Legnaro", "T2_CH_CERN", "T2_UK_London_IC", "T2_IT_Pisa", "T2_US_UCSD", "T2_IT_Rome", "T2_US_MIT" ]
            allowed_T2_for_transfer = [s for s in allowed_T2_for_transfer if s in self.sites_ready]
            self.sites_veto_transfer = [site for site in self.sites_with_goodIO if not site in allowed_T2_for_transfer]

        else:
            ## all to be deprecated if above functions !
            siteblacklist = ['T2_TH_CUNSTDA','T1_TW_ASGC','T2_TW_Taiwan','T2_TR_METU','T2_UA_KIPT','T2_RU_ITEP','T2_RU_INR','T2_RU_PNPI','T2_PL_Warsaw']
            self.sites_with_goodIO = ["T1_DE_KIT","T1_ES_PIC","T1_FR_CCIN2P3","T1_IT_CNAF",
                                      "T1_RU_JINR","T1_UK_RAL","T1_US_FNAL","T2_CH_CERN",
                                      "T2_DE_DESY","T2_DE_RWTH","T2_ES_CIEMAT","T2_FR_IPHC",
                                      "T2_IT_Bari","T2_IT_Legnaro","T2_IT_Pisa","T2_IT_Rome",
                                      "T2_UK_London_Brunel","T2_UK_London_IC","T2_US_Caltech","T2_US_MIT",
                                      "T2_US_Nebraska","T2_US_Purdue","T2_US_UCSD","T2_US_Wisconsin","T2_US_Florida"]
            ## only T2s in that list
            self.sites_with_goodIO = filter(lambda s : s.startswith('T2'), self.sites_with_goodIO)
        
        
            allowed_T2_for_transfer = ["T2_US_Nebraska","T2_US_Wisconsin","T2_US_Purdue","T2_US_Caltech","T2_DE_RWTH","T2_DE_DESY", "T2_US_Florida", "T2_IT_Legnaro", "T2_CH_CERN", "T2_UK_London_IC", "T2_IT_Pisa", "T2_US_UCSD", "T2_IT_Rome", "T2_US_MIT" ]
            # at 400TB ""T2_IT_Bari","T2_IT_Legnaro"
            # border line "T2_UK_London_IC"
            self.sites_veto_transfer = [site for site in self.sites_with_goodIO if not site in allowed_T2_for_transfer]
            
            self.sites_T2s = [s for s in json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/whitelist.json').read()) if s not in siteblacklist and 'T2' in s]

            self.sites_T1s = [s for s in json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/whitelist.json').read()) if s not in siteblacklist and 'T1' in s]


        self.storage = defaultdict(int)
        self.disk = defaultdict(int)
        self.quota = defaultdict(int)
        self.locked = defaultdict(int)
        self.cpu_pledges = defaultdict(int)

        ## list here the site which can accomodate high memory requests
        self.sites_memory = {}

        ## this is still required because of MSS mainly
        bare_info = json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/disktape.json').read())
        for (item,values) in bare_info.items():
            if 'mss' in values:
                self.storage[values['mss']] = values['freemss']
            if 'disk' in values:
                self.disk[values['disk']] = values['freedisk']

        for (dse,free) in self.disk.items():
            if free<0:
                if not dse in self.sites_veto_transfer:
                    self.sites_veto_transfer.append( dse )

        if self.sites_ready:
            for s in self.all_sites:
                ## will get it later from SSB
                self.cpu_pledges[s]=1
                ## will get is later from SSB
                self.disk[ self.CE_to_SE(s)]=0

        else:
            self.cpu_pledges.update(json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/pledged.json').read()))

            ## hack around and put 1 CPU pledge for those with 0
            for (s,p) in self.cpu_pledges.items(): 
                if not p:
                    self.cpu_pledges[s] = 1
                
        self.sites_auto_approve = ['T0_CH_CERN_MSS','T1_FR_CCIN2P3_MSS']

        ## and get SSB sync
        self.fetch_more_info(talk=False)
        
        ## and detox info
        self.fetch_detox_info(talk=False)

        ## and glidein info
        self.fetch_glidein_info(talk=False)

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

    def fetch_glidein_info(self, talk=True):
        try:
            self.sites_memory = json.loads(os.popen('curl --retry 5 -s http://cms-gwmsmon.cern.ch/scheddview/json/totals').read())
        except:
            self.sites_memory = {}

    def sitesByMemory( self, maxMem):
        if not self.sites_memory:
            print "no memory information from glidein mon"
            return None
        allowed = set()
        for site,slots in self.sites_memory.items():
            if any([slot['MaxMemMB']>= maxMem for slot in slots]):
                allowed.add(site)
        return list(allowed)

    def restrictByMemory( self, maxMem, allowed_sites):
        allowed = self.sitesByMemory(maxMem)
        if allowed!=None:
            return list(set(allowed_sites) & set(allowed))
        return allowed_sites

    def fetch_detox_info(self, talk=True):
        ## put a retry in command line
        info = os.popen('curl --retry 5 -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt').read().split('\n')
        if len(info) < 15: 
            ## fall back to dev
            info = os.popen('curl --retry 5 -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS-Dev/DetoxDataOps/SitesInfo.txt').read().split('\n')
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
            _,quota,taken,locked,site = line.split()
            available = int(quota) - int(locked)
            if available >0:
                self.disk[site] = available
            else:
                self.disk[site] = 0 
            self.quota[site] = quota
            self.locked[site] = locked

    def fetch_more_info(self,talk=True):
        ## and complement information from ssb
        columns= {
            'prodCPU' : 159,
            'CPUbound' : 160,
            'FreeDisk' : 106,
            'UsedTape' : 108,
            'FreeTape' : 109
            }
        
        all_data = {}
        for name,column in columns.items():
            if talk: print name,column
            try:
                data = json.loads(os.popen('curl -s "http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=%s&batch=1&lastdata=1"'%column).read())
                all_data[name] = data['csvdata']
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
            key_for_cpu ='prodCPU'
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

            if 'FreeTape' in info and 'UsedTape' in info and tsite in self.storage and info['FreeTape']:
                if info['UsedTape'] and self.storage[tsite] < info['FreeTape']:
                    if talk: print tsite,"could use",info['FreeTape'],"instead of",self.storage[tsite],"for tape"
                    self.storage[tsite] = int(info['FreeTape'])


    def types(self):
        return ['sites_with_goodIO','sites_T1s','sites_T2s','sites_veto_transfer','sites_auto_approve']

    def CE_to_SE(self, ce):
        if ce.startswith('T1') and not ce.endswith('_Disk'):
            return ce+'_Disk'
        else:
            addHoc = {
                'T2_CH_CERN_T0': 'T2_CH_CERN',
                'T2_CH_CERN_HLT' : 'T2_CH_CERN',
                'T2_CH_CERN_AI' : 'T2_CH_CERN'
                }
            if ce in addHoc:
                return addHoc[ce]
            else:
                return ce

    def SE_to_CE(self, se):
        if se.endswith('_Disk'):
            return se.replace('_Disk','')
        elif se.endswith('_MSS'):
            return se.replace('_MSS','')
        else:
            return se

    def pick_SE(self, sites=None):
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
            if rnd < 0:
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



class closeoutInfo:
    def __init__(self):
        self.record = json.loads(open('closedout.json').read())

    def table_header(self):
        text = '<table border=1><thead><tr><th>workflow</th><th>OutputDataSet</th><th>%Compl</th><th>acdc</th><th>Dupl</th><th>CorrectLumis</th><th>Scubscr</th><th>Tran</th><th>dbsF</th><th>dbsIF</th><th>\
phdF</th><th>ClosOut</th></tr></thead>'
        return text

    def one_line(self, wf, wfo, count):
        if count%2:            color='lightblue'
        else:            color='white'
        text=""
        try:
            pid = filter(lambda b :b.count('-')==2, wf.split('_'))[0]
            tpid = 'task_'+pid if 'task' in wf else pid
        except:
            pid ='None'
            tpid= 'None'
            
        ## return the corresponding html
        order = ['percentage','acdc','duplicate','correctLumis','missingSubs','phedexReqs','dbsFiles','dbsInvFiles','phedexFiles']
        wf_and_anchor = '<a id="%s">%s</a>'%(wf,wf)
        for out in self.record[wf]['datasets']:
            text+='<tr bgcolor=%s>'%color
            text+='<td>%s<br><a href=https://cmsweb.cern.ch/reqmgr/view/details/%s>dts</a>, <a href=https://cmsweb.cern.ch/reqmgr/view/splitting/%s>splt</a>, <a href=https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s>perf</a>, <a href=https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s>ac</a>, <a href=assistance.html#%s>%s</a></td>'% (wf_and_anchor,
                                                                                                                                                                                                                                                                                                                            wf, wf, wf,tpid,wf,
                                                                                                                                                                                                                                                                                                                            wfo.status)

            text+='<td>%s</td>'% out
            for f in order:
                if f in self.record[wf]['datasets'][out]:
                    value = self.record[wf]['datasets'][out][f]
                else:
                    value = "-NA-"
                if f =='acdc':
                    text+='<td><a href=https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byprepid?key="%s">%s</a></td>'%(tpid , value)
                else:
                    text+='<td>%s</td>'% value
            text+='<td>%s</td>'%self.record[wf]['closeOutWorkflow']
            text+='</tr>'
            wf_and_anchor = wf

        return text

    def html(self):
        self.summary()
        self.assistance()

    def summary(self):
        os.system('cp closedout.json closedout.json.last')
        
        html = open('/afs/cern.ch/user/c/cmst2/www/unified/closeout.html','w')
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

        short_html = open('/afs/cern.ch/user/c/cmst2/www/unified/assistance_summary.html','w')
        html = open('/afs/cern.ch/user/c/cmst2/www/unified/assistance.html','w')
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
    
        short_html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/last.log target=_blank> log</a> <a href=logs/recoveror/last.log target=_blank> postlog</a> <br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))
        html.write('Last update on %s(CET), %s(GMT), <a href=logs/checkor/last.log target=_blank> log</a> <a href=logs/recoveror/last.log target=_blank> postlog</a><br>'%(time.asctime(time.localtime()),time.asctime(time.gmtime())))

        html.write('<a href=assistance_summary.html> Summary </a> <br>')    
        short_html.write('<a href=assistance.html> Details </a> <br>')

        assist = defaultdict(list)
        for wfo in wfs:
            assist[wfo.status].append( wfo )
        

        for status in sorted(assist.keys()):
            html.write("Workflow in status <b> %s </b> (%d)"% (status, len(assist[status])))
            html.write( self.table_header())
            short_html.write("""
Workflow in status <b> %s </b>
<table border=1>
<thead>
<tr>
<th> workflow </th> <th> output dataset </th><th> completion </th>
</tr>
</thead>
"""% (status))
            for (count,wfo) in enumerate(assist[status]):
                if count%2:            color='lightblue'
                else:            color='white'
                if not wfo.name in self.record: 
                    print "wtf with",wfo.name
                    continue            
                html.write( self.one_line( wfo.name, wfo, count))
                for out in self.record[wfo.name]['datasets']:
                    short_html.write("""
<tr bgcolor=%s>
<td> <a id=%s>%s</a> </td><td> %s </td><td> <a href=closeout.html#%s>%s</a> </td>
</tr>
"""%( color, 
      wfo.name,wfo.name,
      out, 
      wfo.name,
      self.record[wfo.name]['datasets'][out]['percentage'],
      
      ))
            html.write("</table><br><br>")
            short_html.write("</table><br><br>")

        short_html.write("<br>"*100)
        short_html.write("bottom of page</html>")    
        html.write("<br>"*100)
        html.write("bottom of page</html>")    


global_SI = siteInfo()
def getSiteWhiteList( inputs , pickone=False):
    SI = global_SI
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

def findLostBlocks(url, dataset):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

    r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?block=%s%%23*&collapse=n'% dataset)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    lost = []
    for dataset in result['phedex']['dataset']:
        for item in dataset['block']:
            exist=0
            for loc in item['subscription']:
                exist = max(exist, loc['percent_bytes'])
            if not exist:
                #print "We have lost:",item['name']
                #print json.dumps( item, indent=2)
                lost.append( item )
    return lost


def checkTransferStatus(url, xfer_id, nocollapse=False):
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
    if nocollapse:
        subs_url+='&collapse=n'
    r1=conn.request("GET",subs_url)
    r2=conn.getresponse()
    result = json.loads(r2.read())

    #print result
    completions={}
    if len(result['phedex']['dataset'])==0:
        print "trying with an earlier date than",timecreate,"for",xfer_id
        subs_url = '/phedex/datasvc/json/prod/subscriptions?request=%s&create_since=%d'%(str(xfer_id),0)
        r1=conn.request("GET",subs_url)
        r2=conn.getresponse()
        result = json.loads(r2.read())

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

def findCustodialLocation(url, dataset):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    request=result['phedex']
    if 'block' not in request.keys():
        return []
    if len(request['block'])==0:
        return []
    cust=[]
    #veto = ["T0_CH_CERN_MSS"]
    veto = []
    for block in request['block']:
        for replica in block['replica']:
            #print replica
            if replica['custodial']=="y":
                if (replica['node'] in veto):
                    #print replica['node']
                    pass
                else:
                    cust.append(replica['node'])

    return list(set(cust))

def getDatasetBlocksFraction(url, dataset, complete='y', group=None, vetoes=None, sites=None):
    ###count how manytimes a dataset is replicated < 100%: not all blocks > 100% several copies exis
    if vetoes==None:
        vetoes = ['MSS','Buffer','Export']

    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    #all_blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    #all_block_names=set([block['block_name'] for block in all_blocks])
    files = dbsapi.listFileArray( dataset= dataset,validFileOnly=1, detail=True)
    all_block_names = list(set([f['block_name'] for f in files]))
    
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
                block_counts[ item['name'] ] +=1
    
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
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
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

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?block=%s%%23*&collapse=n'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    destinations=defaultdict(set)

    if not len(result['phedex']['dataset']):
        return destinations, all_block_names

    items=result['phedex']['dataset'][0]['block']

    ## the final answer is site : data_fraction_subscribed

    for item in items:
        if item['name'] not in all_block_names and not only_blocks:
            print item['name'],'not yet injected in dbs, counting anyways'
            all_block_names.add( item['name'] )
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
        print "Complementing the destinations with request with no subscriptions"
        r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset=%s'%dataset)
        r2=conn.getresponse()
        result = json.loads(r2.read())
        items=result['phedex']['request']
    else:
        items=[]

    for item in items:
        phedex_id = item['id']
        sites_destinations = [req['name'] for req in item['node'] if req['decision'] in ['approved']]
        sites_destinations = [site for site in sites_destinations if not any(site.endswith(v) for v in vetoes)]
        print phedex_id,sites_destinations
        ## what are the sites for which we have missing information ?
        sites_missing_information = [site for site in sites_destinations if site not in destinations.keys()]


        if len(sites_missing_information)==0: continue

        r3 = conn.request("GET",'/phedex/datasvc/json/prod/transferrequests?request=%s'%phedex_id)
        r4 = conn.getresponse()
        sub_result = json.loads(r4.read())
        sub_items = sub_result['phedex']['request']
        ## skip if we specified group
        if group!=None and not sub_items['group'].lower()==group.lower(): continue
        for req in sub_items:
            for requested_dataset in req['data']['dbs']['dataset']:
                if requested_dataset != dataset: continue
                for site in sites_missing_information:
                    for b in all_block_names:
                        destinations[site].add( (b, 0, phedex_id) )
                    
            for b in req['data']['dbs']['block']:
                if not b['name'] in all_block_names: continue
                destinations[site].add((b['name'], 0, phedex_id) )
        
        
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

def getDatasetPresence( url, dataset, complete='y', only_blocks=None, group=None, vetoes=None, within_sites=None):
    if vetoes==None:
        vetoes = ['MSS','Buffer','Export']
    #print "presence of",dataset
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    all_blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
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
        return {}
    #print full_size
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset=%s'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['block']


    locations=defaultdict(set)
    for item in items:
        for replica in item['replica']:
            if not any(replica['node'].endswith(v) for v in vetoes):
                if within_sites and not replica['node'] in within_sites: continue
                if replica['group'] == None: replica['group']=""
                if complete and not replica['complete']==complete: continue
                #if group!=None and replica['group']==None: continue
                if group!=None and not replica['group'].lower()==group.lower(): continue 
                locations[replica['node']].add( item['name'] )
                if item['name'] not in all_block_names and not only_blocks:
                    print item['name'],'not yet injected in dbs, counting anyways'
                    all_block_names.add( item['name'] )
                    full_size += item['bytes']

    presence={}
    for (site,blocks) in locations.items():
        site_size = sum([ block['file_size'] for block in all_blocks if (block['block_name'] in blocks and block['block_name'] in all_block_names)])
        #print site,blocks,all_block_names
        #presence[site] = (set(blocks).issubset(set(all_block_names)), site_size/float(full_size)*100.)
        presence[site] = (set(all_block_names).issubset(set(blocks)), site_size/float(full_size)*100.)
    #print json.dumps( presence , indent=2)
    return presence

def getDatasetSize(dataset):
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    ## put everything in terms of GB
    return sum([block['file_size'] / (1024.**3) for block in blocks])

def getDatasetChops(dataset, chop_threshold =1000., talk=False, only_blocks=None):
    chop_threshold = float(chop_threshold)
    ## does a *flat* choping of the input in chunk of size less than chop threshold
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)

    ## restrict to these blocks only
    if only_blocks:
        blocks = [block for block in blocks if block['block_name'] in only_blocks]

    sum_all = 0

    ## put everything in terms of GB
    for block in blocks:
        block['file_size'] /= 1000000000.

    for block in blocks:
        sum_all += block['file_size']

    items=[]
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
                
            if talk:
                print len(items[-1]),"items below thresholds",size_chunk
                print items[-1]
    else:
        if talk:
            print "one big",sum_all
        if only_blocks:
            items = [blocks] 
        else:
            items = [[dataset]] 
        if talk:
        print items
    ## a list of list of blocks or dataset
    print "Choped",dataset,"of size",sum_all,"GB (",chop_threshold,"GB) in",len(items),"pieces"
    return items

def distributeToSites( items, sites , n_copies, weights=None):
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
        for item in items:
            at=set()
            #print item,"requires",n_copies,"copies to",len(sites),"sites"
            if len(sites) <= n_copies:
                #more copies than sites
                at = set(sites)
            else:
                # pick at random according to weights
                for pick in range(n_copies):
                    at.add(SI.pick_CE( list(set(sites)-at)))
                #print list(at)
            for site in at:
                spreading[site].extend(item)                
        return dict(spreading)

def getDatasetEventsAndLumis(dataset, blocks=None):
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    all_files = []
    if blocks:
        for b in blocks:
            all_files.extend( dbsapi.listFileSummaries( block_name = b  , validFileOnly=1))
    else:
        all_files = dbsapi.listFileSummaries( dataset = dataset , validFileOnly=1)

    all_events = sum([f['num_event'] for f in all_files])
    all_lumis = sum([f['num_lumi'] for f in all_files])
    return all_events,all_lumis

def getDatasetEventsPerLumi(dataset):
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    all_files = dbsapi.listFileSummaries( dataset = dataset , validFileOnly=1)
    try:
        average = sum([f['num_event']/float(f['num_lumi']) for f in all_files]) / float(len(all_files))
    except:
        average = 100
    return average
                    
def setDatasetStatus(dataset, status):
    dbswrite = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSWriter')

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
        dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        # retrieve dataset summary                                                                                                                                                                                                                                                   
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*',detail=True)
        if len(reply):
            return reply[0]['dataset_access_type']
        else:
            return None

def getDatasets(dataset):
       # initialize API to DBS3                                                                                                                                                                                                                                                      
        dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        # retrieve dataset summary                                                                                                                                                                                                                                                   
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*')
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
    dbs.setAttribute("name", "https://cmsweb.cern.ch/dbs/prod/global/DBSReader")
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
        ## find out from the request itself ?
        nodes = list(nodes)

    params = {
        'decision' : decision,
        'request' : phedexid,
        'node' : ','.join(nodes),
        'comments' : comments
        }
    
    result = phedexPost(url, "/phedex/datasvc/json/prod/updaterequest", params)
    if not result:
        return False

    if 'already' in result:
        return True
    return result

def makeDeleteRequest(url, site,datasets, comments, priority='low'):
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

def makeReplicaRequest(url, site,datasets, comments, priority='normal',custodial='n',approve=False): # priority used to be normal
    dataXML = createXML(datasets)
    r_only = "n" if approve else "y"
    params = { "node" : site,"data" : dataXML, "group": "DataOps", "priority": priority,
                 "custodial":custodial,"request_only":r_only ,"move":"n","no_mail":"n","comments":comments}
    response = phedexPost(url, "/phedex/datasvc/json/prod/subscribe", params)
    return response

def updateSubscription(url, site, item, priority=None, user_group=None):
    params = { "node" : site }
    params['block' if '#' in item else 'dataset'] = item
    if priority:   params['priority'] = priority
    if user_group: params['user_group'] = user_group
    response = phedexPost(url, "/phedex/datasvc/json/prod/updatesubscription", params) 
    print response

def getWorkLoad(url, wf ):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1 = conn.request("GET",'/couchdb/reqmgr_workload_cache/'+wf)
    r2=conn.getresponse()
    data = json.loads(r2.read())
    return data

def getViewByInput( url, details=False):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byinputdataset?'
    if details:
        there+='&include_docs=true'
    r1=conn.request("GET",there)
    r2=conn.getresponse()
    data = json.loads(r2.read())
    items = data['rows']
    return items
    if details:
        return [item['doc'] for item in items]
    else:
        return [item['id'] for item in items]

def getViewByOutput( url, details=False):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    there = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byoutputdataset?'
    if details:
        there+='&include_docs=true'
    r1=conn.request("GET",there)
    r2=conn.getresponse()
    data = json.loads(r2.read())
    items = data['rows']
    return items
    if details:
        return [item['doc'] for item in items]
    else:
        return [item['id'] for item in items]
        
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
    
def getWorkflows(url,status,user=None,details=False):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    go_to = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatus?key="%s"'%(status)
    if details:
        go_to+='&include_docs=true'
    r1=conn.request("GET",go_to)
    r2=conn.getresponse()
    data = json.loads(r2.read())
    items = data['rows']

    workflows = []
    for item in items:
        wf = item['id']
        if (user and wf.startswith(user)) or not user:
            workflows.append(item['doc' if details else 'id'])

    return workflows

class workflowInfo:
    def __init__(self, url, workflow, deprecated=False, spec=True, request=None):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        self.deprecated_request = {}
        if deprecated:
            r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
            r2=conn.getresponse()
            self.deprecated_request = json.loads(r2.read())
        if request == None:
            r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/'+workflow)
            r2=conn.getresponse()
            self.request = json.loads(r2.read())
        else:
            self.request = copy.deepcopy( request )
        if spec:
            r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/%s/spec'%workflow)
            r2=conn.getresponse()
            self.full_spec = pickle.loads(r2.read())
        self.url = url

    def getWorkQueue(self):
        conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/couchdb/workqueue/_design/WorkQueue/_view/elementsByParent?key="%s"&include_docs=true'% self.request['RequestName'])
        r2=conn.getresponse()
        self.workqueue = list([d['doc'] for d in json.loads(r2.read())['rows']])
        
    def getSummary(self):
        conn  =  httplib.HTTPSConnection(self.url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/couchdb/workloadsummary/'+self.request['RequestName'])
        r2=conn.getresponse()
        
        self.summary = json.loads(r2.read())
        
    def _tasks(self):
        return self.full_spec.tasks.tasklist

    def firstTask(self):
        return self._tasks()[0]

    def getComputingTime(self,unit='h'):
        cput = 0
        if 'InputDataset' in self.request:
            ds = self.request['InputDataset']
            if 'BlockWhitelist' in self.request and self.request['BlockWhitelist']:
                (ne,_) = getDatasetEventsAndLumis( ds , eval(self.request['BlockWhitelist']) )
            else:
                (ne,_) = getDatasetEventsAndLumis( ds )
            tpe = self.request['TimePerEvent']
            
            cput = ne * tpe
        elif self.request['RequestType'] == 'TaskChain':
            #print "not implemented yet"
            pass
        else:
            ne = float(self.request['RequestNumEvents'])
            tpe = self.request['TimePerEvent']
            
            cput = ne * tpe

        if unit=='m':
            cput = cput / (60.)
        if unit=='h':
            cput = cput / (60.*60.)
        if unit=='d':
            cput = cput / (60.*60.*24.)
        return cput
    
    def availableSlots(self):
        av = 0
        SI = global_SI
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

    def checkWorkflowSplitting( self ):
        ## this isn't functioning for taskchain BTW
        if 'InputDataset' in self.request:
            average = getDatasetEventsPerLumi(self.request['InputDataset'])
            timing = self.request['TimePerEvent']
  
            ## if we can stay within 48 with one lumi. do it
            timeout = 48 *60.*60. #self.request['OpenRunningTimeout']
            if (average * timing) < timeout:
                ## we are within overboard with one lumi only
                return True

            spl = self.getSplittings()[0]
            algo = spl['splittingAlgo']
            if algo == 'EventAwareLumiBased':
                events_per_job = spl['events_per_job']
                if average > events_per_job:
                    ## need to do something
                    print "This is going to fail",average,"in and requiring",events_per_job
                    return False
        return True

    def getSchema(self):
        new_schema = copy.deepcopy( self.full_spec.request.schema.dictionary_())
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

    def getAllTasks(self, select=None):
        all_tasks = []
        for task in self._tasks():
            ts = getattr(self.full_spec.tasks, task)
            all_tasks.extend( self._taskDescending( ts, select ) )
        return all_tasks

    def getSplittings(self):
        spl =[]
        for task in self.getAllTasks(select={'taskType':['Production','Processing']}):
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

    def getEra(self):
        return self.request['AcquisitionEra']
    def getCurrentStatus(self):
        return self.request['RequestStatus']
    def getProcString(self):
        return self.request['ProcessingString']
    def getRequestNumEvents(self):
        return int(self.request['RequestNumEvents'])
    def getPileupDataset(self):
        if 'MCPileup' in self.request:
            return self.request['MCPileup']
        return None
    def getPriority(self):
        return self.request['RequestPriority']

    def getBlockWhiteList(self):
        bwl=[]
        if self.request['RequestType'] == 'TaskChain':
            bwl_t = self._collectintaskchain('BlockWhitelist')
            for task in bwl_t:
                bwl.extend(eval(bwl_t[task]))
        else:
            if 'BlockWhitelist' in self.request:
                bwl.extend(eval(self.request['BlockWhitelist']))

        return bwl
    def getLumiWhiteList(self):
        lwl=[]
        if self.request['RequestType'] == 'TaskChain':
            lwl_t = self._collectintaskchain('LumiWhitelist')
            for task in lwl_t:
                lwl.extend(eval(lwl_t[task]))
        else:
            if 'LumiWhitelist' in self.request:
                lwl.extend(eval(self.request['LumiWhitelist']))
        return lwl

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
                primary = set([blob['InputDataset']])
            if primary and 'IncludeParent' in blob and blob['IncludeParent']:
                parent = findParent( primary )
            if 'MCPileup' in blob:
                secondary = set([blob['MCPileup']])
            if 'LheInputFiles' in blob and blob['LheInputFiles'] in ['True',True]:
                lhe=True
                
            return (lhe,primary, parent, secondary)

        if self.request['RequestType'] == 'TaskChain':
            t=1
            while 'Task%d'%t in self.request:
                (alhe,aprimary, aparent, asecondary) = IOforTask(self.request['Task%d'%t])
                if alhe: lhe=True
                primary.update(aprimary)
                parent.update(aparent)
                secondary.update(asecondary)
                t+=1
        else:
            (lhe,primary, parent, secondary) = IOforTask( self.request )

        return (lhe,primary, parent, secondary)

    def _collectintaskchain( self , member, func=None):
        coll = {}
        t=1                              
        while 'Task%d'%t in self.request:
            if member in self.request['Task%d'%t]:
                if func:
                    coll[self.request['Task%d'%t]['TaskName']] = func(self.request['Task%d'%t][member])
                else:
                    coll[self.request['Task%d'%t]['TaskName']] = self.request['Task%d'%t][member]
            t+=1
        return coll

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

        if self.request['RequestType'] == 'TaskChain':
            acqEra = self._collectintaskchain('AcquisitionEra', func=invertDigits)
        else:
            acqEra = invertDigits(self.request['Campaign'])
        return acqEra

    def processingString(self):
        if self.request['RequestType'] == 'TaskChain':
            return self._collectintaskchain('ProcessingString')
        else:
            return self.request['ProcessingString']
        
    def getCampaign( self ):
        #if self.request['RequestType'] == 'TaskChain':
        #    return self._collectintaskchain('Campaign')
        #else:
        return self.request['Campaign']

            
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
                (aera,aps,_) = ps.split('-')
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
                (aera,aps,_) = ps.split('-')
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

