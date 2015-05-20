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


def sendEmail( subject, text, sender, destination ):
    print subject
    print text
    print sender
    print destination
    
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = COMMASPACE.join( destination )
    msg['Date'] = formatdate(localtime=True)
    new_msg_ID = make_msgid()  
    msg['Subject'] = subject
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

def listSubscriptions(url, dataset):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset=%s'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['request']
    destinations ={}
    deletes = {}
    for item in items:
        for node in item['node']:
            site = node['name']
            if item['type'] == 'delete' and node['decision'] in [ 'approved','pending']:
                deletes[ site ] = node['time_decided']

    for item in items:
        for node in item['node']:
            if item['type']!='xfer': continue
            site = node['name']            
            #print item
            if not 'MSS' in site:
                ## pending delete
                if site in deletes and not deletes[site]: continue
                ## delete after transfer
                if site in deletes and deletes[site] > node['time_decided']: continue

                destinations[site]=(item['id'], node['decision']=='approved')
                #print node['name'],node['decision']
                #print node
    #print destinations
    return destinations

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
    def __init__(self, block=True, mcm=False):
        self.status ={
            'reqmgr' : False,
            'mcm' : False,
            'dbs' : False,
            'phedex' : False
            }
        try:
            print "checking reqmgr"
            wfi = workflowInfo('cmsweb.cern.ch','pdmvserv_task_B2G-RunIIWinter15wmLHE-00067__v1_T_150505_082426_497')
            self.status['reqmgr'] = True
        except:
            self.tell('reqmgr')
            print "cmsweb.cern.ch unreachable"
            if block:
                sys.exit(123)

        from McMClient import McMClient

        if mcm:
            try:
                mcmC = McMClient(dev=False)
                print "checking mcm"
                test = mcmC.getA('requests',page=0)
                if not test: 
                    self.tell('mcm')
                    print "mcm corrupted"
                    if block: 
                        sys.exit(124)
                self.status['mcm'] = True
            except:
                self.tell('mcm')
                print "mcm unreachable"
                if block:
                    sys.exit(125)
        
        try:
            print "checking dbs"
            dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
            blocks = dbsapi.listBlockSummaries( dataset = '/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM', detail=True)
            if not blocks:
                self.tell('dbs')
                print "dbs corrupted"
                if block:
                    sys.exit(126)
        except:
            self.tell('dbs')
            print "dbs unreachable"
            if block:
                sys.exit(127)

        try:
            print "checking phedex"
            cust = findCustodialLocation('cmsweb.cern.ch','/TTJets_mtop1695_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM')
            
        except:
            self.tell('phedex')
            if block:
                sys.exit(128)

    def tell(self, c):
        sendEmail("%s Component Down"%c,"The component is down, just annoying you with this","vlimant@cern.ch",['vlimant@cern.ch'])

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

def userLock(component):
    lockers = ['dmytro','mcremone','vlimant']
    for who in lockers:
        if os.path.isfile('/afs/cern.ch/user/%s/%s/public/ops/%s.lock'%(who[0],who,component)):
            print "disabled by",who
            return True
    return False

class siteInfo:
    def __init__(self):
        self.siteblacklist = ['T2_TH_CUNSTDA','T1_TW_ASGC','T2_TW_Taiwan']
        self.sites_with_goodIO = ["T1_DE_KIT","T1_ES_PIC","T1_FR_CCIN2P3","T1_IT_CNAF",
                                  "T1_RU_JINR","T1_UK_RAL","T1_US_FNAL","T2_CH_CERN",
                                  "T2_DE_DESY","T2_DE_RWTH","T2_ES_CIEMAT","T2_FR_IPHC",
                                  "T2_IT_Bari","T2_IT_Legnaro","T2_IT_Pisa","T2_IT_Rome",
                                  "T2_UK_London_Brunel","T2_UK_London_IC","T2_US_Caltech","T2_US_MIT",
                                  "T2_US_Nebraska","T2_US_Purdue","T2_US_UCSD","T2_US_Wisconsin","T2_US_Florida"]
        ## only T2s in that list
        self.sites_with_goodIO = filter(lambda s : s.startswith('T2'), self.sites_with_goodIO)
        
        
        if False:
            ## the old scheme
            self.sites_with_goodIO = ["T2_US_Nebraska","T2_US_MIT"]
            self.sites_veto_transfer = ["T2_US_MIT"]#,"T1_UK_RAL"]
        else:
            ## a new scheme with all 
            allowed_T2_for_transfer = ["T2_US_Nebraska","T2_US_Wisconsin","T2_US_Purdue","T2_US_Caltech","T2_DE_RWTH","T2_DE_DESY"]
            #no MB yet "T2_CH_CERN",
            #probable "T2_US_UCSD"
            # at 400TB ""T2_IT_Bari","T2_IT_Legnaro"
            # border line "T2_UK_London_IC"
            self.sites_veto_transfer = [site for site in self.sites_with_goodIO if not site in allowed_T2_for_transfer]
            

        self.sites_T2s = [s for s in json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/whitelist.json').read()) if s not in self.siteblacklist and 'T2' in s]
        self.sites_T1s = [s for s in json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/whitelist.json').read()) if s not in self.siteblacklist and 'T1' in s]

        bare_info = json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/disktape.json').read())
        self.storage = {}
        self.disk = {}
        for (item,values) in bare_info.items():
            if 'mss' in values:
                self.storage[values['mss']] = values['freemss']
            if 'disk' in values:
                self.disk[values['disk']] = values['freedisk']

        for (dse,free) in self.disk.items():
            if free<0:
                if not dse in self.sites_veto_transfer:
                    self.sites_veto_transfer.append( dse )

        self.cpu_pledges = json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/pledged.json').read())
        ## hack around and put 1 CPU pledge for those with 0
        for (s,p) in self.cpu_pledges.items(): 
            if not p:
                self.cpu_pledges[s] = 1

        self.all_sites = list(set(self.sites_T2s + self.sites_T1s + self.sites_with_goodIO))

        if not set(self.all_sites).issubset(set(self.cpu_pledges.keys())):
            print "There are missing sites in pledgeds"
            print list(set(self.all_sites) - set(self.cpu_pledges.keys()))
        
        ## and get SSB sync
        self.fetch_more_info(talk=False)

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
            if 'CPUbound' in info and site in self.cpu_pledges and info['CPUbound']:
                if self.cpu_pledges[site] < info['CPUbound']:
                    if talk: print site,"could use",info['CPUbound'],"instead of",self.cpu_pledges[site],"for CPU"
                    self.cpu_pledges[site] = int(info['CPUbound'])
                elif self.cpu_pledges[site] > 1.5* info['CPUbound']:
                    if talk: print site,"could correct",info['CPUbound'],"instead of",self.cpu_pledges[site],"for CPU"
                    self.cpu_pledges[site] = int(info['CPUbound'])                    

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
        return ['sites_with_goodIO','sites_T1s','sites_T2s','sites_veto_transfer']

    def CE_to_SE(self, ce):
        if ce.startswith('T1') and not ce.endswith('_Disk'):
            return ce+'_Disk'
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
        print "could not make a choice"


    def pick_CE(self, sites):
        #print len(sites),"to pick from"
        #r_weights = {}
        #for site in sites:
        #    r_weights[site] = self.cpu_pledges[site]
        #return r_weights.keys()[self._weighted_choice_sub(r_weights.values())]
        return self._pick(sites, self.cpu_pledges)

def getSiteWhiteList( inputs , pickone=False):
    SI = siteInfo()
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
                if sites and not replica['node'] in sites: continue
                block_counts[ item['name'] ] +=1
    
    first_order = float(len(block_counts) - block_counts.values().count(0)) / float(len(block_counts))
    if first_order <1.:
        print dataset,":not all",len(block_counts)," blocks are available, only",len(block_counts)-block_counts.values().count(0)
        return first_order
    else:
        second_order = sum(block_counts.values())/ float(len(block_counts))
        print dataset,":all",len(block_counts),"available",second_order,"times"
        return second_order
    

def getDatasetPresence( url, dataset, complete='y', only_blocks=None, group=None, vetoes=None):
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
                if replica['group'] == None: replica['group']=""
                if complete and not replica['complete']==complete: continue
                #if group!=None and replica['group']==None: continue
                if group!=None and not replica['group'].lower()==group.lower(): continue 
                locations[replica['node']].add( item['name'] )

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

def getDatasetChops(dataset, chop_threshold =1000., talk=False):
    chop_threshold = float(chop_threshold)
    ## does a *flat* choping of the input in chunk of size less than chop threshold
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
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

def approveSubscription(url, phedexid, nodes=None , comments =None):
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
        'decision' : 'approve',
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

def makeReplicaRequest(url, site,datasets, comments, priority='normal',custodial='n'): # priority used to be normal
    dataXML = createXML(datasets)
    params = { "node" : site,"data" : dataXML, "group": "DataOps", "priority": priority,
                 "custodial":custodial,"request_only":"y" ,"move":"n","no_mail":"n","comments":comments}
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

    def _tasks(self):
        return self.full_spec.tasks.tasklist

    def firstTask(self):
        return self._tasks()[0]

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

    def getSplittings(self):
        spl =[]
        for task in self._tasks():
            ts = getattr(self.full_spec.tasks, task).input.splitting
            spl.append( { "splittingAlgo" : ts.algorithm} )
            get_those = ['events_per_lumi','events_per_job','lumis_per_job']
            for get in get_those:
                if hasattr(ts,get):
                    spl[-1][get] = getattr(ts,get)
                #else:
                #    spl[-1][get] = None

        return spl

    def getEra(self):
        return self.request['AcquisitionEra']
    def getCurrentStatus(self):
        return self.request['RequestStatus']
    def getProcString(self):
        return self.request['ProcessingString']
    def getRequestNumEvents(self):
        return self.request['RequestNumEvents']
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
            if 'BlockWhitelist' in self.request:
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
            if 'InputDataset' in self.request:  
                primary = set([self.request['InputDataset']])
            if primary and 'IncludeParent' in self.request and self.request['IncludeParent']:
                parent = findParent( primary )
            if 'MCPileup' in self.request:
                secondary = set([self.request['MCPileup']])
            if 'LheInputFiles' in self.request and self.request['LheInputFiles'] in ['True',True]:
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
            version = max(0,self.request['ProcessingVersion']-1)
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
                predicted = '/'.join(['',dsn,'-'.join([aera,aps,'v%d'%(version+1)]),tier])
                conflicts = getWorkflowByOutput( self.url, predicted )
                conflicts = filter(lambda wfn : wfn!=self.request['RequestName'], conflicts)
                if len(conflicts):
                    print "There is an output conflict for",self.request['RequestName'],"with",conflicts
                    return None
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
                predicted = '/'.join(['',dsn,'-'.join([aera,aps,'v%d'%(version+1)]),tier])
                conflicts = getWorkflowByOutput( self.url, predicted )
                conflicts = filter(lambda wfn : wfn!=self.request['RequestName'], conflicts)
                if len(conflicts):
                    print "There is an output conflict for",self.request['RequestName'],"with",conflicts
                    return None
        return version+1

