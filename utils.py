#! -*- encoding: utf-8 -*-

import sys
import urllib, urllib2
import logging
from dbs.apis.dbsClient import DbsApi
import reqMgrClient
import httplib
import os
import json 
from collections import defaultdict
import random
from xml.dom.minidom import getDOMImplementation
import copy 


FORMAT = "%(module)s.%(funcName)s(%(lineno)s) => %(message)s (%(asctime)s)"
DATEFMT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format = FORMAT, datefmt = DATEFMT, level=logging.DEBUG)


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

def listSubscriptions(url, dataset):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset=%s'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['request']
    destinations ={}
    for item in items:
        for node in item['node']:
            if item['type']!='xfer': continue
            #print item
            if not 'MSS' in node['name']:
                destinations[node['name']]=(node['decision']=='approved')
                #print node['name'],node['decision']
                #print node
    #print destinations
    return destinations

class campaignInfo:
    def __init__(self):
        #this contains accessor to aggreed campaigns, with maybe specific parameters
        self.campaigns = {
            'Fall14DR73' : {'go':True},
            'RunIIWinter15GS' : {'go':True},
            'RunIIWinter15wmLHE' : {'go':True},
            'TP2023SHCALDR' : {'go':True},
            'HiFall13DR53X' : {'go':True, 'parameters' : {'MergedLFNBase' : '/store/himc', 'SiteWhitelist' :['T1_FR_CCIN2P3']}}
            }
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

class siteInfo:
    def __init__(self):
        self.siteblacklist = ['T2_TH_CUNSTDA','T1_TW_ASGC','T2_TW_Taiwan']
        self.sites_with_goodIO = ["T1_DE_KIT","T1_ES_PIC","T1_FR_CCIN2P3","T1_IT_CNAF",
                                  "T1_RU_JINR","T1_UK_RAL","T1_US_FNAL","T2_CH_CERN",
                                  "T2_DE_DESY","T2_DE_RWTH","T2_ES_CIEMAT","T2_FR_IPHC",
                                  "T2_IT_Bari","T2_IT_Legnaro","T2_IT_Pisa","T2_IT_Rome",
                                  "T2_UK_London_Brunel","T2_UK_London_IC","T2_US_Caltech","T2_US_MIT",
                                  "T2_US_Nebraska","T2_US_Purdue","T2_US_UCSD","T2_US_Wisconsin","T2_US_Florida"]
        self.sites_with_goodIO = filter(lambda s : s.startswith('T2'), self.sites_with_goodIO)
        
        self.sites_T2s = [s for s in json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/whitelist.json').read()) if s not in self.siteblacklist and 'T2' in s]
        self.sites_T1s = [s for s in json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/whitelist.json').read()) if s not in self.siteblacklist and 'T1' in s]

        self.cpu_pledges = json.loads(open('/afs/cern.ch/user/c/cmst2/www/mc/pledged.json').read())
        if not set(self.sites_T2s + self.sites_T1s + self.sites_with_goodIO).issubset(set(self.cpu_pledges.keys())):
            print "There are missing sites in pledgeds"
            print list(set(self.sites_T2s + self.sites_T1s + self.sites_with_goodIO) - set(self.cpu_pledges.keys()))
        
    def CE_to_SE(self, ce):
        if 'T1' in ce and not ce.endswith('_Disk'):
            return ce+'_Disk'
        else:
            return ce

    def pick_CE(self, sites):
        r_weights = {}
        for site in sites:
            r_weights[site] = self.cpu_pledges[site]

        def weighted_choice_sub(ws):
            rnd = random.random() * sum(ws)
            for i, w in enumerate(ws):
                rnd -= w
                if rnd < 0:
                    return i
        return r_weights.keys()[weighted_choice_sub(r_weights.values())]


def getSiteWhiteList( inputs , pickone=False):
    SI = siteInfo()
    (lheinput,primary,parent,secondary) = inputs
    sites_allowed=[]
    if lheinput:
        sites_allowed = ['T2_CH_CERN'] ## and that's it
    elif not primary and not secondary and not parent:
        sites_allowed =list(set( SI.sites_T1s + SI.sites_T2s ))
    elif primary and secondary:
        sites_allowed = list(set(SI.sites_T1s + SI.sites_with_goodIO))
        if pickone:
            sites_allowed = [SI.pick_CE( sites_allowed )]
    elif primary and not secondary:
        sites_allowed =list(set(SI.sites_T1s + SI.sites_T2s))
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

def checkTransferStatus(url, xfer_id):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?request=%s'%(str(xfer_id)))
    #r2=conn.getresponse()
    #result = json.loads(r2.read())
    #timestamp = result['phedex']['request_timestamp']
    r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?request='+str(xfer_id))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    timecreate=min([r['time_create'] for r in result['phedex']['request']])
    r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?request=%s&create_since=%d'%(str(xfer_id),timecreate))
    r2=conn.getresponse()
    result = json.loads(r2.read())

    completions={}
    for item in  result['phedex']['dataset']:
        completions[item['name']]={}
        for sub in item['subscription']:
            if sub['percent_bytes']:
                #completions[item['name']] [sub['node']] = float(sub['percent_bytes'])
                completions[item['name']] [sub['node']] = float(sub['percent_files'])
            else:
                completions[item['name']] [sub['node']] = 0.

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

def getDatasetPresence( url, dataset, complete='y'):
    #print "presence of",dataset
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    all_blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    all_block_names=set([block['block_name'] for block in all_blocks])
    full_size = sum([block['file_size'] for block in all_blocks])
    if not full_size:
        print dataset,"is nowhere"
        return {}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset=%s'%(dataset))
    r2=conn.getresponse()
    result = json.loads(r2.read())
    items=result['phedex']['block']


    locations=defaultdict(set)
    for item in items:
        for replica in item['replica']:
            if not 'MSS' in replica['node'] and not 'Buffer' in replica['node']:
                if complete and not replica['complete']==complete: continue
                locations[replica['node']].add( item['name'] )

    presence={}
    for (site,blocks) in locations.items():
        site_size = sum([ block['file_size'] for block in all_blocks if block['block_name'] in blocks])

        presence[site] = (blocks == all_block_names, site_size/float(full_size)*100.)
    #print json.dumps( presence , indent=2)
    return presence

def getDatasetChops(dataset, chop_threshold =4., talk=False):
    ## does a *flat* choping of the input in chunk of size less than chop threshold
    dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
    blocks = dbsapi.listBlockSummaries( dataset = dataset, detail=True)
    sum_all = 0

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
            at=[]
            for pick in range(n_copies):
                site =  SI.pick_CE( sites )
                drop=100
                while (site in at)and(drop>0):
                    site =  SI.pick_CE( sites )
                    drop-=1
                #print site,"picked"
                if drop<=0:
                    print "had to drop an item to ditribute"
                    return {} ## just as good as exiting

                at.append(site)
                spreading[site].extend(item)

        return dict(spreading)

def getDatasetStatus(dataset):
        # initialize API to DBS3                                                                                                                                                                                                                                                     
        dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        # retrieve dataset summary                                                                                                                                                                                                                                                   
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*',detail=True)
        return reply[0]['dataset_access_type']

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
    encodedParams = urllib.urlencode(params)
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

def approveSubscription(url, phedexid, nodes=None ):
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
        'comments' : 'auto-approve of production prestaging'
        }
    
    result = phedexPost(url, "/phedex/datasvc/json/prod/updaterequest", params)
    if not result:
        return False

    if 'already' in result:
        return True
    return result

def makeReplicaRequest(url, site,datasets, comments, priority='high',custodial='n'): # priority used to be normal
    dataXML = createXML(datasets)
    params = { "node" : site,"data" : dataXML, "group": "DataOps", "priority": priority,
                 "custodial":custodial,"request_only":"y" ,"move":"n","no_mail":"n","comments":comments}
    response = phedexPost(url, "/phedex/datasvc/json/prod/subscribe", params)
    return response

def getWorkLoad(url, wf ):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1 = conn.request("GET",'/couchdb/reqmgr_workload_cache/'+wf)
    r2=conn.getresponse()
    data = json.loads(r2.read())
    return data

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
    
def getWorkflows(url,status,user):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype?stale=update_after')
    r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatus?key="%s"'%(status))
    r2=conn.getresponse()
    data = json.loads(r2.read())
    items = data['rows']

    workflows = []
    for item in items:
        wf = item['id']
        if wf.startswith(user):
            workflows.append(item['id'])

    return workflows

class workflowInfo:
    def __init__(self, url, workflow ):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        self.request = json.loads(r2.read())
        
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
        version = 0
        outputs = self.request['OutputDatasets']
        #print outputs
        era = self.acquisitionEra()
        ps = self.processingString()
        if self.request['RequestType'] == 'TaskChain':
            for outs in outputs:
                for output in outs:
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
        else:
            for outs in  outputs:
                for output in outs:
                    (_,dsn,ps,tier) = output.split('/')
                    (aera,aps,_) = ps.split('-')
                    if aera == 'None':
                        print "no era, using ",era
                        aera=era
                    if aps == 'None':
                        print "no process string, using wild char"
                        aps='*'
                    pattern = '/'.join(['',dsn,'-'.join([aera,aps,'v*']),tier])
                    wilds = getDatasets( pattern )
                    print pattern,"->",len(wilds),"match(es)"
                    for wild in wilds:
                        (_,_,mid,_) = wild.split('/')
                        v = int(mid.split('-')[-1].replace('v',''))
                        version = max(v,version)
            #print "version found so far",version
        return version+1

