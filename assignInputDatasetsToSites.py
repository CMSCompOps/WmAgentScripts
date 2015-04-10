#!/usr/bin/env python
import urllib2, urllib, httplib, sys, re, os, json, time, optparse
from dbs.apis.dbsClient import DbsApi
from das_client import get_data
from xml.dom.minidom import getDOMImplementation
from collections import defaultdict
import phedexClient


url='cmsweb.cern.ch'
dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
das_host='https://cmsweb.cern.ch'

def listSubscriptions(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&type=xfer')
        r2=conn.getresponse()
        result = json.loads(r2.read())
        requests=result['phedex']

        sites = []
        created = {}
        decisions = {}
        decided = {}

        if 'request' not in requests.keys():
           return [sites, decisions, decided]
        
        for request in result['phedex']['request']:
           tc = request['time_create']
           for node in request['node']:
              name = node['name']
              if 'MSS' not in name and 'Buffer' not in name and 'Export' not in name:
                 sites.append(name)
                 de = node['decision']
                 td = node['time_decided']
                 decisions[name] = de
                 decided[name] = td
                 created[name] = tc
        return [sites, created, decisions, decided]

def getReplicaFileCount(site,datasetName):
    url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas?dataset=' + datasetName+'&node='+site
    result = json.loads(urllib2.urlopen(url).read())
    blocks=result['phedex']['block']
    cnt=0
    if blocks:
       for block in blocks:
          if 'files' in block:
             cnt = cnt + int(block['files'])
    return cnt

def getFileCount(dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve dataset summary
        reply = dbsapi.listBlockSummaries(dataset=dataset,detail=True)
        cnt=0
        for block in reply:
           cnt = cnt + int(block['num_file'])
        return cnt

def getSizeAtSite(site, dataset):
   actualFiles = getFileCount(dataset)
   haveFiles = getReplicaFileCount(site, dataset)
   if actualFiles > 0:
      return 100.0*float(haveFiles)/float(actualFiles)
   return 0

def getInputDataSet(url, workflow):
   conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
   r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
   r2=conn.getresponse()
   request = json.loads(r2.read())
   if 'InputDataset' in request:
      inputDataSets=request['InputDataset']
      if len(inputDataSets)<1:
         print "ERROR: No InputDataSet for workflow"
      else:
         return inputDataSets
   else:
      print "ERROR: No InputDataSet for workflow"
   return ''

def getWorkflows(state):
   conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
   r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype?stale=update_after')
   r2=conn.getresponse()
   data = json.loads(r2.read())
   items = data['rows']

   workflows = []
   for item in items:
      if state in item['key'] and 'ReDigi' in item['key']:
         workflows.append(item['key'][0])

   return workflows

def getBlockSizeDataSet(dataset):
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    reply = dbsapi.listBlockSummaries(dataset=dataset)
    return int(reply[0]['file_size'])/1000000000000.0

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
    result = json.loads(r2.read())
    conn.close()
    return result

def makeReplicaRequest(url, site,datasets, comments, priority='high'): # priority used to be normal
    dataXML = createXML(datasets)
    params = { "node" : site,"data" : dataXML, "group": "DataOps", "priority": priority,
                 "custodial":"n","request_only":"y" ,"move":"n","no_mail":"n","comments":comments}
    response = phedexPost(url, "/phedex/datasvc/json/prod/subscribe", params)
    return response

def findCustodialLocation(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
        r2=conn.getresponse()
        result = json.loads(r2.read())
        request=result['phedex']
        if 'block' not in request.keys():
                return "No Site"
        if len(request['block'])==0:
                return "No Site"
        for replica in request['block'][0]['replica']:
                if replica['custodial']=="y" and replica['node']!="T0_CH_CERN_MSS":
                        return replica['node']
        return "None"

def checkAcceptedSubscriptionRequest(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&type=xfer')
        r2=conn.getresponse()
        result = json.loads(r2.read())
        requests=result['phedex']
        if 'request' not in requests.keys():
                return [False, False]
        subscribed=False
        approved=False
        for request in result['phedex']['request']:
                for node in request['node']:
                        if 'Disk' in node['name']:
                                subscribed=True
                                if node['decision']=='approved':
                                        approved=True
        return[subscribed, approved]

def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
        parser.add_option('-e', '--execute', help='Actually subscribe data',action="store_true",dest='execute')
	(options,args) = parser.parse_args()

        t1s = ['T1_IT_CNAF_Disk','T1_ES_PIC_Disk','T1_DE_KIT_Disk','T1_FR_CCIN2P3_Disk','T1_UK_RAL_Disk','T1_US_FNAL_Disk']

        data = {}
        sizes = {}
        workflows = getWorkflows('assignment-approved')
        for workflow in workflows:
           dataset = getInputDataSet(url, workflow)
           if 'GEN-SIM' in dataset or 'GEN-RAW' in dataset:
              subscribe = 1
              print ''
              print 'CONSIDERING',workflow,dataset

              # Get list of sites which have at least part of this dataset
              #sitesWithReplicas = phedexClient.getBlockReplicaSites(dataset)

              # Check if input dataset is 100% complete at any site
              #for site in sites:
              #   if 'Buffer' not in site and 'MSS' not in site and 'Export' not in site and 'T1' in site:
              #      size = getSizeAtSite(site, dataset)

              # Get list of subscriptions
              [sites, created, decisions, decided] = listSubscriptions(url, dataset)
          
              # Check if any subscriptions to disk exist
              found = 0
              for site in sites:
                 print '- checking subscription to',site,created[site],decisions[site],decided[site]
                 if 'Buffer' not in site and 'MSS' not in site and 'Export' not in site and 'T1' in site:
                    if int(time.time()) - int(created[site]) < 5184000:
                       print '-- found potential candidate'
                       subscribe = 0
              if subscribe == 1:
                 siteCustodial = findCustodialLocation(url, dataset)
                 print '- custodial site is:',siteCustodial
                 size = getBlockSizeDataSet(dataset)
                 siteDisk = siteCustodial.replace("MSS","Disk")

                 # if no custodial site, pick a site to use
                 if siteDisk == 'None':
                    siteDisk = 'T1_US_FNAL_Disk'
                    print '- no custodial site, picking:',siteDisk

                 # don't use PIC for huge workflows
                 if 'PIC' in siteDisk and size > 16.0:
                    siteDisk = 'T1_US_FNAL_Disk'
                    print '- too big for PIC, picking:',siteDisk

                 if siteDisk not in data:
                    data[siteDisk] = []
                 if siteDisk not in sizes:
                    sizes[siteDisk] = 0
                 if dataset not in data[siteDisk]:
                    data[siteDisk].append(dataset)
                    sizes[siteDisk] = sizes[siteDisk] + size
        for site in data:
           print ''
           print 'Subscription to',site,'of size',sizes[site],'TB'
           #print data[site]
           for bit in data[site]:
              print bit
           if options.execute:
              print makeReplicaRequest(url, site, data[site], 'prestaging')

	sys.exit(0)

if __name__ == "__main__":
	main()
