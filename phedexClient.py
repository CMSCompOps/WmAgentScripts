#!/usr/bin/env python
"""
    Encapsulates requests to Phedex API
    Should be usead instead of phedexSubscription. When used
    Directly, it creates a custodial subscription to a given
    site for a given list of datasets
"""

import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation


def hasCustodialSubscription(datasetName):
    """
    Returns true if a given dataset has at least
    one custodial subscription
    """
    url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/Subscriptions?dataset=' + datasetName
    result = json.loads(urllib2.urlopen(url).read())
    datasets=result['phedex']['dataset']
    if datasets:
        dicts=datasets[0]
        subscriptions=dicts['subscription']
        #check all subscriptions
        for subscription in subscriptions:
            # if at least one subscription is custodial
            if subscription['level']=='DATASET' and subscription['custodial']=='y':
                return True
        #if no subscription found
        return False
    else:
        return False

def getSubscriptionSites(datasetName):
    """
    Return the list of sites wich have a subscription
    Either custodial or non custodial of a given site
    """
    url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions?dataset=' + datasetName
    result = json.loads(urllib2.urlopen(url).read())
    datasets = result['phedex']    
    sites = []
    if 'dataset' not in datasets.keys():
        return sites
    else:
        if not result['phedex']['dataset']:
            return sites
        #check all subscriptions
        for subscription in result['phedex']['dataset'][0]['subscription']:
            sites.append(subscription['node'])
        return sites

def getBlockReplicaSites(datasetName, onlycomplete=False):
    """
    Return the list of sites wich have any replica
    of any block of a given dataset, either if they do have
    subscription or not.
    if onlycomplete, it will return only the sites that have all the blocks completely transferred.
    """
    url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas?dataset=' + datasetName
    result = json.loads(urllib2.urlopen(url).read())
    blocks = result['phedex']    
    sites = set()
    if 'block' not in blocks:
        return sites
    elif not result['phedex']['block']:
        return sites
    firstblock = True
    #check all subscriptions
    for block in result['phedex']['block']:
        blocksites = set()
        for r in block['replica']:
            if r['complete'] == 'y':           
                blocksites.add(r['node'])
        #check for first block
        if firstblock:
            sites = blocksites
            firstblock = False
        #if we want any site, we do Union between sets
        if not onlycomplete:
            sites = sites | blocksites
        #if we want only sites with all the blocks, we do intersection.
        else:
            sites = sites & blocksites
    return list(sites)

def getCustodialMoveSubscriptionSite(datasetName):
    """
    Returns the site for which a custodial move subscription for a dataset was created,
    if none is found it returns False
    """
    url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions?dataset=' + datasetName
    result = json.loads(urllib2.urlopen(url).read())
    datasets = result['phedex']    
    if 'dataset' not in datasets.keys():
        return False
    else:
        if not result['phedex']['dataset']:
            return False
        #check all subscriptions
        for subscription in result['phedex']['dataset'][0]['subscription']:
            #if at least one is custodial
            if subscription['custodial']=='y':
                return subscription['node']
        #if no subscription found
        return False

def phedexGet(url, request, auth=True):
    """
    Queries PhEDEx through a HTTPS GET method
    using the environment certificates for authentication.
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url
    auth: if aouthentication needs to be used
    """
    if auth:
        conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), 
                                            key_file = os.getenv('X509_USER_PROXY'))
        r1 = conn.request("GET", request)
        r2 = conn.getresponse()
        result = json.loads(r2.read())
        conn.close()
        return result           
    else:
        r1 = urllib2.urlopen('https://'+url+request)
        result = json.loads(r1.read())
        return result

def getFileCountDataset(url, dataset):
    """
    Returns the number of files registered in phedex
    """
    result = phedexGet(url, '/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset, auth=False)
    if 'block' not in result['phedex']:
        return 0
    elif not result['phedex']['block']:
        return 0
    files = 0
    #check all blocks
    for block in result['phedex']['block']:
        files += block['files']
    return files
        
def getTransferPercentage(url, dataset, site):
    """
    Calculates a transfer percentage (0 to 1.0)from 
    given dataset to a given site by counting how many
    blocks have been completely transferred.
    """
    result = phedexGet(url, '/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset+'&node='+site, False)
    blocks = result['phedex']
    #if block not present
    if 'block' not in blocks:
        return 0
    if not result['phedex']['block']:
        return 0
    total = len(blocks['block'])
    completed = 0
    #count the number of blocks which transfer is complete
    for block in blocks['block']:
        if block['replica'][0]['complete']=='y':
            completed += 1 
    return float(completed)/float(total)

def transferComplete(url, dataset, site):
    """
    Gets if the transfer of a given dataset to a given site
    is completed (all blocks show completed in 'y')
    """
    result = phedexGet(url,'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset+'&node='+site+'_MSS')
    blocks = result['phedex']
    if 'block' not in blocks.keys():
        return False
    if len(result['phedex']['block'])==0:
        return False
    for block in blocks['block']:
        if block['replica'][0]['complete']!='y':
            return False
    return True        

def testAcceptedSubscritpionSpecialRequest(url, dataset, site):
    """
    gets if a given dataset has an approved special subscription on
    the given site.
    """
    result = phedexGet(url, ('/phedex/datasvc/json/prod/requestlist?dataset='
                                +dataset+'&node='+site+'&type=xfer'+'&approval=approved'))
    requests=result['phedex']
    if 'request' not in requests.keys():
        return False
    for request in result['phedex']['request']:
        for node in request['node']:
            if node['node']==site and node['decision']=='approved':
                return True
    return False

def testSubscritpionSpecialRequest(url, dataset, site):
    """
    gets if a given dataset has a special subscription on
    the given site.
    """
    result = phedexGet(url, '/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&node='+site+'&type=xfer')
    requests=result['phedex']
    if 'request' not in requests.keys():
        return False
    for request in result['phedex']['request']:
        for node in request['node']:
            if node['name']==site:
                return True
    return False

def testCustodialSubscriptionRequested(url, dataset, site):
    """
    Gets if a custodial subscription was requested for
    the given dataset at a given site.
    """
    result = phedexGet(url, '/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&node='+site+'_MSS')
    requests=result['phedex']
    #gets dataset subscription requests
    if 'request' not in requests.keys():
        return False
    #if there is a request
    for request in result['phedex']['request']:
        #if there are pending or aprroved request, watch the satus of them
        if request['approval']=='pending' or request['approval']=='approved':
            requestId = request['id']
            result = phedexGet(url, '/phedex/datasvc/json/prod/transferrequests?request='+str(requestId))
            if result['phedex']['request']:
                requestSubscription = result['phedex']['request'][0]
            else:
                return False
            #see if its custodial
            if requestSubscription['custodial']=='y':
                return True
    return False

def getCustodialSubscriptionRequestSite(datasetName):
    try:
        r = try_getCustodialSubscriptionRequestSite(datasetName)
    except:
        try:
            r = try_getCustodialSubscriptionRequestSite(datasetName)

        except:
            ## yes or NO ?
            r = []
    return r
            

def try_getCustodialSubscriptionRequestSite(datasetName):
    """
    Returns the site (or sites) in which the dataset has
    a custodial subscription request, no matter if it was approved
    or rejected.
    Returns false if no custodial subscription request has been made for
    the dataset
    """
    url = 'cmsweb.cern.ch'
    result = phedexGet(url, '/phedex/datasvc/json/prod/requestlist?dataset='+datasetName+'&type=xfer&node=T*_MSS')
    #result = phedexGet(url, '/phedex/datasvc/json/prod/requestlist?dataset='+datasetName+'&type=xfer')
    requests=result['phedex']
    #gets dataset subscription requests
    if 'request' not in requests.keys():
        print "no result for",datasetName,"in phedex request list"
        return []
    sites = []
    #if there is a request
    for request in result['phedex']['request']:
        #if there are pending or aprroved request, watch the satus of them
        if request['approval'] in ['pending','approved','mixed']:
            requestId = request['id']
            ### check only MSS endpoints
            #print "check",requestId
            result = phedexGet(url, '/phedex/datasvc/json/prod/transferrequests?request='+str(requestId))
            #if not empty
            if result['phedex']['request']:
                #print len(result['phedex']['request'])
                requestSubscription = result['phedex']['request'][0]
                #see if its custodial
                if requestSubscription['custodial']=='y':
                    sites.append(requestSubscription['destinations']['node'][0]['name'])
    return sites if sites else []

def testOutputDataset(datasetName):
    """
    Tests whether a dataset was subscribed to phedex
    """
    url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/Data?dataset=' + datasetName
    result = json.loads(urllib2.urlopen(url))
    dataset=result['phedex']['dbs']
    if len(dataset)>0:
        return True
    else:
        return False

def testWorkflows(workflows):
    """
    Test whether the output datasets for a workflow were subscribed
    """
    print "Testing the subscriptions, this process may take some time"
    for workflow in workflows:
        print "Testing workflow: "+workflow
        datasets=outputdatasetsWorkflow(workflow)
        numsubscribed=len(datasets)
        for dataset in datasets:
            if not testOutputDataset(dataset):
                print "Couldn't subscribe: "+ dataset
            else:
                numsubscribed=numsubscribed-1
        if numsubscribed==0:
            closeOutWorkflow(workflow)
            print "Everything subscribed and closedout"

def datasetforWorkfows(workflows):
    """
    Return a list of outputdatasets for the workflows on the given list
    """
    datasets = []
    for workflow in workflows:
        datasets = datasets + outputdatasetsWorkflow(workflow)
    return datasets


def createXML(datasets):
    """
    From a list of datasets return an XML of the datasets in the format required by Phedex
    """
    # Create the minidom document
    impl = getDOMImplementation()
    doc = impl.createDocument(None, "data", None)
    result = doc.createElement("data")
    result.setAttribute('version', '2')
    # Create the <dbs> base element
    dbs = doc.createElement("dbs")
    dbs.setAttribute("name", "https://cmsweb.cern.ch/dbs/prod/global/DBSReader")
    result.appendChild(dbs)    
    #Create each of the <dataset> element            
    for datasetname in datasets:
        dataset = doc.createElement("dataset")
        dataset.setAttribute("is-open","y")
        dataset.setAttribute("is-transient","y")
        dataset.setAttribute("name",datasetname)
        dbs.appendChild(dataset)
    return result.toprettyxml(indent="  ")



def phedexPost(url, request, params):
    """
    Queries PhEDEx through a HTTPS POST method
    using the environment certificates for authentication.
    url: the instance used, i.e. url='cmsweb.cern.ch' 
    request: the request suffix url
    params: a dictionary with the POST parameters
    """
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), 
                                        key_file = os.getenv('X509_USER_PROXY'))
    encodedParams = urllib.urlencode(params, doseq=True)
    #encodedParams = json.dumps(params)
    r1 = conn.request("POST", request, encodedParams)
    r2 = conn.getresponse()
    message = r2.read()
    conn.close()
    try:
        result = json.loads(message)
    except:
        return message
    return result

def createParams(site, datasetXML, comments):
    """
    Create the parameters of the request
    """
    params = { "node" : site+"_MSS","data" : datasetXML, "group": "DataOps",
                "priority":'normal', "custodial":"y","request_only":"n" ,
                "move":"n","no_mail":"n", "comments":comments}
    return params

def makeDeletionRequest(url, site, datasets, comments):
    """
    Creates a deletion request
    """
    dataXML = createXML(datasets)
    params = {  "node":site,
                "data":dataXML,
                "level":"dataset",
                "rm_subscriptions":"y", 
                "comments":comments}
    
    response = phedexPost(url, "/phedex/datasvc/json/prod/delete", params)
    return response

def makeMoveRequest(url, site, datasets, comments, priority='high',custodial='n'):
    dataXML = createXML(datasets)
    params = { "node" : site,
                "data" : dataXML,
                "group": "DataOps",
                "priority": priority,
                "custodial":custodial,
                "request_only":"y",
                "move":"n",
                "no_mail":"n",
                "comments":comments}
    response = phedexPost(url, "/phedex/datasvc/json/prod/subscribe", params)
    return response

def makeReplicaRequest(url, site,datasets, comments, priority='high',custodial='n'): # priority used to be normal
    dataXML = createXML(datasets)
    params = { "node" : site,
                "data" : dataXML,
                "group": "DataOps",
                "priority": priority,
                "custodial":custodial,
                "request_only":"y",
                "move":"n",
                "no_mail":"n",
                "comments":comments}
    response = phedexPost(url, "/phedex/datasvc/json/prod/subscribe", params)
    return response

def main():
    args = sys.argv[1:]
    if not len(args)==3:
        print "usage site_name file comments"
    site = args[0]
    filename = args[1]
    comments = args[2]
    url='cmsweb.cern.ch'
    #reads file, striping and ignoring empty lines
    outputdatasets = [ds.strip() for ds in open(filename).readlines() if ds.strip()]
    resp = makeReplicaRequest(url, site, outputdatasets, comments)
    print resp
    
    sys.exit(0);

if __name__ == "__main__":
    main()

