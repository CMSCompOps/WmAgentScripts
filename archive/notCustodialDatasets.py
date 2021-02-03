#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, json, time

def BlockPresentatSite(url, block, node):
	print block
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?block='+block+'&node='+node)
	r2=conn.getresponse()
	result = json.loads(r2.read())
	resultBlocks=result['phedex']['block']
	if len(resultBlocks)>0:
		return True
	else:
		return False

def dataSetPresentatSite(url, dataset, node):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset+'&node='+node)
	r2=conn.getresponse()
	result = json.loads(r2.read())
	resultBlocks=result['phedex']['block']
	if len(resultBlocks)>0:
		return True
	else:
		return False


def blockSetCustodiallyandCompletedSomewherElse(url, block, node):
	PresentSite="T1_IT_FNAL"
	if 'Disk' in node:
		PresentSite=node[:-5]#to take the '_Disk' out of the name
	if 'Buffer' in node:
		PresentSite=node[:-7]#to take the '_Buffer' out of the name
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?percent_min=100&block='+block+'&node=T1_*_MSS&custodial=y')
	r2=conn.getresponse()
	result = json.loads(r2.read())
	if len(result['phedex']['dataset'])==0:
		return False
	resultSubscriptions=result['phedex']['dataset'][0]['subscription']
	if len(resultSubscriptions)==0 :
		return False
	else:
		for subscription in resultSubscriptions:
			if subscription['custodial']=='y' and PresentSite not in subscription['node']:
				return True
		return False	


def dataSetCustodialiySubscribedSomewhereElse(url, dataset, node):
	PresentSite="T1_IT_FNAL"
	if 'Disk' in node:
		PresentSite=node[:-5]#to take the '_Disk' out of the name
	if 'Buffer' in node:
		PresentSite=node[:-7]#to take the '_Buffer' out of the name
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?percent_min=100&dataset='+dataset+'&node=T1_*&custodial=y')
	r2=conn.getresponse()
	result = json.loads(r2.read())
	if len(result['phedex']['dataset'])==0:
		return False
	resultSubscriptions=result['phedex']['dataset'][0]['subscription']
	if len(resultSubscriptions)==0 :
		return False
	else:
		subscribedAtSite=False
		subscribedElse=False
		for subscription in resultSubscriptions:
			if PresentSite in subscription['node']:
				subscribedAtSite=True
			else:
				subscribedElse=True
		return not subscribedAtSite and subscribedElse
	

def getBlocksProducedLastWeek(url):
	BlocksWeek=[]
	oneWeekTime=time.time()-2*7*24*60*60
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/data?create_since='+str(oneWeekTime)+'&dataset=/*/*/GEN-SIM&level=block')
	r2=conn.getresponse()
	result = json.loads(r2.read())
	resultdatasets=result['phedex']['dbs'][0]['dataset']
	for dataset in resultdatasets:
		for block in dataset['block']:
			BlocksWeek.append(block['name'])
	return BlocksWeek

def getdatasetsProducedLastWeek(url):
	datasetsWeek=[]
	oneWeekTime=time.time()-30*24*60*60
        #oneWeekTime=time.time()-3*30*24*60*60
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/data?create_since='+str(oneWeekTime)+'&dataset=/*/*/*SIM&level=block')
	r2=conn.getresponse()
	result = json.loads(r2.read())
	resultdatasets=result['phedex']['dbs'][0]['dataset']
	for dataset in resultdatasets:
		datasetsWeek.append(dataset['name'])
	return datasetsWeek
		
	
def main():
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage:notCustodialDatasets sitename"
        sys.exit(0)
    site=args[0]
    url='cmsweb.cern.ch'
    blocksWeek=getBlocksProducedLastWeek(url)
    datasetsWeek=getdatasetsProducedLastWeek(url)
    for dataset in datasetsWeek:
	if dataSetPresentatSite(url, dataset, site):
		if dataSetCustodialiySubscribedSomewhereElse(url, dataset, site):	
			print dataset
    sys.exit(0);

if __name__ == "__main__":
    main()
