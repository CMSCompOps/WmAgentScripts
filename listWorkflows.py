#!/usr/bin/env python -w
import sys,urllib,urllib2,re,time,os
import simplejson as json
import optparse
import httplib
import datetime
import shutil
import phedexSubscription

team2type = { 'mc' : ['MonteCarlo','MonteCarloFromGEN'], 't1' : ['ReDigi','ReReco'] ,'all' :['MonteCarlo','MonteCarloFromGEN','ReDigi','ReReco'] }
reqmgrsocket='vocms204.cern.ch'
dashost = 'https://cmsweb.cern.ch'
cachedasage=60
overview = ''
count = 1

def getoverview():
	global overview
	return overview

def getnewoverview():
        c = 0
        s = ''
        while c < 5:
                conn  =  httplib.HTTPSConnection(reqmgrsocket, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                r1=conn.request("GET",'/reqmgr/monitorSvc/requestmonitor')
                r2=conn.getresponse()
                print r2.status, r2.reason
                if r2.status == 500: 
                        c = c + 1
                        print "retrying"
                else:   
                        c = 100 
                        s = json.loads(r2.read())
                        conn.close()
        return s

def getRequestsByTeamStatus(team,status):
	typelist = team2type[team]
	s = getoverview()
	r = []
	for i in s:
		t = ''
		if 'type' in i.keys():
			t = i['type']
		if 'status' in i.keys():
			st = i['status']
		if t in typelist and st in status:
			r.append(i['request_name'])
	return r
	
def getPhEDExRequestInfo(datasetName):
	info = {}
        url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/RequestList?dataset=' + datasetName
	try:
        	result = json.load(urllib.urlopen(url))
	except:
		print "Cannot get subscription status from PhEDEx"
		return None

	try:
		r = result['phedex']['request']
	except:
		return None
	for i in range(0,len(r)):
        	approval = r[i]['approval']
        	requested_by = r[i]['requested_by']
		custodialsite = r[i]['node'][0]['name']
		id = r[i]['id']
		if 'T1_' in custodialsite:
			info['custodialsite'] = custodialsite
			info['requested_by'] = requested_by
			info['approval'] = approval
			info['id'] = id
			return info
	return None
			
def getRequestByrequestName(workflow):
	#try:
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	s = json.loads(data)
	conn.close()
	#except:
	#	print "Cannot get request (getRequestByrequestName) " 
	#	sys.exit(1)
	return s

def inputdatasetWorkflow(jreq,workflow):
	try:
		dataset = jreq['InputDatasets']
	except:
		print "No Inpudatasets for this workflow: "+workflow
		return ''
        return dataset[0]

def main():
        args=sys.argv[1:]
        if len(args) == 1:
           state=args[0]
        else:
           state='assignment-approved'

	global overview,count
	overview = getnewoverview()
	status = [state]
	list = getRequestsByTeamStatus('t1',status)
	list.sort()

	for workflow in list:
           jreq = getRequestByrequestName(workflow)
           ids = inputdatasetWorkflow(jreq,workflow)
           print workflow,ids
        sys.exit(0)

if __name__ == "__main__":
        main()
