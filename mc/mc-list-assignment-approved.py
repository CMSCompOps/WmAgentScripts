#!/usr/bin/env python -w
import sys,urllib,urllib2,re,time,os
import simplejson as json
import optparse
import httplib
import datetime
import shutil
import optparse

dashost = 'https://cmsweb.cern.ch'
reqmgrsocket='vocms204.cern.ch'
overview = ''
count = 1

def getzonebyt1(s):
	custodial = '?'
	if not s:
		return custodial
	t1list = {'T1_FR_CCIN2P3':'IN2P3','T1_TW_ASGC':'ASGC','T1_IT_CNAF':'CNAF','T1_US_FNAL':'FNAL','T1_DE_KIT':'KIT','T1_ES_PIC':'PIC','T1_UK_RAL':'RAL'}
	for i in t1list.keys():
		if i in s:
			custodial = t1list[i]
	return custodial

def getWorkflowInfo(workflow):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/view/showWorkload?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	conn.close()
	list = data.split('\n')

	primaryds = ''
	priority = -1
	timeev = -1
	prepid = ''
	globaltag = ''
	sites = []
	for raw in list:
		if 'primaryDataset' in raw:
			primaryds = raw[raw.find("'")+1:]
			primaryds = primaryds[0:primaryds.find("'")]
		elif 'PrepID' in raw:
			prepid = raw[raw.find("'")+1:]
			prepid = prepid[0:prepid.find("'")]
		elif 'TimePerEvent' in raw:
			a = raw.find("'")
			if a >= 0:
				b = raw.find("'",a+1)
				timeev = int(float(raw[a+1:b]))
			else:
				a = raw.find(" =")
				b = raw.find('<br')
				timeev = int(float(raw[a+3:b]))
			if timeev < 0:
				timeev = 0
		elif 'request.priority' in raw:
			a = raw.find("'")
			if a >= 0:
				b = raw.find("'",a+1)
				priority = int(raw[a+1:b])
			else:
				a = raw.find(" =")
				b = raw.find('<br')
				priority = int(raw[a+3:b])
		elif 'white' in raw and not '[]' in raw:
			sites = '['+raw[raw.find("[")+1:raw.find("]")]+']'	
			sites = eval(sites)		
		elif 'request.schema.GlobalTag' in raw:
			globaltag = raw[raw.find("'")+1:]
			globaltag = globaltag[0:globaltag.find(":")]
	custodialt1 = '?'
	for i in sites:
		if 'T1_' in i:
			custodialt1 = i
			break

	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	s = json.loads(data)
	conn.close()
	try:
		filtereff = float(s['FilterEfficiency'])
	except:
		filtereff = -1
	try:
		type = s['RequestType']
	except:
		type = ''
	try:
		status = s['RequestStatus']
	except:
		status = ''
	try:
		reqsize = s['RequestNumEvents']
	except:
		try:
			reqsize = s['RequestSizeEvents']
		except:
			reqsize = -1
	try:
		inputdataset = s['InputDatasets'][0]
	except:
		inputdataset = ''
	
	if type in ['MonteCarlo']:
		expectedevents = int(reqsize)
	elif type in ['MonteCarloFromGEN']:
		[ie,ist] = getdsdetail(inputdataset)
		expectedevents = int(filtereff*ie)
	else:
		expectedevents = -1
	
	duration = timeev*expectedevents/3600
	return {'filtereff':filtereff,'type':type,'status':status,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'priority':priority,'sites':sites,'custodialt1':custodialt1,'zone':getzonebyt1(custodialt1),'duration':duration}

def getpriorities(reqinfo):
	priorities = []
	for i in reqinfo.keys():
		if not reqinfo[i]['priority'] in priorities:
			priorities.append(reqinfo[i]['priority'])
	priorities.sort(reverse=True)
	return priorities

def getrequestsByPriority(reqinfo,priority):
	requests = []
	for i in reqinfo.keys():
		if reqinfo[i]['priority'] == priority:
			requests.append(i)
	requests.sort()
	return requests

def getoverview():
	c = 0
	print "Getting overview..",
	while c < 3:
		try:
			conn  =  httplib.HTTPSConnection(reqmgrsocket, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
			r1=conn.request("GET",'/reqmgr/monitorSvc/requestmonitor')
			r2=conn.getresponse()
			print r2.status, r2.reason
			if r2.status == 500:
				c = c + 1
				print "retrying... ",
			else:
				c = 100
			s = json.loads(r2.read())
			conn.close()
		except :
			print "Cannot get overview [1]" 
			sys.exit(1)
	if s:
		return s
	else:
		print "Cannot get overview [2]"
		sys.exit(1)

def getRequestsByTypeStatus(typelist,status):
	global overview
	r = []
	for i in overview:
		t = ''
		st = ''
		if 'type' in i.keys():
			t = i['type']
		if 'status' in i.keys():
			st = i['status']
		if t in typelist and st in status:
			r.append(i['request_name'])
	return r
	
def getdsdetail(dataset):

	[e,st] = dbs_get_data(dataset)
	if e == -1:
		return [0,'']
	else:
		return [e,st]

def dbs_get_data(dataset):
	q = "/afs/cern.ch/user/s/spinoso/public/dbssql --input='find sum(block.numevents),dataset.status where dataset="+dataset+"'"
	output=os.popen(q+ "|grep '[0-9]\{1,\}'").read()
	ret = output.split(' ')
	try:
		e = int(ret[0])
	except:
		e = 0
	try:
		st = ret[1].rstrip()
	except:
		st = ''
	return [int(e),st]

def getnextprocessingversion(r):
	c = 0
	[e,st] = [1,'xxx']
	y = 0
	for i in r['ods']:
		if 'GEN-SIM' in i:
			y = 1
			break
	if y:
		while e > 0:
			acqera = getacqera(r)
			c = c + 1
			nextoutputdataset = '/%s/%s-%s-v%s/GEN-SIM' % (r['primaryds'],acqera,r['globaltag'],c)
			#print nextoutputdataset
			[e,st] = getdsdetail(nextoutputdataset)
			#print [e,st]
		return '%s-v%s' % (r['globaltag'],c)
	else:
		return '-'

def getacqera(r):
	prepid = r['prepid']
	return prepid.split('-')[1]

def main():
	global overview,count,jobcount

	parser = optparse.OptionParser()
	parser.add_option('-w', '--workflow', help='workflow name',dest='workflow')
	parser.add_option('-l', '--workflowlist', help='workflow list in textfile',dest='list')
	(options,args) = parser.parse_args()

	overview = getoverview()

	list = []
	print
	if options.list:
		list = open(options.list).read().splitlines()
		for i, item in enumerate(list):
			list[i] = item.rstrip()
			list[i] = list[i].lstrip()
	elif options.workflow:
		list = [options.workflow]
	else:
		listtype = ['MonteCarlo','MonteCarloFromGEN']
		liststatus = ['assignment-approved']
		list = getRequestsByTypeStatus(listtype,liststatus)

	list.sort()
	print "Number of requests: %s" % len(list)
	reqinfo = {}

	for workflow in list:
		#print "%s" % (workflow) 
		#sys.stdout.flush()

		r = getWorkflowInfo(workflow)
		if r['status'] == 'assignment-approved':
			reqinfo[workflow] = r

	print

	priorities = getpriorities(reqinfo)
	for p in range(0,len(priorities)):
		for i in getrequestsByPriority(reqinfo,priorities[p]):
			#acqera = getacqera(reqinfo[i])
			#procversion = getnextprocessingversion(reqinfo[i])
			print "%s prio=%s events=%s cpuh=%s" %(i,reqinfo[i]['priority'],reqinfo[i]['expectedevents'],reqinfo[i]['duration'])

        sys.exit(0)

if __name__ == "__main__":
        main()
