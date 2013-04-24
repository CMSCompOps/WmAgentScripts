#!/usr/bin/env python
#TODO select zone
#TODO analysis (running >=95%)
#TODO merge -p -t -s
import sys,urllib,urllib2,re,time,os
try:
    import json
except ImportError:
    import simplejson as json
import optparse
import httplib
import datetime
import time
import shutil

eras = ['Summer11','Summer12']
tftiers = ['GEN-SIM','GEN-SIM-RECO','DQM','AODSIM']
#tftiers = ['GEN-SIM','GEN-SIM-RECO','DQM','AODSIM','GEN-SIM-DIGI-RECO']
dashost = 'https://cmsweb.cern.ch'
reqmgrsocket='vocms204.cern.ch'
typelist = ['MonteCarlo','MonteCarloFromGEN','ReReco','ReDigi']
statuslist = ['assignment-approved','acquired','running','completed','closed-out','announced']
#cachedoverview = '/tmp/' + os.environ['USER'] + '/overview.cache'
cachedoverview = '/afs/cern.ch/user/s/spinoso/public/overview.cache'
forceoverview = 0
sum = {}
sum['events'] = {'PRODUCTION':0,'VALID':0,'INVALID':0}

def addToSummary(r):
	global sum
	for i in ['queued','cooloff','pending','running','success','failure','inWMBS','total_jobs']:
		#print "*** %s" % r
		if i in sum.keys():
			sum[i] = sum[i] + r['js'][i]
		else:
			sum[i] = r['js'][i]
	for i in ['expectedevents','cpuhours','remainingcpuhours']:
		if i not in sum.keys():
			sum[i] = 0
		sum[i] = sum[i] + r[i]
	for i in ['status','team','zone','priority']:
		if i not in sum.keys():
			sum[i] = {}
		if r[i] not in sum[i].keys():
			sum[i][r[i]] = 1
		else:
			sum[i][r[i]] = sum[i][r[i]] + 1
	for o in r['outputdataset']:
		if 'status' in o.keys() and 'events' in o.keys():
			if o['status'] in ['PRODUCTION','VALID','INVALID']:
				sum['events'][o['status']] = o['events'] + sum['events'][o['status']]
			
	
	
def getRequestsByTypeStatus(type,status):
	global overview
	r = []
	for i in overview:
		if 'type' in i.keys():
			t = i['type']
		if 'status' in i.keys():
			st = i['status']
		if t in type and st in status:
			r.append(i['request_name'])
	return r
	
def getRequestsByPREPID(prepid):
	r = []
	for i in overview:
		if prepid in i['request_name']:
			r.append(i['request_name'])
	return r
	
def getzonebyt1(s):
	custodial = '?'
	if not s:
		return custodial
	t1list = {'T1_CH_CERN':'CERN','T1_FR_CCIN2P3':'IN2P3','T1_TW_ASGC':'ASGC','T1_IT_CNAF':'CNAF','T1_US_FNAL':'FNAL','T1_DE_KIT':'KIT','T1_ES_PIC':'PIC','T1_UK_RAL':'RAL'}
	for i in t1list.keys():
		if i in s:
			custodial = t1list[i]
	return custodial

def getWorkflowInfo(workflow,nodbs=0):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/view/showWorkload?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	conn.close()
	list = data.split('\n')

	primaryds = ''
	priority = -1
	timeev = 0
	sizeev = 0
	prepid = ''
	globaltag = ''
	sites = []
	events_per_job = 0
	lumis_per_job = 0
	acquisitionEra = None
	processingVersion = None
	for raw in list:
		if 'acquisitionEra' in raw:
                        a = raw.find("'")
                        if a >= 0:
                                b = raw.find("'",a+1)
                                acquisitionEra = raw[a+1:b]
                        else:
                                a = raw.find(" =")
                                b = raw.find('<br')
                                acquisitionEra = raw[a+3:b]
		elif 'primaryDataset' in raw:
			primaryds = raw[raw.find("'")+1:]
			primaryds = primaryds[0:primaryds.find("'")]
		elif 'cmsswVersion' in raw:
			cmssw = raw[raw.find("'")+1:]
			cmssw = cmssw[0:cmssw.find("'")]
		elif 'PrepID' in raw:
			prepid = raw[raw.find("'")+1:]
			prepid = prepid[0:prepid.find("'")]
		elif 'lumis_per_job' in raw:
			a = raw.find(" =")
			b = raw.find('<br')
			lumis_per_job = int(raw[a+3:b])
		elif 'events_per_job' in raw:
			a = raw.find(" =")
			b = raw.find('<br')
			events_per_job = int(raw[a+3:b])
		elif 'SizePerEvent' in raw:
                        a = raw.find("'")
                        if a >= 0:
                                b = raw.find("'",a+1)
                                sizeev = int(raw[a+1:b])
                        else:
                                a = raw.find(" =")
                                b = raw.find('<br')
                                sizeev = int(float(raw[a+3:b]))
		elif 'TimePerEvent' in raw:
                        a = raw.find("'")
                        if a >= 0:
                                b = raw.find("'",a+1)
                                timeev = int(raw[a+1:b])
                        else:
                                a = raw.find(" =")
                                b = raw.find('<br')
                                timeev = int(float(raw[a+3:b]))
		elif 'request.priority' in raw:
			a = raw.find("'")
			if a >= 0:
				b = raw.find("'",a+1)
				priority = int(raw[a+1:b])
			else:
				a = raw.find(" =")
				b = raw.find('<br')
				#print "*%s*" % raw[a+3:b]
				priority = int(raw[a+3:b])
		elif 'RequestDate' in raw:
			reqdate = raw[raw.find("[")+1:raw.find("]")]	
			reqdate = reqdate.replace("'","")
			reqdate= "datetime.datetime(" + reqdate + ")"
			reqdate= eval(reqdate)
			requestdays = (datetime.datetime.now()-reqdate).days
		elif 'white' in raw and not '[]' in raw:
			sites = '['+raw[raw.find("[")+1:raw.find("]")]+']'	
			sites = eval(sites)		
		elif 'processingVersion' in raw:
			processingVersion = raw[raw.find("'")+1:]
			processingVersion = processingVersion[0:processingVersion.find("'")]
                        a = raw.find("'")
                        if a >= 0:
                                b = raw.find("'",a+1)
                                processingVersion = raw[a+1:b]
                        else:
                                a = raw.find(" =")
                                b = raw.find('<br')
                                processingVersion = raw[a+3:b]
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
		filtereff = 1
	try:
		team = s['Assignments']
		if len(team) > 0:
			team = team[0]
		else:
			team = ''
	except:
		team = ''
	try:
		type = s['RequestType']
	except:
		type = ''
	try:
		status = s['RequestStatus']
	except:
		status = ''
	try:
                reqevts = s['RequestSizeEvents']
        except:
                try:
                        reqevts = s['RequestNumEvents']
                except:
                        reqevts = 0
	inputdataset = {}
	try:
		inputdataset['name'] = s['InputDatasets'][0]
	except:
		pass
	
	if type in ['MonteCarlo']:
		expectedevents = int(reqevts)
		expectedjobs = int(expectedevents/(events_per_job*filtereff))
		expectedjobcpuhours = int(timeev*(events_per_job*filtereff)/3600)
	elif type in ['MonteCarloFromGEN','ReReco','ReDigi']:
		if nodbs:
			[inputdataset['events'],inputdataset['status']] = [0,'']
		else:
			[inputdataset['events'],inputdataset['status']] = getdsdetail(inputdataset['name'])
		if nodbs:
			inputdataset['lumicount'] = 0
		else:
			inputdataset['lumicount'] = dbs_get_lumicount(inputdataset['name'])
		try:
			expectedjobs = inputdataset['lumicount']/lumis_per_job
		except:
			expectedjobs = 0
		expectedevents = int(filtereff*inputdataset['events'])
		try:
			expectedjobcpuhours = int(lumis_per_job*timeev*inputdataset['events']/inputdataset['lumicount']/3600)
		except:
			expectedjobcpuhours = 0
	else:
		expectedevents = -1
		expectedjobs = -1
		expectedjobcpuhours = -1
	
	expectedtotalsize = sizeev * expectedevents / 1000000
	j = {}
	k = {'success':'success','failure':'failure','Pending':'pending','Running':'running','cooloff':'cooloff','pending':'queued','inWMBS':'inWMBS','total_jobs':'total_jobs','local_queue':'local_queue'}
	for r in overview:
		if r['request_name'] == workflow:
			break
	if r:
		for k1 in k.keys():
			k2 = k[k1]
			if k1 in r.keys():
				j[k2] = r[k1]
				j[k2]
			else:
				if k2 == 'local_queue':
					j[k2] = ''
				else:
					j[k2] = 0
	else:
		print " getjobsummary error: No such request: %s" % workflow
		sys.exit(1)
	
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	s = json.loads(data)
	conn.close()
	ods = s
        if len(ods)==0:
                print "No Outpudatasets for this workflow: "+workflow
	outputdataset = []
	eventsdone = 0
	for o in ods:
		oel = {}
		oel['name'] = o
		if 1:
			if nodbs:
				[oe,ost] = [0,'']
			else:
				[oe,ost] = getdsdetail(o)
			oel['events'] = oe
			oel['status'] = ost
		
			phreqinfo = {}
       		 	url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/RequestList?dataset=' + o
			try:
        			result = json.load(urllib.urlopen(url))
			except:
				print "Cannot get subscription status from PhEDEx"
			try:
				r = result['phedex']['request']
			except:
				r = None
			for i in range(0,len(r)):
       			 	approval = r[i]['approval']
 			       	requested_by = r[i]['requested_by']
				custodialsite = r[i]['node'][0]['name']
				id = r[i]['id']
				if 'T1_' in custodialsite:
					phreqinfo['custodialsite'] = custodialsite
					phreqinfo['requested_by'] = requested_by
					phreqinfo['approval'] = approval
					phreqinfo['id'] = id
			oel['phreqinfo'] = phreqinfo
		
			phtrinfo = {}
			url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions?dataset=' + o
			try:
	       		 	result = json.load(urllib.urlopen(url))
			except:
				print "Cannot get transfer status from PhEDEx"
			try:
				r = result['phedex']['dataset'][0]['subscription']
			except:
				r = []
			for i in r:
				node = i['node']
				custodial = i['custodial']
				if 'T1_' in node and custodial == 'y': 
					if i['move'] == 'n':
						phtype = 'Replica'
					else:
						phtype = 'Move'
					phtrinfo['node'] = node
					phtrinfo['time_create'] = datetime.datetime.fromtimestamp(int(i['time_create']))
					phtrinfo['time_create_days'] = (datetime.datetime.now() - phtrinfo['time_create']).days
					try:
						phtrinfo['perc'] = int(float(i['percent_bytes']))
					except:
						phtrinfo['perc'] = 0
					phtrinfo['type'] = phtype
			oel['phtrinfo'] = phtrinfo
			outputdataset.append(oel)
		eventsdone = eventsdone + oe

	cpuhours = timeev*expectedevents/3600
	remainingcpuhours = timeev*(expectedevents-eventsdone)/3600
	return {'filtereff':filtereff,'type':type,'status':status,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'sizeev':sizeev,'priority':priority,'sites':sites,'custodialt1':custodialt1,'zone':getzonebyt1(custodialt1),'js':j,'outputdataset':outputdataset,'cpuhours':cpuhours,'remainingcpuhours':remainingcpuhours,'team':team,'acquisitionEra':acquisitionEra,'requestdays':requestdays,'processingVersion':processingVersion,'events_per_job':events_per_job,'lumis_per_job':lumis_per_job,'expectedjobs':expectedjobs,'expectedjobcpuhours':expectedjobcpuhours,'cmssw':cmssw,'expectedtotalsize':expectedtotalsize}

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
	global cachedoverview,forceoverview
        cacheoverviewage = 180
        if not os.path.exists(cachedoverview) or forceoverview or (os.path.exists(cachedoverview) and ( (time.time()-os.path.getmtime(cachedoverview)>cacheoverviewage*60))):
		print "Reloading cache overview"
                s = getnewoverview()
                os.remove(cachedoverview)
                output = open(cachedoverview, 'w')
                output.write("%s" % s)
                output.close()
        else:
                d = open(cachedoverview).read()
                s = eval(d)
        return s

def getnewoverview():
	global cachedoverview
	c = 0
	while c < 10:
		try:
			conn  =  httplib.HTTPSConnection(reqmgrsocket, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
			r1=conn.request("GET",'/reqmgr/monitorSvc/requestmonitor')
			r2=conn.getresponse()
			#print r2.status, r2.reason
			if r2.status == 500:
				c = c + 1
				#print "retrying... ",
			else:
				c = 100
			s = json.loads(r2.read())
			conn.close()
		except :
			print "Cannot get overview [1]" 
                	os.remove(cachedoverview)
			sys.exit(1)
	if s:
		return s
	else:
		print "Cannot get overview [2]"
                os.remove(cachedoverview)
		sys.exit(2)


def getdsdetail(dataset):
	[e,st] = dbs_get_data(dataset)
	if e == -1:
		return [0,'',0]
	else:
		return [e,st]

def dbs_get_lumicount(dataset):
	q = "/afs/cern.ch/user/s/spinoso/public/dbssql --input='find count(lumi) where dataset="+dataset+"'"
	output=os.popen(q+ "|awk -F \"'\" '/count_lumi/{print $4}'").read()
	ret = output.split(' ')
	try:
		lc = ret[0].rstrip()
	except:
		lc = 0
	return int(lc)

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

def getacqera(r):
	prepid = r['prepid']
	return prepid.split('-')[1]

def changeSplittingWorkflow(url, workflow, split,RequestType):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        if RequestType=='MonteCarlo':
                params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/Production", "splittingAlgo":"EventBased", "events_per_job":str(split), "timeout":""}
        elif RequestType=='MonteCarloFromGEN':
                params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/MonteCarloFromGEN", "splittingAlgo":"LumiBased", "lumis_per_job":str(split), "timeout":"", "include_parents":"False", "files_per_job":"",'halt_job_on_file_boundaries':'True'}
        headers={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        encodedParams = urllib.urlencode(params)
        conn.request("POST", "/reqmgr/view/handleSplittingPage", encodedParams, headers)
        response = conn.getresponse()
        #print response.status, response.reason
        data = response.read()
        #print data
        conn.close()

def main():
	global overview,forceoverview,sum
	url='cmsweb.cern.ch'
	overview = getoverview()

	listtype = ['MonteCarloFromGEN']
	liststatus = ['assignment-approved']
	list = getRequestsByTypeStatus(listtype,liststatus)

	list.sort()
	print "Number of assignment-approved MonteCarloFromGEN requests: %s" % len(list)
	reqinfo = {}

	print "Getting requests: "
	for workflow in list:
		r = getWorkflowInfo(workflow)
		if r['status'] == 'assignment-approved':
			reqinfo[workflow] = r
			if reqinfo[workflow]['type'] == 'MonteCarloFromGEN':
				h = float(reqinfo[workflow]['timeev'])*reqinfo[workflow]['inputdataset']['events']/reqinfo[workflow]['inputdataset']['lumicount']/3600 # h hours per lumi
			
				#print "h = %s * %s / %s / 3600 = %s" % (reqinfo[workflow]['timeev'],reqinfo[workflow]['inputdataset']['events'],reqinfo[workflow]['inputdataset']['lumicount'],h)
				if 'FS52' in r['prepid'] or 'FS53' in r['prepid']:
					ljmax = 100
				else:
					ljmax = 3
				lj = min(int(round(8/h)),ljmax)
				if lj < 1:
					lj = 1
				if lj != reqinfo[workflow]['lumis_per_job']:
					flag = "FIX! %s -> %s => %s(h/job)" % (reqinfo[workflow]['lumis_per_job'],lj,round(lj*h))
       	 				changeSplittingWorkflow(url, workflow, lj,reqinfo[workflow]['type'])
				else:
					flag = 'OK'
				print "%s %s %s(lumis/job) %s(h/lumis) -> %s(h/job)%s" % (workflow,reqinfo[workflow]['type'],reqinfo[workflow]['lumis_per_job'],round(h,1),round(reqinfo[workflow]['lumis_per_job']*h),flag) 
	print
	
        sys.exit(0)

if __name__ == "__main__":
        main()

