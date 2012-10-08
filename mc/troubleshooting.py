#!/usr/bin/env
#TODO select zone
#TODO analysis (running >=95%)
#TODO merge -p -t -s
import sys,urllib,urllib2,re,time,os
import simplejson as json
import optparse
import httplib
import datetime
import time
import shutil
from datetime import timedelta
from datetime import date

dashost = 'https://cmsweb.cern.ch'
reqmgrsocket='vocms204.cern.ch'
overview = ''
typelist = ['MonteCarlo','MonteCarloFromGEN','ReReco','ReDigi']
statuslist = ['assignment-approved','acquired','running','completed','closed-out','announced']

def getRequestsByTypeStatus(type,status):
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
		#reqprepid = getPrepID(i['request_name'])
		if prepid in i['request_name']:
			r.append(i['request_name'])
	return r
	
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
	datafmt = '%Y %m %d %H %M %S'

	for raw in list:
		if 'acquisitionEra' in raw:
			acquisitionEra = raw[raw.find("'")+1:]
			acquisitionEra = acquisitionEra[0:acquisitionEra.find("'")]
		elif 'primaryDataset' in raw:
			primaryds = raw[raw.find("'")+1:]
			primaryds = primaryds[0:primaryds.find("'")]
		elif 'PrepID' in raw:
			prepid = raw[raw.find("'")+1:]
			prepid = prepid[0:prepid.find("'")]
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
		team = s['Assignments']
	except:
		team = []
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
                        print "No RequestNumEvents for this workflow: "+workflow
                        return ''
	inputdataset = {}
	try:
		inputdataset['name'] = s['InputDatasets'][0]
	except:
		pass
	
	if type in ['MonteCarlo']:
		expectedevents = int(reqevts)
	elif type in ['MonteCarloFromGEN']:
		[inputdataset['events'],inputdataset['date'],inputdataset['status']] = getdsdetail(inputdataset['name'])
		expectedevents = int(filtereff*inputdataset['events'])
	else:
		expectedevents = -1
	
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
	for o in ods:
		oel = {}
		oel['name'] = o
		if status in ['running','completed','closed-out','announced']:
			[oe,od,ost] = getdsdetail(o)
			oel['events'] = oe
			oel['date'] = od
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

	duration = timeev*expectedevents/3600
	
	return {'filtereff':filtereff,'type':type,'status':status,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'priority':priority,'sites':sites,'custodialt1':custodialt1,'zone':getzonebyt1(custodialt1),'js':j,'outputdataset':outputdataset,'duration':duration,'team':team,'acquisitionEra':acquisitionEra,'reqdate':reqdate,'requestdays':requestdays}

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
	cacheoverviewage = 60
	cachedoverview = os.environ['HOME'] + '/public/overview.cache'
	#cachedoverview = '/tmp/' + os.environ['USER'] + '/overview.cache'
	if (os.path.exists(cachedoverview)) and (time.time()-os.path.getmtime(cachedoverview)>cacheoverviewage*60):
		os.remove(cachedoverview)
	if (not os.path.exists(cachedoverview)):
		s = getnewoverview()
		output = open(cachedoverview, 'w')
		output.write("%s" % s)
		output.close()
	else:
		d = open(cachedoverview).read()
		s = eval(d)
	return s

def getnewoverview():
	c = 0
	#print "Getting overview..",
	while c < 3:
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
			sys.exit(1)
	if s:
		return s
	else:
		print "Cannot get overview [2]"
		sys.exit(1)


def getdsdetail(dataset):
	[e,d,st] = dbs_get_data(dataset)
	if e == -1:
		return [0,0,'']
	else:
		return [e,d,st]

def dbs_get_data(dataset):
	output=os.popen("/afs/cern.ch/user/s/spinoso/public/dbssql --input='find sum(block.numevents),max(block.moddate),dataset.status where dataset="+dataset+"'"+ "|grep '[0-9]\{1,\}'").read()
	ret = output.split(' ')
	try:
		e = int(ret[0])
	except:
		e = 0
	try:
		d = int(ret[1])
	except:
		d = 0
	try:
		st = ret[2].rstrip()
	except:
		st = ''
	return [e,d,st]

def main():
	global overview
	
	parser = optparse.OptionParser()
	parser.add_option('-t', '--transfer', help='check for pending transfers on completed/closed-out',dest='transfer',action="store_true")
	parser.add_option('-n', '--no-running', help='check for running requests with 0 running',dest='norunning',action="store_true")
	parser.add_option('-e', '--enough-events', help='check for running requests having >=90% events',dest='enough',action="store_true")
	parser.add_option('-c', '--close', help='running, transfer 100%% and dataset >=95%%+PRODUCTION status, can be set as VALID',dest='close',action="store_true")
	parser.add_option('-s', '--stuck', help='check for stuck requests (dataset moddate older than 1 week)',dest='stuck',action="store_true")
	parser.add_option('-o', '--old', help='check for requests with very old injection date',dest='old',action="store_true")
	parser.add_option('-l', '--lessevents', help='check for less events than expected',dest='lessevents',action="store_true")

	(options,args) = parser.parse_args()

	overview = getoverview()
	print
	reqinfo = {}

	if options.transfer: #completed/closed-out requests not proceeding in PhEDEx transfer
		print "Workflows in completed/closed-out having PhEDEx transfer pending:\n"
		list = getRequestsByTypeStatus(['MonteCarlo','MonteCarloFromGEN'],['completed','closed-out'])
		for w in list:
			r = getWorkflowInfo(w)
			for o in r['outputdataset']:
				if 'perc' in o['phtrinfo'].keys():
					if o['phtrinfo']['perc'] < 100 and o['phtrinfo']['time_create_days'] > 8:
						print "%s (%s/%s %s %s%% https://cmsweb.cern.ch/phedex/prod/Request::View?request=%s)" % (w,o['phtrinfo']['time_create'].strftime('%b %d'),r['custodialt1'],o['name'],o['phtrinfo']['perc'],o['phreqinfo']['id'])

	elif options.lessevents: # 
		for s in ['completed','running']:
			print "Workflows in status '%s' having less events than expected\n" % s
			list = getRequestsByTypeStatus(['MonteCarlo','MonteCarloFromGEN'],[s])
			for w in list:
				r = getWorkflowInfo(w)
				expectedevents = r['expectedevents']
				inWMBS = r['js']['inWMBS']
				if inWMBS > 0:
					expectedeventsperjob = expectedevents/inWMBS
					successfuljobs = r['js']['success']
					failedjobs = r['js']['failure']
					if r['js']['total_jobs'] > 0:
						p = float(successfuljobs)/r['js']['total_jobs']
					else:
						p = 0
					if p > .9: # 90% of the jobs are done
						for o in r['outputdataset']:
							events = o['events']
							eventsperjob = events/successfuljobs
							ratio = float(eventsperjob)/expectedeventsperjob
							if ratio < .80:
								print "%s (%s%% of expected events/job)" % (w,100*round(ratio,2))
						jratio = float(successfuljobs-failedjobs)/successfuljobs
						if jratio < .80:
							print "%s (job failures: %s)" % (w,round(jratio,2))
						

	elif options.norunning: # running with 0 jobs running
		print "Workflows running and having no queued/pending/running jobs:\n"
		list = getRequestsByTypeStatus(['MonteCarlo','MonteCarloFromGEN'],['running'])
		for w in list:
			r = getWorkflowInfo(w)
			if r['js']['running'] + r['js']['pending'] + r['js']['queued'] == 0:
				print "%s" % (w)

	elif options.enough: # enough events
		print "Workflows running, dataset >95%, PRODUCTION:\n"
		list = getRequestsByTypeStatus(['MonteCarlo','MonteCarloFromGEN'],['running'])
		for w in list:
			r = getWorkflowInfo(w)
			expectedevents = r['expectedevents']
			for o in r['outputdataset']:
				perc = int(100*o['events']/expectedevents)
				if perc > 95 and o['status'] == 'PRODUCTION':
					if 'phtrinfo' in o.keys():
						if 'perc' in o['phtrinfo']:
							print "%s dataset:%s%% transfer:%s%%" % (w,perc,o['phtrinfo']['perc'])
						else:
							print "%s dataset:%s%% transfer:NO" % (w,perc)

	elif options.close: # close
		print "Workflows running, dataset >95%, transfer 100% and PRODUCTION: dataset could be set as VALID:\n"
		list = getRequestsByTypeStatus(['MonteCarlo','MonteCarloFromGEN'],['running'])
		ds = []
		for w in list:
			r = getWorkflowInfo(w)
			expectedevents = r['expectedevents']
			for o in r['outputdataset']:
				perc = int(100*o['events']/expectedevents)
				if perc > 95 and o['status'] == 'PRODUCTION':
					if 'phtrinfo' in o.keys():
						if 'perc' in o['phtrinfo']:
							if o['phtrinfo']['perc'] == 100:
								print "%s" % (w)
								ds.append(o['name'])
		print
		print "The corresponding datasets can be safely set as VALID:\n"
		for d in ds:
			print d

	elif options.stuck:
		n = 7
		print "Workflows running with output dataset stuck since %s days:\n" % n
		list = getRequestsByTypeStatus(['MonteCarlo','MonteCarloFromGEN'],['running'])
		now = datetime.datetime.now()
		d = timedelta(days=n)
		for w in list:
			r = getWorkflowInfo(w)
			for o in r['outputdataset']:
				if o['date'] == 0:	
					continue
				t = now-datetime.datetime.fromtimestamp(o['date'])
				if ( t > d and r['status'] in ['running']):
					print "%s (%s days)" % (w,d.days)
					continue
		print

	elif options.old: 
		print "Workflows injected long time ago and still running/completed" 
		list = getRequestsByTypeStatus(['MonteCarlo','MonteCarloFromGEN'],['running','completed'])
		for w in list:
			r = getWorkflowInfo(w)
			if r['requestdays'] > 14:
				print "%s (%s days)" % (w,r['requestdays'])
		print

	print
        sys.exit(0)

if __name__ == "__main__":
        main()
