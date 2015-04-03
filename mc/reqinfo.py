#!/usr/bin/env python
#TODO workflow summary https://cmsweb.cern.ch/couchdb/workloadsummary/alahiff_Run2012Cv1_536_130604_003624_5904
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
import math

eras = ['Summer11','Summer12']
tftiers = ['GEN-SIM','GEN-SIM-RECO','DQM','AODSIM']
dashost = 'https://cmsweb.cern.ch'
typelist = ['MonteCarlo','MonteCarloFromGEN','ReReco','ReDigi','LHEStepZero','TaskChain']
statuslist = ['assignment-approved','acquired','running','completed','closed-out','announced']
cachedoverview = '/afs/cern.ch/user/c/cmst2/public/overview.cache'
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

def getWorkloadSummary(w):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/couchdb/workloadsummary/' + w)
	r2=conn.getresponse()
	data = r2.read()
	s = json.loads(data)
	conn.close()
	#print s.keys()
	avgtimeev = 0
	overflow = 0
	if 'performance' not in s:
		return {}
	count = 0
	for i in s['performance']['/%s/Production' % w]['cmsRun1']['AvgEventTime']['histogram']:
		count = count + i['nEvents']
		#print "%s %.4f %s" % (i['type'],i['average'],i['nEvents'])
		if i['type'] == 'standard':
			avgtimeev = avgtimeev+i['nEvents']*i['average']
		elif i['type'] == 'overflow':
			overflow = overflow + 1
	if count == 0:
		return {}
	avgtimeev = avgtimeev / count
	#print "avgtimeev = %.4f overflow = %.0f%%" % (avgtimeev,100*overflow/count)
	#print s['performance']['/%s/Production' % w]['cmsRun1']['MinEventCPU']['average']
	#print s['performance']['/%s/Production' % w]['cmsRun1']['MaxEventTime']['average']
	return {'avgtimeev':avgtimeev,'count':count,'overflow':overflow}

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
	custodialsites = []
	events_per_job = 0
	lumis_per_job = 0
	acquisitionEra = None
	processingVersion = None
	campaign = ''
	requestdays=0
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
                elif '.schema.Campaign' in raw:
                        campaign = raw[raw.find("'")+1:]
                        campaign = campaign[0:campaign.find("'")]
		elif '.events_per_job' in raw:
			a = raw.find(" =")
			b = raw.find('<br')
			events_per_job = int(float(raw[a+3:b]))
		elif 'SizePerEvent' in raw:
                        a = raw.find("'")
                        if a >= 0:
                                b = raw.find("'",a+1)
                                sizeev = int(raw[a+1:b])
                        else:
                                a = raw.find(" =")
                                b = raw.find('<br')
                                sizeev = int(float(raw[a+3:b]))
                elif '.custodialSites' in raw and not '[]' in raw:
                        custodialsites = '['+raw[raw.find("[")+1:raw.find("]")]+']'
                        custodialsites = eval(custodialsites)
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
	
	if type in ['MonteCarlo','LHEStepZero']:
		expectedevents = int(reqevts)
		try:
			expectedjobs = int(expectedevents/(events_per_job*filtereff))
			expectedjobcpuhours = int(timeev*(events_per_job*filtereff)/3600)
		except:
			expectedjobs = 0
			expectedjobcpuhours = 0
	elif type in ['MonteCarloFromGEN','ReReco','ReDigi']:
		if nodbs:
			[inputdataset['events'],inputdataset['status']] = [0,'']
		else:
			[inputdataset['events'],inputdataset['status'],inputdataset['createts'],inputdataset['lastmodts'],inputdataset['lumicount']] = getdsdetail(inputdataset['name'])
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
		expectedevents = 0
		expectedjobs = 0
		expectedjobcpuhours = 0
	
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

	cpuhours = timeev*expectedevents/3600
	etah = 0
	for o in ods:
		oel = {}
		oel['name'] = o
		if 1:
			if nodbs:
				[oe,ost,ocreatets,olastmodts] = [0,'',0,0]
			else:
				[oe,ost,ocreatets,olastmodts,lumicount] = getdsdetail(o)
			oel['events'] = oe
			oel['status'] = ost
			oel['createts'] = ocreatets
			oel['lastmodts'] = olastmodts

			if oel['createts'] > 0 and j['running']>0:
				etah = etah + ( float(expectedevents) / oel['events'] - 1 ) * (oel['lastmodts']-oel['createts'])/3600/j['running']
			else:
				etah = 0
		
			phreqinfo = {}
       		 	url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/RequestList?dataset=' + o
			try:
        			result = json.load(urllib.urlopen(url))
			except:
				pass #print "Cannot get subscription status from PhEDEx"
			try:
				r = result['phedex']['request']
			except:
				r = None
			if r:
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
				pass #print "Cannot get transfer status from PhEDEx"
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
					phtrinfo['time_create'] = (datetime.datetime.fromtimestamp(int(i['time_create']))).ctime()
					phtrinfo['time_create_days'] = (datetime.datetime.now() - datetime.datetime.fromtimestamp(int(i['time_create']))).days
					try:
						phtrinfo['perc'] = int(float(i['percent_bytes']))
					except:
						phtrinfo['perc'] = 0
					phtrinfo['type'] = phtype
			oel['phtrinfo'] = phtrinfo
			outputdataset.append(oel)
		eventsdone = eventsdone + oe

        t2zone = {'T1_TW_ASGC':'ASGC','T1_IT_CNAF':'CNAF','T1_DE_KIT':'KIT','T1_FR_CCIN2P3':'IN2P3','T1_ES_PIC':'PIC','T1_UK_RAL':'RAL','T1_US_FNAL':'FNAL'}
        try:
                zone = t2zone[custodialsites[0]]
        except:
                zone = '?'
	remainingcpuhours = timeev*(expectedevents-eventsdone)/3600
	return {'requestname':workflow,'filtereff':filtereff,'type':type,'campaign':campaign,'status':status,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'sizeev':sizeev,'priority':priority,'sites':sites,'custodialsites':custodialsites,'zone':zone,'js':j,'outputdataset':outputdataset,'cpuhours':cpuhours,'etah':math.ceil(etah*10)/10,'remainingcpuhours':remainingcpuhours,'team':team,'acquisitionEra':acquisitionEra,'requestdays':requestdays,'processingVersion':processingVersion,'events_per_job':events_per_job,'lumis_per_job':lumis_per_job,'expectedjobs':expectedjobs,'expectedjobcpuhours':expectedjobcpuhours,'cmssw':cmssw,'expectedtotalsize':expectedtotalsize}

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

def getnewoverview_DEPREC():
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
	#[e,st,createts,lastmodts] = das_get_data(dataset,timestamps)
	[e,st,createts,lastmodts,lumicount] = dbs3_get_data(dataset)
	if e == -1:
		return [0,'',0,0,0]
	else:
		return [e,st,createts,lastmodts,lumicount]

def das_get_data(dataset,timestamps=1):
        q = 'python26 /afs/cern.ch/user/c/cmst2/mc/external/das_cli.py --query "dataset dataset=%s|grep dataset.status,dataset.nevents" --format=json' % dataset
        output=os.popen(q).read()
        output = output.rstrip()
        if output == '' or output == '[]' or (type(output) == dict and 'status' in output.keys() and output['status']=='fail'):
                return [0, '', 0, 0] # dataset is not in DBS
        tmp = eval(output)
        if tmp:
                if 'dataset' in tmp[0].keys():
                        for i in tmp[0]['dataset']:
                                if i:
                                        break
                        events = i['nevents']
                        status = i['status']
        createts = 0
        lastmodts = 0
        if timestamps:
                q = 'python26 /afs/cern.ch/user/c/cmst2/mc/external/das_cli.py --query "block dataset=%s | min(block.creation_time),max(block.modification_time)"|grep "(block"' % dataset
                output=os.popen(q).read()
                output = output.rstrip()
                lines = output.split('\n')
                for line in lines:
                        if 'min' in line:
                                createts = int(line.split('=')[1])
                        elif 'max' in line:
                                lastmodts = int(line.split('=')[1])
        ret = [int(events),status,int(createts),int(lastmodts)]
        return ret


def dbs3_get_data(dataset):
	q = "/afs/cern.ch/user/c/cmst2/mc/scripts/dbs3wrapper.sh /afs/cern.ch/user/c/cmst2/mc/scripts/datasetinfo.py --dataset %s --json" % dataset
	#print q
	output=os.popen(q).read()
	#print ">%s<" % output
	s = json.loads(output)
	try:
		e = s['num_event']
	except:
		e = 0
	try:
		st = s['dataset_access_type']
	except:
		st = ''
	try:
		createts = s['creation_date']
	except:
		createts = 0
	try:
		lastmodts = s['last_modification_date']
	except:
		lastmodts = 0
	try:
		num_lumi = s['num_lumi']
	except:
		num_lumi = 0

	return [e,st,createts,lastmodts,num_lumi]

def getacqera(r):
	prepid = r['prepid']
	return prepid.split('-')[1]

def main():
	global overview,forceoverview,sum
	
	viewchoices = ['names','all','production','dataset','run','assignment']
	parser = optparse.OptionParser()
	parser.add_option('-l', '--listfile', help='analyze workflows listed in textfile',dest='list')
	parser.add_option('-m', help='print a brief report at the end',dest='mreport',action="store_true")
	parser.add_option('-w', '--workflow', help='analyze specific workflow',dest='wf')
	parser.add_option('-p', '--prepid', help='analyze workflow with PREPID',dest='prepid')
	parser.add_option('-s', '--status', help='analyze workflow in status STATUS',dest='status')
	parser.add_option('-t', '--type', help='analyze workflow of type TYPE',dest='type')
	parser.add_option('-n', '--names', help='print just request names',dest='names',action="store_true")
	parser.add_option('-a', '--all', help='print all information about the requests',dest='raw',action="store_true")
	parser.add_option('--summary', help='print summary',dest='summary',action="store_true")
	parser.add_option('--csv', help='print all information about the requests in CSV format',dest='csv',action="store_true")
	parser.add_option('-x', '--export', help='export all information about the requests in JSON format',dest='json',action="store_true")
	parser.add_option('-d', '--datasets', help='print just output datasets',dest='datasets',action="store_true")
	parser.add_option('-b', '--no-dbs', help='don\'t contact DBS (faster)',dest='nodbs',action="store_true")
	parser.add_option('-j', '--jobs', help='print just information useful in workflow management context',dest='jobs',action="store_true")
	parser.add_option('-e', '--events', help='print just information useful in user reporting',dest='events',action="store_true")
	parser.add_option('--tapefamilies', help='print tape families',dest='tapefamilies',action="store_true")

	(options,args) = parser.parse_args()

	overview = getoverview()

	if options.wf:
		list = [options.wf]
	elif options.list:
		list2 = open(options.list).read().splitlines()
		list = []
		for prepid in list2:
			i = prepid.strip()
			for j in getRequestsByPREPID(i):
				if not j in list:
					list.append(j)
	elif options.prepid:
		list = getRequestsByPREPID(options.prepid)
	elif options.status or options.type:
		rtype = typelist
		if options.type:
			rtype = options.type.split(',')
		rstatus = statuslist
		if options.status:
			rstatus = options.status.split(',')
		list = getRequestsByTypeStatus(rtype,rstatus)
	else:
		print "List not provided."
		sys.exit(1)
	list.sort()
		
	reqinfo = {}
	if options.nodbs:
		nodbs = 1
	else:	
		nodbs = 0

	if options.names:
		for w in list:
			print w
	elif options.summary:
		for w in list:
			print w
			print getWorkloadSummary(w)
		sys.exit(0)
	elif options.json:
		struct = []
		for workflow in list:
			reqinfo[workflow] = getWorkflowInfo(workflow,nodbs=nodbs)
			struct.append(reqinfo[workflow])
		print json.dumps(struct,indent=4,sort_keys=True)
	elif options.raw:
		for workflow in list:
			reqinfo[workflow] = getWorkflowInfo(workflow,nodbs=nodbs)
			addToSummary(reqinfo[workflow])
			print "%s" % (workflow)
			for i in reqinfo[workflow].keys():
				print " %s: %s" % (i,reqinfo[workflow][i])
			print
	elif options.csv:
		keys = ['outputdataset','events','expectedevents','prepid','request','priority','type','status','requestdays','custodialt1','cpuhours','team']
		print ",".join(v for k,v in enumerate(keys))
		for w in list:
			reqinfo[w] = getWorkflowInfo(w,nodbs=nodbs)
			addToSummary(reqinfo[w])
			#sys.stderr.write ("%s\n" % (w))
			for o in reqinfo[w]['outputdataset']:
				sys.stdout.write ("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (o['name'],o['events'],reqinfo[w]['expectedevents'],reqinfo[w]['prepid'],w,reqinfo[w]['priority'],reqinfo[w]['type'],reqinfo[w]['status'],reqinfo[w]['requestdays'],reqinfo[w]['custodialsites'][0],reqinfo[w]['cpuhours'],reqinfo[w]['team']))
	elif options.tapefamilies:
		expectedbatchsize = 0
		print
		for workflow in list:
			reqinfo[workflow] = getWorkflowInfo(workflow,nodbs=nodbs)
                	prepid = reqinfo[workflow]['prepid']
			expectedbatchsize = expectedbatchsize + reqinfo[workflow]['expectedtotalsize']
               		acqera = prepid.split('-')[1]
                	if acqera not in eras:
                        	acqera = "Summer12"
                	prids = reqinfo[workflow]['primaryds']
                	for i in tftiers:
                        	tf = "/store/mc/"+acqera+"/"+prids+"/"+i
                        	#print "%s (~%sGB)" % (tf,expectedtotalsize)
				print "%s" % (tf)
		print 
		print "Total expected size for GEN-SIM: %s(GB)" % expectedbatchsize
		print 
        	#print "PREPIDs: "+" ".join(reqinfo[x]['prepid'] for x in reqinfo.keys())
	elif options.datasets:
		for workflow in list:
			conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
			r1=conn.request('GET','/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=' + workflow)
			r2=conn.getresponse()
			data = r2.read()
			ods = json.loads(data)
			conn.close()
			for o in ods:
				sys.stdout.write("%s %s\n" % (workflow,o))
				sys.stdout.flush()
	elif options.jobs:
		print
		print "%-70s %6s %6s %6s %6s %6s %6s %6s %6s %6s %-11s %-6s %-10s" % ('REQUEST','Q','C','P','R','S','F','I','T','status','team','custT1','prio')
		print "---------------------------------------------------------------------------------------------------------"
		for w in list:
			reqinfo[w] = getWorkflowInfo(w,nodbs=nodbs)
			addToSummary(reqinfo[w])
			r = reqinfo[w]['js']
			print "%-70s %6s %6s %6s %6s %6s %6s %6s %6s %6s %-11s %-6s %-10s" % (w,r['queued'],r['cooloff'],r['pending'],r['running'],r['success'],r['failure'],r['inWMBS'],r['total_jobs'],reqinfo[w]['status'][0:6],reqinfo[w]['team'],reqinfo[w]['zone'],reqinfo[w]['priority'])
		print
	elif options.events:
		for w in list:
			reqinfo[w] = getWorkflowInfo(w,nodbs=nodbs)
			addToSummary(reqinfo[w])
			r = reqinfo[w]['js']
			for o in reqinfo[w]['outputdataset']:
				print "%s %s %s %s (%s%%)" % (w,reqinfo[w]['prepid'],o['name'],o['events'],int(100*o['events']/reqinfo[w]['expectedevents']))
	else:
		print
		for w in list:
			reqinfo[w] = getWorkflowInfo(w,nodbs=nodbs)
			addToSummary(reqinfo[w])
			print "%s (%s,%s,%s at %s)" % (w,reqinfo[w]['prepid'],reqinfo[w]['type'],reqinfo[w]['status'],reqinfo[w]['zone'])
			r = reqinfo[w]['js']
			
			print " Days: %s ExpectedJobs: %s CPUHours: %s Jobs: Q:%s C:%s P:%s R:%s S:%s F:%s T:%s\n Priority: %s Team: %s Timeev: %s Sizeev: %s Hours/job: %s ExpectedEvts/job: %s\n FilterEff: %s %s Lumis/Job: %s GlobalTag: %s" % (reqinfo[w]['requestdays'],reqinfo[w]['expectedjobs'],reqinfo[w]['cpuhours'],r['queued'],r['cooloff'],r['pending'],r['running'],r['success'],r['failure'],r['total_jobs'],reqinfo[w]['priority'],reqinfo[w]['team'],reqinfo[w]['timeev'],reqinfo[w]['sizeev'],reqinfo[w]['expectedjobcpuhours'],reqinfo[w]['events_per_job']*reqinfo[w]['filtereff'],reqinfo[w]['filtereff'],reqinfo[w]['cmssw'],reqinfo[w]['lumis_per_job'],reqinfo[w]['globaltag'])
			for o in reqinfo[w]['outputdataset']:
				try:
					oo = 100*o['events']/reqinfo[w]['expectedevents']
				except:
					oo = 0
				print " %s %s (reached %s%%, expect %s, status '%s')" % (o['name'],o['events'],oo,reqinfo[w]['expectedevents'],o['status'])
				if o['phtrinfo'] != {}:
					print "  subscribed to %s (%s,%s%%)" % (o['phtrinfo']['node'],o['phtrinfo']['type'],o['phtrinfo']['perc'])
				if 'phreqinfo' in o.keys() and o['phreqinfo'] != {}:
					print "  request %s: https://cmsweb.cern.ch/phedex/prod/Request::View?request=%s" % (o['phreqinfo']['approval'],o['phreqinfo']['id'])
			print

	if sum and options.mreport:
		print "Summary: \n---------------------------------"
		total_jobs = sum['success']+sum['failure']
		if total_jobs > 0:
			percsucc = 100*round(float(sum['success'])/total_jobs,2)
			percfail = 100*round(float(sum['failure'])/total_jobs,2)
		else:
			percsucc = 0
			percfail = 0
		print "Queued: %s Running: %s TOTAL: %s Successful: %s (%s%%) Failed: %s (%s%%)" % (sum['queued'],sum['running'],total_jobs,sum['success'],percsucc,sum['failure'],percfail)

		expectedevents = sum['expectedevents']
		cpuhours = sum['cpuhours']
		remainingcpuhours = sum['remainingcpuhours']
		print "| Number of requests | %s |" % len(list)
		print "| Total requested events | %sM |" % round(expectedevents/1000000,1)
		print "| Total CPUHours | %s |" % cpuhours
		print "| Total remaining CPUHours | %s |" % remainingcpuhours
		print "| PRODUCTION | %sM (%s%%)|" % (round(sum['events']['PRODUCTION']/1000000,2),round(100*(float(sum['events']['PRODUCTION'])/expectedevents),2))
		print "| VALID | %sM (%s%%)|" % (round(sum['events']['VALID']/1000000,2),round(100*(float(sum['events']['VALID'])/expectedevents),2))
		print "| PRODUCTION+VALID | %sM (%s%%)|" % (round(sum['events']['PRODUCTION']/1000000,2)+round(sum['events']['VALID']/1000000,2),round(100*(float(sum['events']['PRODUCTION'])/expectedevents),2)+round(100*(float(sum['events']['VALID'])/expectedevents),2))

		for y in ['events','team','priority','zone','status']:
			print "%-17s " % y,
			for x in sum[y].keys():
				if not x:
					xx = '<none>'
				else:
					xx = x
				print "%s(%s)" % (xx,sum[y][x]),
			print
		print
        sys.exit(0)

if __name__ == "__main__":
        main()

