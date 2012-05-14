#!/usr/bin/env python2.6 -w
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

dashost = 'https://cmsweb.cern.ch'
reqmgrsocket='vocms204.cern.ch'
overview = ''
typelist = ['MonteCarlo','MonteCarloFromGEN','ReReco','ReDigi']
statuslist = ['assignment-approved','acquired','running','completed','closed-out','announced']
cachedoverview = '/tmp/' + os.environ['USER'] + '/overview.cache'
forceoverview = 0
sum = {}
sum['events'] = {'PRODUCTION':0,'VALID':0,'INVALID':0}
nodbs = 0

def addToSummary(r):
	global sum
	for i in ['queued','cooloff','pending','running','success','failure','inWMBS','total_jobs']:
		if i in sum.keys():
			sum[i] = sum[i] + r['js'][i]
		else:
			sum[i] = r['js'][i]
	for i in ['expectedevents','cpuhours']:
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
		elif 'processingVersion' in raw:
			processingVersion = raw[raw.find("'")+1:]
			processingVersion = processingVersion[0:processingVersion.find("'")]
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
		[inputdataset['events'],inputdataset['status']] = getdsdetail(inputdataset['name'])
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

	cpuhours = timeev*expectedevents/3600
	
	return {'filtereff':filtereff,'type':type,'status':status,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'priority':priority,'sites':sites,'custodialt1':custodialt1,'zone':getzonebyt1(custodialt1),'js':j,'outputdataset':outputdataset,'cpuhours':cpuhours,'team':team,'acquisitionEra':acquisitionEra,'reqdate':reqdate,'requestdays':requestdays,'processingVersion':processingVersion}

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
        if (os.path.exists(cachedoverview)) and ( (time.time()-os.path.getmtime(cachedoverview)>cacheoverviewage*60) or forceoverview):
                os.remove(cachedoverview)
        if (not os.path.exists(cachedoverview)):
		print "Reloading cache overview"
                s = getnewoverview()
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
	#print "Getting overview..",
	while c < 10:
		try:
			#conn  =  httplib.HTTPSConnection(reqmgrsocket, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'),timeout=10)
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
	global nodbs
	if 0:
		return [0,'']
	[e,st] = dbs_get_data(dataset)
	if e == -1:
		return [0,'']
	else:
		return [e,st]

def dbs_get_data(dataset):
	output=os.popen("/afs/cern.ch/user/s/spinoso/public/dbssql --input='find sum(block.numevents),dataset.status where dataset="+dataset+"'"+ "|grep '[0-9]\{1,\}'").read()
	ret = output.split(' ')
	try:
		e = int(ret[0])
	except:
		e = 0
	try:
		st = ret[1].rstrip()
	except:
		st = ''
	return [e,st]

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
	global overview,forceoverview,sum
	
	viewchoices = ['names','all','production','dataset','run','assignment']
	parser = optparse.OptionParser()
	parser.add_option('-l', '--listfile', help='analyze workflows listed in textfile',dest='list')
	parser.add_option('-f', '--force-overview-update', help='force overview update',dest='forceoverview',action="store_true")
	parser.add_option('-m', '--summary', help='print a brief summary at the end of the report',dest='summary',action="store_true")
	parser.add_option('-w', '--workflow', help='analyze specific workflow',dest='wf')
	parser.add_option('-p', '--prepid', help='analyze workflow with PREPID',dest='prepid')
	parser.add_option('-s', '--status', help='analyze workflow in status STATUS',dest='status')
	parser.add_option('-t', '--type', help='analyze workflow of type TYPE',dest='type')
	parser.add_option('-n', '--names', help='print just request names',dest='names',action="store_true")
	parser.add_option('-a','--all', help='print all information about the requests',dest='raw',action="store_true")
	parser.add_option('-g', '--assignment', help='print just information useful in assignment context',dest='assignment',action="store_true")
	parser.add_option('-d', '--datasets', help='print just output datasets',dest='datasets',action="store_true")
	parser.add_option('-b', '--no-dbs', help='don\'t contact DBS (faster)',dest='nodbs',action="store_true")
	parser.add_option('-j', '--jobs', help='print just information useful in workflow management context',dest='jobs',action="store_true")
	parser.add_option('-e', '--events', help='print just information useful in user reporting',dest='events',action="store_true")

	(options,args) = parser.parse_args()

	overview = getoverview()
	print
	if options.forceoverview:
		forceoverview = 1
	else:
		forceoverview = 0
	if options.wf:
		list = [options.wf]
	elif options.list:
		list = open(options.list).read().splitlines()
	elif options.prepid:
		list = getRequestsByPREPID(options.prepid)
	elif options.status or options.type:
		rtype = typelist
		if options.type:
			rtype = options.type.split(',')
		rstatus = statuslist
		if options.status:
			rstatus = options.status.split(',')
		#print "Selecting requests with type %s and status %s" % (rtype,rstatus)
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
	elif options.raw:
		for workflow in list:
			reqinfo[workflow] = getWorkflowInfo(workflow)
			addToSummary(reqinfo[workflow])
			print "%s" % (workflow)
			for i in reqinfo[workflow].keys():
				print " %s: %s" % (i,reqinfo[workflow][i])
			print
	elif options.assignment:
		for w in list:
			reqinfo[w] = getWorkflowInfo(w)
			addToSummary(reqinfo[w])
			sites = reqinfo[w]['sites']
			sites.sort()
			print "%s PREPID:%s type:%s status:%s priority:%s expectedevents:%s cpuhours:%s custodialt1: %s team: %s" % (w,reqinfo[w]['prepid'],reqinfo[w]['type'],reqinfo[w]['status'],reqinfo[w]['priority'],reqinfo[w]['expectedevents'],reqinfo[w]['cpuhours'],reqinfo[w]['custodialt1'],reqinfo[w]['team'])
			#print " team: %s custodialT1: %s zone: %s" % (",".join(x for x in reqinfo[w]['team']),reqinfo[w]['custodialt1'],reqinfo[w]['zone'])
	elif options.datasets:
		for workflow in list:
			conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
			r1=conn.request('GET','/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=' + workflow)
			r2=conn.getresponse()
			data = r2.read()
			ods = json.loads(data)
			conn.close()
			for o in ods:
				print "%s" % o
			print
	elif options.jobs:
		print
		print "%-70s %6s %6s %6s %6s %6s %6s %6s %6s %6s %-11s %-6s %-10s" % ('REQUEST','Q','C','P','R','S','F','I','T','status','team','custT1','prio')
		print "---------------------------------------------------------------------------------------------------------"
		for w in list:
			reqinfo[w] = getWorkflowInfo(w)
			addToSummary(reqinfo[w])
			r = reqinfo[w]['js']
			print "%-70s %6s %6s %6s %6s %6s %6s %6s %6s %6s %-11s %-6s %-10s" % (w,r['queued'],r['cooloff'],r['pending'],r['running'],r['success'],r['failure'],r['inWMBS'],r['total_jobs'],reqinfo[w]['status'][0:6],reqinfo[w]['team'],reqinfo[w]['zone'],reqinfo[w]['priority'])
		print
	elif options.events:
		for w in list:
			reqinfo[w] = getWorkflowInfo(w)
			addToSummary(reqinfo[w])
			r = reqinfo[w]['js']
			for o in reqinfo[w]['outputdataset']:
				print "%s %s %s %s %s (%s%%), requested %s, priority %s, running %s" % (w,reqinfo[w]['prepid'],reqinfo[w]['status'],o['name'],o['events'],int(100*o['events']/reqinfo[w]['expectedevents']),reqinfo[w]['expectedevents'],reqinfo[w]['priority'],r['running'])
	else:
		for w in list:
			reqinfo[w] = getWorkflowInfo(w)
			addToSummary(reqinfo[w])
			print "%s (%s,%s,%s at %s)" % (w,reqinfo[w]['prepid'],reqinfo[w]['type'],reqinfo[w]['status'],reqinfo[w]['custodialt1'])
			r = reqinfo[w]['js']
			print " Priority: %s Team: %s Jobs: Q:%s C:%s P:%s R:%s S:%s F:%s T:%s" % (reqinfo[w]['priority'],reqinfo[w]['team'],r['queued'],r['cooloff'],r['pending'],r['running'],r['success'],r['failure'],r['total_jobs'])
			for o in reqinfo[w]['outputdataset']:
				print " %s %s (reached %s%%, expect %s, status '%s', priority %s)" % (o['name'],o['events'],int(100*o['events']/reqinfo[w]['expectedevents']),reqinfo[w]['expectedevents'],o['status'],reqinfo[w]['priority'])
				if o['phtrinfo'] != {}:
					print "  subscribed to %s (%s,%s%%)" % (o['phtrinfo']['node'],o['phtrinfo']['type'],o['phtrinfo']['perc'])
				if o['phreqinfo'] != {}:
					print "  request %s: https://cmsweb.cern.ch/phedex/prod/Request::View?request=%s" % (o['phreqinfo']['approval'],o['phreqinfo']['id'])
			print

	if sum and options.summary:
		print "Summary: \n---------------------------------"
		percsucc = round(float(sum['success'])/sum['total_jobs'],2)
		percfail = round(float(sum['failure'])/sum['total_jobs'],2)
		print "Queued: %s Running: %s TOTAL: %s Successful: %s (%s%%) Failed: %s (%s%%)" % (sum['queued'],sum['running'],sum['total_jobs'],sum['success'],percsucc,sum['failure'],percfail)

		expectedevents = sum['expectedevents']
		print "| Total requested | %sM |" % round(expectedevents/1000000,1)
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
	print
        sys.exit(0)

if __name__ == "__main__":
        main()

