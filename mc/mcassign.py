#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os
import optparse
import time
import datetime
try:
    import json
except ImportError:
    import simplejson as json

# TODO suggest TeamList 
# TODO automatic acqera
# TODO guess procversion

legal_eras = ['Summer11','Summer12']
teams_hp = ['production']
teams_lp = ['integration','dataops','dataops']
zones = ['FNAL','CNAF','ASGC','IN2P3','RAL','PIC','KIT']
zone2t1 = {'FNAL':'T1_US_FNAL','CNAF':'T1_IT_CNAF','ASGC':'T1_TW_ASGC','IN2P3':'T1_FR_CCIN2P3','RAL':'T1_UK_RAL','PIC':'T1_ES_PIC','KIT':'T1_DE_KIT'}
siteblacklist = ['T2_FR_GRIF_IRFU','T2_KR_KNU','T2_PK_NCP','T2_PT_LIP_Lisbon','T2_RU_RRC_KI','T2_UK_SGrid_Bristol','T2_US_Vanderbilt','T2_CH_CERN']
cachedoverview = '/afs/cern.ch/user/s/spinoso/public/overview.cache'
forceoverview = 0

def get_linkedt2s(custodialT1):
	list = []
	try:
		url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/links?status=ok&to=%s_Buffer&from=T2_*" % custodialT1
		response = urllib2.urlopen(url)
		j = json.load(response)["phedex"]
		for dict in j['link']:
			list.append(dict['from'])
		list.sort()
		return list
	except	Exception:
        	print 'Status:',response.status,'Reason:',response.reason
        	print sys.exc_info()
		sys.exit(1)

def getsitelist(zone):
	global zones,siteblacklist,zone2t1
	if zone in zones:
		sitelist = []
		sitelist.append(zone2t1[zone])
		#print "Zone: %s" % zone
		#print "Custodial T1 is %s" % zone2t1[zone]
		t2list = get_linkedt2s(zone2t1[zone])
		for i in t2list:
			if not i in siteblacklist:
				sitelist.append(i)
	else:
		sitelist = zone.split(',')
		t1count = 0
		for i in sitelist:
			if 'T1_' in i:
				custodialT1 = i
				t1count = t1count + 1
		if t1count > 1:
			print "WARNING: More than 1 T1 has been specified in %s" % (sitelist)
		t2list = get_linkedt2s(custodialT1)
		if t1count == 1:
			for i in sitelist:
				if 'T2_' in i:
					if not i in t2list:
						print "%s has no PhEDEx uplink to %s" % (i,custodialT1)
						sys.exit(1)
	return sitelist

def getcampaign(r):
        prepid = r['prepid']
        return prepid.split('-')[1]

def getacqera(r):
	global legal_eras
        prepid = r['prepid']
        era = prepid.split('-')[1]
	if era in legal_eras:
		return era
	else:
		print "*********************************************************"
		print "WARNING: '%s' is not a known era, using Summer12"  % era
		print "*********************************************************"
		return 'Summer12'

def assignMCRequest(url,workflow,team,sitelist,era,procversion):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": sitelist,
              "SiteBlacklist": [],
              "MergedLFNBase": "/store/mc",
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 50000,
	      "maxRSS": 2294967,
              "maxVSize": 4394967000,
              "AcquisitionEra": era,
	      "dashboard": "production",
              "ProcessingVersion": procversion,
              "checkbox"+workflow: "checked"}

    encodedParams = urllib.urlencode(params, True)

    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("POST",  "/reqmgr/assign/handleAssignmentPage", encodedParams, headers)
    response = conn.getresponse()
    if response.status != 200:
        print 'could not assign request with following parameters:'
        for item in params.keys():
            print item + ": " + str(params[item])
        print 'Response from http call:'
        print 'Status:',response.status,'Reason:',response.reason
        print 'Explanation:'
        data = response.read()
        print data
        print "Exiting!"
  	sys.exit(1)
    conn.close()
    return

def getzonebyt1(s):
	custodial = '?'
	if not s:
		return custodial
	t1list = {'T1_CH_CERN':'CERN','T1_FR_CCIN2P3':'IN2P3','T1_TW_ASGC':'ASGC','T1_IT_CNAF':'CNAF','T1_US_FNAL':'FNAL','T1_DE_KIT':'KIT','T1_ES_PIC':'PIC','T1_UK_RAL':'RAL'}
	for i in t1list.keys():
		if i in s:
			custodial = t1list[i]
	return custodial

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


def getWorkflowInfo(workflow,nodbs=0):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/view/showWorkload?requestName=%s' % workflow)
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
	events_per_job = None
	lumis_per_job = None
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
	r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=%s' % workflow)
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
		typ = s['RequestType']
	except:
		typ = ''
	try:
		status = s['RequestStatus']
	except:
		status = ''
	#try:
        #        reqevts = s['RequestSizeEvents']
        #except:
        #        try:
        #                reqevts = s['RequestNumEvents']
        #        except:
        #                print "No RequestNumEvents for this workflow: %s" % workflow
        #                return ''
	reqevts = 0
	inputdataset = {}
	try:
		inputdataset['name'] = s['InputDatasets'][0]
	except:
		pass
	
	if typ in ['MonteCarlo']:
		expectedevents = int(reqevts)
		expectedjobs = int(expectedevents/(events_per_job*filtereff))
		expectedjobcpuhours = int(timeev*(events_per_job*filtereff)/3600)
	elif typ in ['MonteCarloFromGEN']:
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
			expectedjobcpuhours = int(timeev*inputdataset['events']/inputdataset['lumicount']/3600)
		except:
			expectedjobcpuhours = 0
	else:
		expectedevents = -1
		expectedjobs = -1
		expectedjobcpuhours = -1
	
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
	#for o in ods:
	#	if nodbs:
	#		[oe,ost] = [0,'']
	#	else:
	#		[oe,ost] = getdsdetail(o)
	remainingcpuhours = timeev*(expectedevents-eventsdone)/3600
	return {'type':typ,'status':status,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'priority':priority,'sites':sites,'custodialt1':custodialt1,'zone':getzonebyt1(custodialt1),'js':j,'outputdataset':outputdataset,'cpuhours':cpuhours,'remainingcpuhours':remainingcpuhours,'team':team,'acquisitionEra':acquisitionEra,'requestdays':requestdays,'processingVersion':processingVersion,'events_per_job':events_per_job,'lumis_per_job':lumis_per_job,'expectedjobs':expectedjobs,'expectedjobcpuhours':expectedjobcpuhours,'cmssw':cmssw}

def isDatasetNameUsed(datasetname):
	[e,st] = getdsdetail(datasetname)
	if e > 0:
		return 1
	else:
		return 0

def getRequestsByPREPID(prepid):
	r = []
	for i in overview:
		if prepid in i['request_name']:
			r.append(i['request_name'])
	return r
	
def getZoneFromRequest(w):
	print "Guessing zone"
	reqinfo = getWorkflowInfo(w,nodbs=1)
	prepid = reqinfo['prepid']
	zone = []
	reqs = getRequestsByPREPID(prepid)
	print "Found %s total requests with the same PREP ID: %s" % (len(reqs),prepid)
	for r in reqs:
		info = getWorkflowInfo(r)
		print "%s (%s) %s" % (r,info['status'],info['zone'])
		if info['zone'] not in zone:
			zone.append(info['zone'])
	if '?' in zone:
		zone.remove('?')
	if len(zone) > 1:
		print "More than one zone is associated to %s" % zone
		sys.exit(2)
	if len(zone) < 1:
		print "No zones can be guessed."
		sys.exit(2)
	print "Zone is %s" % zone[0]
	return zone[0]

def getnextprocessingversion(w):
	print "Guessing processing version"
	rqinfo = getWorkflowInfo(w,nodbs=0)
	prepid = rqinfo['prepid']
	reqs = getRequestsByPREPID(prepid)
	olddatasets = []
	for r in reqs:
		reqinfo = getWorkflowInfo(r)
		if reqinfo['globaltag'] in reqinfo['processingVersion']:
			for o in reqinfo['outputdataset']:
				if not o['name'] in olddatasets:
					print "Found %s" % o['name']
					olddatasets.append(o['name'])
	olddatasets.sort()
	
	y = 1
	c = 1
	while y > 0:
		y = 0
		for o in olddatasets:
			v = '-v%s' % c
			if v in o:
				y = 1
				break
		if y:
			c = c + 1
	e = 1
	acqera = getacqera(rqinfo)
	while e > 0:
		nextoutputdataset = '/%s/%s-%s-v%s/GEN-SIM' % (rqinfo['primaryds'],acqera,rqinfo['globaltag'],c)
		[e,st] = getdsdetail(nextoutputdataset)
	print "%s has no events in DBS" % nextoutputdataset
	print "Version is v%s" % (c)
				
	return 'v%s' % (c)

def main():
	global overview,forceoverview,sum,nodbs
	global legal_eras,zones

	overview = getoverview()
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-w', '--workflow', help='workflow name',dest='workflow')
	parser.add_option('-l', '--workflowlist', help='workflow list in textfile',dest='list')
	parser.add_option('-t', '--team', help='team: one of %s' % (teams_hp+teams_lp),dest='team')
	parser.add_option('--test', action="store_true",default=True,help='test mode: don\'treally assign at the end',dest='test')
	parser.add_option('--assign', action="store_false",default=True,help='assign mode',dest='test')
	parser.add_option('-z', '--zone', help='Zone %s or single site or comma-separated list (i.e. T1_US_FNAL,T2_FR_CCIN2P3,T2_DE_DESY)' % zones,dest='zone')
	parser.add_option('-a', '--acqera', help='Acquisition era: one of %s' % legal_eras,dest='acqera')
	#parser.add_option('-p', '--procversion', help='Processing Version',dest='procversion')
	parser.add_option('-v', '--version', help='Version (it is the vx part of the ProcessingVersion)',dest='version')
	parser.add_option('-p', '--processingversion', help='optionally provide processing version; if not, it will default to GlobalTag-vX',dest='optprocessingversion')
	(options,args) = parser.parse_args()

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
		print "Please provide at least one workflow to assign!"
		sys.exit(1)

	if options.team:
		teams=options.team.split(',')
		if 'auto' in teams:
			teams = []
	else:
		teams = []
	if options.zone:
		zone = options.zone
		sitelist = getsitelist(zone)
	else:
		zone = 'auto'
		#sys.exit(1)
	#print "Zone: %s" % zone

	#if options.acqera:
	#	acqera = options.acqera
	#else:
	#	print "Acquisition Era not provided, please provide one among %s" % legal_eras
	#	sys.exit(1)
	if options.optprocessingversion:
		optprocessingversion = options.optprocessingversion
	else:
		optprocessingversion = ''
	if options.version:
		version = options.version
	else:
		version = 'auto'
		#print "Please provide a version!"
		#sys.exit(1)
		
	reqinfo = {}
	
	for w in list:
		reqinfo[w] = getWorkflowInfo(w)
		if reqinfo[w]['status'] == '':
			print "Cannot get information for %s!" % w
			sys.exit(1)
		if not 'MonteCarlo' in reqinfo[w]['type']:
			print "%s: not a MonteCarlo/MonteCarloFromGEN request!" % w
			sys.exit(1)
		print "* CHECKING %s" % w

		#for i in reqinfo[w].keys():
		#	print " %s: %s" % (i,reqinfo[w][i])
		if reqinfo[w]['status'] != 'assignment-approved':
			print "%s: not in status assignment-approved! (status is '%s')" % (w,reqinfo[w]['status'])
			sys.exit(1)

	tcount_hp = 0
	tcount_lp = 0
	tcount = 0
	for w in list:
		priority = reqinfo[w]['priority']
		if teams == []:
			if priority >=100000:
				team = teams_hp[tcount_hp % len(teams_hp)]
				tcount_hp = tcount_hp + 1
			else:
				team = teams_lp[tcount_lp % len(teams_lp)]
				tcount_lp = tcount_lp + 1
		else:
			team = teams[tcount % len(teams)]
			tcount = tcount + 1

		# T3_US_Omaha hook
		newsitelist = sitelist[:]
		if 'T2_US_Nebraska' in sitelist and reqinfo[w]['type'] == 'MonteCarlo':
			newsitelist.append('T3_US_Omaha')

		# T3_US_Colorado hook
		if 'T2_US_Nebraska' in sitelist:
			newsitelist.append('T3_US_Colorado')
		
		# relval WMA (CERN) hook
		#if team == 'relval':
		#	custodialT1 = ''
		#	for i in sitelist:
		#		if 'T1_' in i:
		#			custodialT1 = i
		#	if custodialT1 == '':
		#		newsitelist = ['T1_CH_CERN']
		#	else:
		#		newsitelist = ['T1_CH_CERN',custodialT1]
		#	print "Using relval instance with sitelist = %s" % newsitelist

		campaign = getcampaign(reqinfo[w])
		acqera = getacqera(reqinfo[w])
		if zone == 'auto':
			zone = getZoneFromRequest(w)
			sitelist = getsitelist(zone)
		if optprocessingversion == '':
			if version == 'auto':
				version = getnextprocessingversion(w)
			procversion = "%s-%s" % (reqinfo[w]['globaltag'],version)
		else:
			procversion = optprocessingversion

		suminfo = "%s\n\tteam: %s\tpriority: %s\n\tacqera: %s\tProcessingVersion: %s\n\tZone: %s\tWhitelist: %s" % (w,team,priority,acqera,procversion,zone,newsitelist)
		if options.test:
			print "TEST:\t%s" % suminfo
		else:
			print "ASSIGN:\t%s" % suminfo
			assignMCRequest(url,w,team,newsitelist,acqera,procversion)
		print
	
	if options.test:
		ts = "TESTED"
	else:
		ts = "ASSIGNED"
	print "The following requests have been %s:\n" % ts
	for w in list:
		print "%s" % w
	print "\n"

	sys.exit(0)

if __name__ == "__main__":
	main()
