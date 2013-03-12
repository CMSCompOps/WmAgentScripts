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
teams_lp = ['mc']
teams_hp = ['mc_highprio']

zones = ['FNAL','CNAF','ASGC','IN2P3','RAL','PIC','KIT']

siteblacklist = ['T2_FR_GRIF_IRFU','T2_PK_NCP','T2_PT_LIP_Lisbon','T2_RU_RRC_KI','T2_UK_SGrid_Bristol']
siteblacklist.extend(['T2_PL_Warsaw','T2_RU_PNPI','T2_KR_KNU','T2_UA_KIPT','T2_AT_Vienna'])

sitelistsmallrequests = ['T2_DE_DESY','T2_IT_Pisa','T2_ES_CIEMAT','T2_IT_Bari','T2_US_Purdue','T2_US_Caltech','T2_DE_RWTH','T2_IT_Legnaro','T2_IT_Rome','T2_US_Florida','T2_US_MIT','T2_US_Wisconsin','T2_US_UCSD','T2_US_Nebraska','T2_RU_IHEP','T2_US_Vanderbilt']
sitelisthirequests = ['T2_US_MIT','T2_US_Wisconsin','T2_US_Nebraska','T2_US_Vanderbilt','T2_FR_CCIN2P3']
sitelisthimemrequests = ['T2_US_MIT','T2_US_Wisconsin','T2_US_Nebraska','T2_US_Vanderbilt','T2_FR_CCIN2P3']

siteliststep0long = ['T2_US_Purdue','T2_US_Nebraska','T3_US_Omaha']
sitelistgensubscrnoncust = ['T1_DE_KIT_MSS' ,'T1_ES_PIC_MSS' ,'T1_FR_CCIN2P3_MSS' ,'T1_IT_CNAF_MSS' ,'T1_TW_ASGC_MSS' ,'T1_UK_RAL_MSS' ,'T2_BE_IIHE' ,'T2_BE_UCL' ,'T2_BR_SPRACE' ,'T2_CH_CSCS' ,'T2_CN_Beijing' ,'T2_DE_DESY' ,'T2_DE_RWTH' ,'T2_EE_Estonia' ,'T2_ES_CIEMAT' ,'T2_ES_IFCA' ,'T2_FI_HIP' ,'T2_FR_CCIN2P3' ,'T2_FR_GRIF_LLR' ,'T2_FR_IPHC' ,'T2_HU_Budapest' ,'T2_IN_TIFR' ,'T2_IT_Bari' ,'T2_IT_Legnaro' ,'T2_IT_Pisa' ,'T2_IT_Rome' ,'T2_PL_Warsaw' ,'T2_PT_NCG_Lisbon','T2_RU_JINR' ,'T2_RU_SINP' ,'T2_TR_METU' ,'T2_TW_Taiwan' ,'T2_UK_London_Brunel' ,'T2_UK_London_IC' ,'T2_UK_SGrid_RALPP' ,'T2_US_Caltech' ,'T2_US_Florida' ,'T2_US_MIT' ,'T2_US_Nebraska' ,'T2_US_Purdue' ,'T2_US_UCSD' ,'T2_US_Wisconsin','T2_BR_UERJ','T3_US_Colorado','T2_RU_IHEP','T2_RU_ITEP','T2_CH_CERN']

cachedoverview = '/afs/cern.ch/user/s/spinoso/public/overview.cache'
forceoverview = 0
tcount_hp = 0
tcount_lp = 0
tcount = 0

def get_linkedt2s(custodialT1):
	list = []
	if custodialT1 == '':
		return []
	try:
		# get list of linked T2s
		url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/links?status=ok&to=%s_Buffer&from=T2_*" % custodialT1
		response = urllib2.urlopen(url)
		j = json.load(response)["phedex"]
		for dict in j['link']:
			list.append(dict['from'])

		# add list of commissioned T3s that are linked
		url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/links?status=ok&to=%s_Buffer&from=T3_*" % custodialT1
		response = urllib2.urlopen(url)
		j = json.load(response)["phedex"]
		for dict in j['link']:
			if dict['from'] in ['T3_US_Omaha','T3_US_Colorado']:
				list.append(dict['from'])

		list.sort()
		return list

	except	Exception:
        	print 'Status:',response.status,'Reason:',response.reason
        	print sys.exc_info()
		sys.exit(1)

def getSitelistFromZone(zone,r,hi,siteblacklist):
	global zones
	blacklist = siteblacklist[:]
	zone2t1 = {'FNAL':'T1_US_FNAL','CNAF':'T1_IT_CNAF','ASGC':'T1_TW_ASGC','IN2P3':'T1_FR_CCIN2P3','RAL':'T1_UK_RAL','PIC':'T1_ES_PIC','KIT':'T1_DE_KIT'}
	# if zone is automatic, guess zone
	if zone == 'auto':
		if hi:
			zone = 'IN2P3'
		elif r['type'] == 'LHEStepZero':
			zone = 'FNAL'
		else:
			zone = getZoneFromRequest(r['prepid'])

	# convert zone to list of t1 + linked t2
	if zone in zones:
		sitelist = []
		
		# run MC/MCFromGEN @ T1
		if r['type'] in ['MonteCarlo','MonteCarloFromGEN']:
			sitelist.append(zone2t1[zone])

		# get all linked T2s
		t2list = get_linkedt2s(zone2t1[zone])
		
		# add T3_US_Omaha if T2_US_Nebraska is usable, merged files will always get to Nebraska
		if 'T2_US_Nebraska' in t2list:
			t2list.append('T3_US_Omaha')

		# exclude Vanderbilt if not Heavy Ion
		if not hi:
			if not 'T2_US_Vanderbilt' in blacklist:
				blacklist.extend(['T2_US_Vanderbilt'])

		# exclude Omaha from MCFromGEN
		if r['type'] == 'MonteCarloFromGEN':
			if not 'T3_US_Omaha' in blacklist:
				blacklist.extend(['T3_US_Omaha'])

		# no blacklist for MonteCarloFromGEN: we will subscribe only to sites supposed to work		

		for i in t2list:
			if not i in blacklist:
				sitelist.append(i)
	else:
		# explicit sitelist, no guesses
		sitelist = zone.split(',')
		t1count = 0
		custodialT1 = ''
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

def getacqera(prepid):
	global legal_eras
        era = prepid.split('-')[1]
	for e in legal_eras:
		if e in prepid:
			return e
	print " Cannot guess acquisition era for %s" % prepid
	sys.exit(1)
	

def assignMCRequest(url,workflow,team,sitelist,era,procversion,mergedlfnbase,minmergesize,maxRSS,custodialsites,softtimeout):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": sitelist,
              "SiteBlacklist": [],
              "MergedLFNBase": mergedlfnbase,
              "UnmergedLFNBase": "/store/unmerged",
	      "SoftTimeout": softtimeout,
              "MinMergeSize": minmergesize,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 50000,
	      #"maxRSS": 2294967,
	      "maxRSS": maxRSS,
              "maxVSize": 4394967000,
              "AcquisitionEra": era,
	      "dashboard": "production",
              "ProcessingVersion": procversion,
		"CustodialSites":custodialsites,
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
	t1list = {'T1_FR_CCIN2P3':'IN2P3','T1_TW_ASGC':'ASGC','T1_IT_CNAF':'CNAF','T1_US_FNAL':'FNAL','T1_DE_KIT':'KIT','T1_ES_PIC':'PIC','T1_UK_RAL':'RAL'}
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
	outputtier = None
	reqevts = 0
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
		elif 'output.dataTier' in raw:
			outputtier = raw[raw.find("'")+1:]
			outputtier = outputtier[0:outputtier.find("'")]
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
		elif 'splitting.events_per_job' in raw:
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
	
	if typ in ['MonteCarlo','LHEStepZero']:
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
		
		if 0:
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
	return {'type':typ,'status':status,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'priority':priority,'sites':sites,'custodialt1':custodialt1,'zone':getzonebyt1(custodialt1),'js':j,'outputdataset':outputdataset,'cpuhours':cpuhours,'remainingcpuhours':remainingcpuhours,'team':team,'acquisitionEra':acquisitionEra,'requestdays':requestdays,'processingVersion':processingVersion,'events_per_job':events_per_job,'lumis_per_job':lumis_per_job,'expectedjobs':expectedjobs,'expectedjobcpuhours':expectedjobcpuhours,'cmssw':cmssw,'outputtier':outputtier}

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
	
def getZoneFromRequest(prepid):
	print "Guessing zone"
	zone = []
	reqs = getRequestsByPREPID(prepid)
	#print "Found %s total requests with the same PREP ID: %s" % (len(reqs),prepid)
	for r in reqs:
		info = getWorkflowInfo(r,nodbs=1)
		#print "%s (%s) %s" % (r,info['status'],info['zone'])
		if info['zone'] not in zone:
			zone.append(info['zone'])
	if '?' in zone:
		zone.remove('?')
	if len(zone) > 1:
		print "Requests have multiple custodial T1 candidates: %s" % zone
		for i in reqs:
			print " %s" % i
		sys.exit(2)
	if len(zone) < 1:
		print "No zones can be guessed."
		sys.exit(2)
	#print "Zone is %s" % zone[0]
	return zone[0]

def isInDBS(dataset):
	q = "/afs/cern.ch/user/s/spinoso/public/dbssql --input='find dataset where dataset="+dataset+"*' "
	#print q
	output=os.popen("%s|grep '/'" % (q)).read()
	#print output
	ret = output.split(' ')
	#print ret
	try:
		ds = ret[1].rstrip()
		#print ds
		if ds == '':
			return False
		else:
			return True
	except:
		return False

def getteam(teams,r):
	global tcount_hp,tcount_lp,tcount,teams_lp,teams_hp
	if teams == 'auto':
		if r['priority'] >=100000:
			team = teams_hp[tcount_hp % len(teams_hp)]
			tcount_hp = tcount_hp + 1
		else:
			team = teams_lp[tcount_lp % len(teams_lp)]
			tcount_lp = tcount_lp + 1
	else:
		team = teams[tcount % len(teams)]
		tcount = tcount + 1
	return team

def main():
	global overview,forceoverview,sum,nodbs,siteblacklist
	global legal_eras,zones

	overview = getoverview()
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-w', '--workflow', help='workflow name',dest='workflow')
	parser.add_option('--debug', help='add debug info',dest='debug',default=False,action="store_true")
	parser.add_option('-l', '--workflowlist', help='workflow list in textfile',dest='list')
	parser.add_option('-t', '--team', help='team: one of %s' % (teams_hp+teams_lp),dest='team')
	parser.add_option('--test', action="store_true",default=True,help='test mode: don\'treally assign at the end',dest='test')
	parser.add_option('--assign', action="store_false",default=True,help='assign mode',dest='test')
	parser.add_option('--small', action="store_true",default=False,help='assign requests considering them small',dest='small')
	parser.add_option('--hi', action="store_true",default=False,help='heavy ion request (add Vanderbilt to the whitelist, use /store/himc)',dest='hi')
	parser.add_option('--himem', action="store_true",default=False,help='high memory request (use sites allowing 3GB/job, increase maxRSS)',dest='himem')
	parser.add_option('--fsim', action="store_true",default=False,help='FastSim request (add _FSIM in the processing version)',dest='fsim')
	parser.add_option('-z', '--zone', help='Zone %s or single site or comma-separated list (i.e. T1_US_FNAL,T2_FR_CCIN2P3,T2_DE_DESY)' % zones,dest='zone')
	parser.add_option('-a', '--acqera', help='Acquisition era',dest='acqera')
	parser.add_option('-v', '--version', help='Version (it is the vx part of the ProcessingVersion), default is v1',dest='version')
	parser.add_option('-p', '--processingversion', help='Processing Version, default to GlobalTag-vX',dest='procversion')
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
			teams = 'auto'
	else:
		teams = 'auto'
	if options.zone:
		zone = options.zone
	else:
		zone = 'auto'
	if options.procversion:
		procversion = options.procversion
	else:
		procversion = 'auto'
	if options.version:
		version = options.version
	else:
		version = 'v1'
		
	reqinfo = {}

	if options.acqera:
		acqera = options.acqera
	else:
		print "Please provide the acquisition era."
		sys.exit(1)

	siteblacklist.sort()
	print "Default site blacklist: %s\n" % (",".join(x for x in siteblacklist))
	if options.hi:
		print "Heavy Ion flag is set, parameters will be configured accordingly\n"

	elif options.himem:
		print "High memory flag is set, parameters will be configured accordingly\n"

	print "Preparing requests:\n"
	#print "REQUEST TEAM PRIORITY ACQERA PROCVS ZONE"
	assign_data = {}
	for w in list:
		print "%s" % w
		reqinfo[w] = getWorkflowInfo(w)

		# status
		if reqinfo[w]['status'] == '':
			print "Cannot get information for %s!" % w
			sys.exit(1)
		if reqinfo[w]['status'] != 'assignment-approved':
			print "%s: not in status assignment-approved! (status is '%s')" % (w,reqinfo[w]['status'])
			sys.exit(1)
	
		# type
		if not 'MonteCarlo' in reqinfo[w]['type'] and not 'LHEStepZero' in reqinfo[w]['type']:
			print "%s: not a MonteCarlo/MonteCarloFromGEN/LHEStepZero request!" % w
			sys.exit(1)

		# priority
		priority = reqinfo[w]['priority']
		
		# internal assignment parameters
		hi = False
		himem = False
		if reqinfo[w]['type'] == 'LHEStepZero':
			#print 'LHEStepZero request: %s' % w
			team = 'step0'
			minmergesize = 1000000000
			softtimeout = 108000*2
			mergedlfnbase = '/store/generator'
			maxRSS = 2294967
		elif options.hi:
			hi = True
			team = getteam(teams,reqinfo[w])
			softtimeout = 108000
			mergedlfnbase = '/store/himc'
			minmergesize = 2147483648
			maxRSS = 3500000
		elif options.himem:
			himem = True
			team = getteam(teams,reqinfo[w])
			softtimeout = 108000
			mergedlfnbase = '/store/mc'
			minmergesize = 2147483648
			maxRSS = 3500000
		else:
			mergedlfnbase = '/store/mc'
			team = getteam(teams,reqinfo[w])
			softtimeout = 108000
			minmergesize = 2147483648
			maxRSS = 2294967

		# acqera
		#if acqera == 'auto':
		#	newacqera = getacqera(reqinfo[w]['prepid'])
		#else:
		#	newacqera = acqera
		newacqera = acqera

		# sitelist adjustment
		newsitelist = getSitelistFromZone(zone,reqinfo[w],hi,siteblacklist)
		custodialsites = []

		if options.hi:
			oldsitelist = newsitelist[:]
			newsitelist = []
			for i in oldsitelist:
				if 'T1_' in i:
					newsitelist.append(i)
				elif i in sitelisthirequests:
					newsitelist.append(i)
		elif options.himem:
			oldsitelist = newsitelist[:]
			newsitelist = []
			for i in oldsitelist:
				if 'T1_' in i:
					newsitelist.append(i)
				elif i in sitelisthimemrequests:
					newsitelist.append(i)
		elif reqinfo[w]['priority'] >= 100000 or options.small:
			oldsitelist = newsitelist[:]
			newsitelist = []
			for i in oldsitelist:
				if 'T1_' in i:
					newsitelist.append(i)
				elif i in sitelistsmallrequests:
					newsitelist.append(i)

		if reqinfo[w]['type'] == 'LHEStepZero':
			custodialsites.append('T1_US_FNAL')
		else:
			for i in newsitelist:
				if 'T1_' in i:
					custodialsites.append(i)

		# processing version
		
		if procversion == 'auto':
			if reqinfo[w]['type'] == 'MonteCarloFromGEN' and ('_FSIM-' in reqinfo[w]['inputdataset']['name'] or options.fsim):
				newprocversion = "%s_FSIM-%s" % (reqinfo[w]['globaltag'],version)
			else:
				newprocversion = "%s-%s" % (reqinfo[w]['globaltag'],version)
			dataset = '/%s/%s-%s/%s' % (reqinfo[w]['primaryds'],newacqera,newprocversion,reqinfo[w]['outputtier'])
			if isInDBS(dataset):
				print "Processing version already in use for %s : %s" % (w,dataset)
				sys.exit(1)
		else:
			newprocversion = procversion
			dataset = '/%s/%s-%s/%s' % (reqinfo[w]['primaryds'],newacqera,newprocversion,reqinfo[w]['outputtier'])
			#print dataset
			if isInDBS(dataset):
				print "Processing version already in use for %s : %s" % (w,dataset)
				sys.exit(1)

		# save the parameters for assignment of request 'w'
		assign_data[w] = {}
		assign_data[w]['team'] = team
		assign_data[w]['priority'] = priority
		assign_data[w]['events'] = reqinfo[w]['expectedevents']
		assign_data[w]['cpuhours'] = reqinfo[w]['cpuhours']
		assign_data[w]['whitelist'] = newsitelist
		assign_data[w]['acqera'] = newacqera
		assign_data[w]['procversion'] = newprocversion
		assign_data[w]['custodialsites'] = custodialsites
		assign_data[w]['dataset'] = dataset
		assign_data[w]['mergedlfnbase'] = mergedlfnbase
		assign_data[w]['maxRSS'] = maxRSS
		assign_data[w]['minmergesize'] = minmergesize
		assign_data[w]['softtimeout'] = softtimeout

	print "\n----------------------------------------------\n"


	if not options.test: 
		print "Assignment:"
		print
	for w in list:
		suminfo = "%s\nprio:%s events:%s cpuhours:%s team:%s era:%s procvs:%s LFNBase:%s maxRSS:%s MinMerge:%s SoftTimeout:%s\nOutputDataset: %s\nCustodialSites: %s\nWhitelist: %s" % (w,assign_data[w]['priority'],assign_data[w]['events'],assign_data[w]['cpuhours'],assign_data[w]['team'],assign_data[w]['acqera'],assign_data[w]['procversion'],assign_data[w]['mergedlfnbase'],assign_data[w]['maxRSS'],assign_data[w]['minmergesize'],assign_data[w]['softtimeout'],assign_data[w]['dataset'],assign_data[w]['custodialsites'],",".join(x for x in assign_data[w]['whitelist']))
		if options.test:
			print "TEST:\t%s\n" % suminfo
		if not options.test:
			print "ASSIGN:\t%s\n" % suminfo
			assignMCRequest(url,w,assign_data[w]['team'],assign_data[w]['whitelist'],assign_data[w]['acqera'],assign_data[w]['procversion'],assign_data[w]['mergedlfnbase'],assign_data[w]['minmergesize'],assign_data[w]['maxRSS'],assign_data[w]['custodialsites'],assign_data[w]['softtimeout'])
	
	sys.exit(0)

if __name__ == "__main__":
	main()
