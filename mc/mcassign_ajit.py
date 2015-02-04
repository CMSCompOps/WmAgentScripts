#!/usr/bin/env python
#TODO https://github.com/dmwm/WMCore/blob/master/test/data/ReqMgr/requests/ReReco.json
#TODO use reqmgr.py 
#TODO config, add split events/job for MonteCarlo
#TODO check for duplicated datasets in the known requests with the same prepid
import urllib2,urllib, httplib, sys, re, os
import optparse
import time
import datetime

try:
    import json
except ImportError:
    import simplejson as json

max_events_per_job = 500
extstring = '-ext'
teams = ['mc','mc_highprio']
t1s = {'FNAL':'T1_US_FNAL','CNAF':'T1_IT_CNAF','ASGC':'T1_TW_ASGC','IN2P3':'T1_FR_CCIN2P3','RAL':'T1_UK_RAL','PIC':'T1_ES_PIC','KIT':'T1_DE_KIT'}

siteblacklist = ['T2_FR_GRIF_IRFU','T2_PK_NCP','T2_PT_LIP_Lisbon','T2_RU_RRC_KI']
siteblacklist.extend(['T2_PL_Warsaw','T2_RU_PNPI','T2_KR_KNU','T2_UA_KIPT','T2_AT_Vienna'])
siteblacklist.extend(['T2_IN_TIFR','T2_RU_JINR','T2_UK_SGrid_RALPP'])

sitelistsmallrequests = ['T2_DE_DESY','T2_IT_Pisa','T2_ES_CIEMAT','T2_IT_Bari','T2_US_Purdue','T2_US_Caltech','T2_DE_RWTH','T2_IT_Legnaro','T2_IT_Rome','T2_US_Florida','T2_US_MIT','T2_US_Wisconsin','T2_US_UCSD','T2_US_Nebraska','T2_EE_Estonia','T2_US_Vanderbilt']
sitelisthimemrequests = ['T2_US_MIT','T2_US_Wisconsin','T2_US_Nebraska','T2_US_Vanderbilt','T2_FR_CCIN2P3']

siteliststep0long = ['T2_US_Purdue','T2_US_Nebraska','T3_US_Omaha']
gensubscriptionsites = ['T2_CH_CERN','T2_IT_Bari' ,'T2_IT_Legnaro' ,'T2_IT_Pisa' ,'T2_IT_Rome' ,'T1_IT_CNAF','T2_ES_CIEMAT','T2_ES_IFCA','T2_EE_Estonia','T2_US_Wisconsin','T1_DE_KIT' ,'T1_ES_PIC' ,'T1_FR_CCIN2P3','T1_UK_RAL_Disk' ,'T2_BE_IIHE' ,'T2_BE_UCL' ,'T2_BR_SPRACE' ,'T2_CH_CSCS' ,'T2_CN_Beijing' ,'T2_DE_DESY' ,'T2_DE_RWTH' ,'T2_FI_HIP' ,'T2_FR_CCIN2P3' ,'T2_FR_GRIF_LLR' ,'T2_FR_IPHC' ,'T2_HU_Budapest' ,'T2_IN_TIFR' ,'T2_PT_NCG_Lisbon','T2_RU_JINR' ,'T2_RU_SINP' ,'T2_TR_METU' ,'T2_TW_Taiwan' ,'T2_UK_London_Brunel' ,'T2_UK_London_IC' ,'T2_UK_SGrid_RALPP' ,'T2_US_Caltech' ,'T2_US_Florida' ,'T2_US_MIT' ,'T2_US_Nebraska' ,'T2_US_Purdue' ,'T2_US_UCSD' ,'T2_BR_UERJ','T3_US_Colorado','T2_RU_IHEP','T2_RU_ITEP']
autoapprovelist = ['T2_CH_CERN','T2_IT_Bari' ,'T2_IT_Legnaro' ,'T2_IT_Pisa' ,'T2_IT_Rome' ,'T1_IT_CNAF','T2_ES_CIEMAT','T2_ES_IFCA','T2_EE_Estonia','T2_US_Wisconsin','T1_UK_RAL_Disk','T3_US_Colorado']


cachedoverview = '/afs/cern.ch/user/s/spinoso/public/overview.cache'
forceoverview = 0

def human(n):
        if n<1000:
                return "%s" % n
        elif n>=1000 and n<1000000:
                order = 1
        elif n>=1000000 and n<1000000000:
                order = 2
        else:
                order = 3

        norm = pow(10,3*order)

        value = float(n)/norm

        letter = {1:'k',2:'M',3:'G'}
        return ("%.1f%s" % (value,letter[order])).replace(".0", "")

def setSplit(url, workflow, typ, split):
	print "Set Split %s %s %s" % (workflow, typ, split)
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

	if typ=='MonteCarloFromGEN':
		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/MonteCarloFromGEN", "splittingAlgo":"LumiBased", "lumis_per_job":str(split), "timeout":"", "include_parents":"False", "files_per_job":"",'halt_job_on_file_boundaries':'True'}
	elif typ=='MonteCarlo':
		params = {"requestName":workflow,"splittingTask" : '/'+workflow+"/Production", "splittingAlgo":"EventBased", "events_per_job":str(split), "timeout":""}
	else:
		print "Cannot set splitting on %s requests"
		sys.exit(1)


        headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
        encodedParams = urllib.urlencode(params)
        conn.request("POST", "/reqmgr/view/handleSplittingPage", encodedParams, headers)
        response = conn.getresponse()
        #print response.status, response.reason
        data = response.read()
        #print data
        conn.close()

def loadcampaignconfig(f):
        try:
                d = open(f).read()
        except:
                print "Cannot load config file %s" % f
                sys.exit(1)
        try:
                s = eval(d)
        except:
                print "Cannot eval config file %s " % f
                sys.exit(1)
	print "\nConfiguration loaded successfully from %s" % f
        return s

def get_linkedt2s(custodialt1):
	global siteblacklist
	list = []
	if custodialt1 == '':
		return []
	try:
		# get list of linked T2s
		url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/links?status=ok&to=%s_Buffer&from=T2_*" % custodialt1
		response = urllib2.urlopen(url)
		j = json.load(response)["phedex"]
		for dict in j['link']:
			if dict['from'] not in siteblacklist:
				list.append(dict['from'])

		# add list of commissioned T3s that are linked
		url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/links?status=ok&to=%s_Buffer&from=T3_*" % custodialt1
		response = urllib2.urlopen(url)
		j = json.load(response)["phedex"]
		for dict in j['link']:
			if dict['from'] in ['T3_US_Omaha','T3_US_Colorado'] and dict['from'] not in siteblacklist:
				list.append(dict['from'])

		list.sort()
		return list

	except	Exception:
        	print 'Status:',response.status,'Reason:',response.reason
        	print sys.exc_info()
		sys.exit(1)

def assignMCRequest(url,workflow,team,sitelist,era,processingstring,processingversion,mergedlfnbase,minmergesize,maxRSS,custodialsites,noncustodialsites,custodialsubtype,autoapprovesubscriptionsites,softtimeout,blockclosemaxevents,maxmergeevents):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": sitelist,
              "SiteBlacklist": [],
              "MergedLFNBase": mergedlfnbase,
              "UnmergedLFNBase": "/store/unmerged",
	      "SoftTimeout": softtimeout,
	      "BlockCloseMaxEvents": blockclosemaxevents,
              "MinMergeSize": minmergesize,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": maxmergeevents,
	      "MaxRSS": maxRSS,
              "MaxVSize": 4394967000,
              "AcquisitionEra": era,
	      "Dashboard": "production",
              "ProcessingVersion": processingversion,
              "ProcessingString": processingstring,
	      "CustodialSites":custodialsites,
	      "NonCustodialSites":noncustodialsites,
	      "AutoApproveSubscriptionSites":autoapprovesubscriptionsites,
	      "CustodialSubType":custodialsubtype,
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


def getWorkflowConfig(workflow):
	conn = httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/view/showOriginalConfig/%s' % workflow)
	r2=conn.getresponse()
	data = r2.read()
	conn.close()
	list = data.split('\n')
	list2 = []
	for i in list:
		list2.append(i.strip())
	return list2	

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
	prepmemory = 0
	requestdays=0
	campaign = ''
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
                elif '.schema.Campaign' in raw:
                        campaign = raw[raw.find("'")+1:]
                        campaign = campaign[0:campaign.find("'")]
		elif 'splitting.events_per_job' in raw:
			a = raw.find(" =")
			b = raw.find('<br')
			events_per_job = int(raw[a+3:b])
		elif 'request.schema.Memory' in raw:
			a = raw.find(" =")
			b = raw.find('<br')
			prepmemory = int(raw[a+3:b])
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
	# TODO to be fixed
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
	remainingcpuhours = timeev*(expectedevents-eventsdone)/3600
	return {'requestname':workflow,'type':typ,'status':status,'campaign':campaign,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'priority':priority,'sites':sites,'custodialt1':custodialt1,'js':j,'outputdataset':outputdataset,'cpuhours':cpuhours,'remainingcpuhours':remainingcpuhours,'team':team,'acquisitionEra':acquisitionEra,'requestdays':requestdays,'processingVersion':processingVersion,'events_per_job':events_per_job,'lumis_per_job':lumis_per_job,'expectedjobs':expectedjobs,'expectedjobcpuhours':expectedjobcpuhours,'cmssw':cmssw,'outputtier':outputtier,'prepmemory':prepmemory}

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

def getteam(team,r):
	global teams
	if team == 'auto':
		#if r['expectedevents'] <=100000 or r['priority'] >= 100000:
		if r['expectedevents'] <=100000 and r['priority'] >= 80000:
			newteam = 'mc_highprio'
		else:
			newteam = 'mc'
	else:
		newteam = team
	return newteam

def main():
	global overview,forceoverview,sum,nodbs,siteblacklist,teams

	campaignconfig = loadcampaignconfig('/afs/cern.ch/user/c/cmst2/public/MCCONFIG/campaign.cfg')
	overview = getoverview()
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-w', '--workflow', help='workflow name',dest='workflow')
	parser.add_option('--debug', help='add debug info',dest='debug',default=False,action="store_true")
	parser.add_option('-l', '--workflowlist', help='workflow list in textfile',dest='list')
	parser.add_option('-t', '--team', help='team (one of %s)' % (",".join(x for x in teams)),dest='team')
	parser.add_option('--test', action="store_true",default=True,help='test mode: don\'treally assign at the end',dest='test')
	parser.add_option('--assign', action="store_false",default=True,help='assign mode',dest='test')
	parser.add_option('--small', action="store_true",default=False,help='assign requests considering them small',dest='small')
	parser.add_option('--hi', action="store_true",default=False,help='heavy ion request (add Vanderbilt to the whitelist, use /store/himc)',dest='hi')
	parser.add_option('--himem', action="store_true",default=False,help='high memory request (use sites allowing 3GB/job, increase maxRSS)',dest='himem')
	parser.add_option('-e','--extension', action="store_true",default=False,help='extension (add -ext to the processing string)',dest='ext')
	parser.add_option('-c', '--custodialt1', help='Custodial T1',dest='custodialt1')
	parser.add_option('--tapefamilies', help='Tape Families',dest='tapefamilies')
	parser.add_option('-s', '--sites', help='Single site or comma-separated list (i.e. T1_US_FNAL,T2_FR_CCIN2P3,T2_DE_DESY)',dest='sites')
	parser.add_option('-a', '--acqera', help='<AcquisitionEra> in <AcquisitionEra>-<ProcString>-v<ProcVer>',dest='acqera')
	parser.add_option('-p', '--processingstring', help='<ProcString> in <AcquisitionEra>-<ProcString>-v<ProcVer>, default=GlobalTag-vX',dest='processingstring')
	parser.add_option('-x', '--specialprocstringextn', help='<ProcString> in <AcquisitionEra>-<ProcString>-v<ProcVer>, default=GlobalTag-vX',dest='specialprocstringextn')
	parser.add_option('-v', '--processingversion', help='<ProcVer> in <AcquisitionEra>-<ProcString>-v<ProcVer>), default=1',dest='processingversion')
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
		team = options.team
	else:
		team = 'auto'

	if options.custodialt1:
		custodialt1 = options.custodialt1
		if custodialt1 not in t1s.values():
			if custodialt1 in t1s.keys():
				custodialt1 = t1s[custodialt1]
			else:
				print "%s is not a T1." % custodialt1
				sys.exit(1)
	else:
		custodialt1 = ''
	if options.sites:
		sites = options.sites
	else:
		sites = 'auto'
	if options.processingversion:
		processingversion = options.processingversion
	else:
		processingversion = '1'

	if options.processingstring:
		processingstring = options.processingstring
	else:
		processingstring = 'auto'

	if options.specialprocstringextn:
		specialprocstringextn = options.specialprocstringextn
	else:
		specialprocstringextn = ''
		
	reqinfo = {}

	if options.acqera:
		acqera = options.acqera
	else:
		acqera = 'auto'

	siteblacklist.sort()
	print "Default site blacklist: %s\n" % (",".join(x for x in siteblacklist))
	print "T1s: %s" % (t1s.keys())
		
	#if options.hi:
	#	print "Heavy Ion flag is set, parameters will be configured accordingly\n"

	if options.himem:
		print "High memory flag is set, parameters will be configured accordingly\n"

	if options.tapefamilies:
		campaigns = []
		acqera = None
		batch = options.tapefamilies
		print "Building tape families for batch %s\n" % batch
		if custodialt1 == '':
			print "Please provide the custodial T1"
			sys.exit(1)
		else:
			if custodialt1 not in t1s.keys():
				if custodialt1 in t1s.values():
					for i in t1s.keys():
						if custodialt1 == t1s[i]:
							custodialt1 = i
							break
		totalevents = 0
		for w in list:
			reqinfo[w] = getWorkflowInfo(w)
			totalevents = totalevents + reqinfo[w]['expectedevents']
			print "%s (%s events)" % (w,human(reqinfo[w]['expectedevents']))
			if reqinfo[w]['campaign'] not in campaignconfig.keys():
				print "Unknown campaign %s" % reqinfo[w]['campaign']
				sys.exit(1)
			else:
				if reqinfo[w]['campaign'] not in campaigns:
					campaigns.append(reqinfo[w]['campaign'])
			if acqera:
				if acqera != campaignconfig[reqinfo[w]['campaign']]['acqera']:
					for i in reqinfo.keys():
						print "\nERROR\n\n%s->%s" % (reqinfo[i]['requestname'],campaignconfig[reqinfo[w]['campaign']]['acqera'])
						break
					print "%s->%s" % (w,campaignconfig[reqinfo[w]['campaign']]['acqera'])
					print "\nRequests have different acquisition era. Aborting."
					sys.exit(1)
			else:
				acqera = campaignconfig[reqinfo[w]['campaign']]['acqera']
				print "[Detected acquisition era: %s]" % (acqera)
		print "\n----------------------------------------------------------------\n"
		print "\nCustodial LFNs for %s %s (%s)\n" % (" ".join(x for x in campaigns),batch,custodialt1)
		print "Dear admins,\n\nplease create the tape families below[*], needed for MC production.\n\n"
		
		tf = []
		ids = []
		for w in reqinfo.keys():
			if reqinfo[w]['type'] == 'MonteCarloFromGEN':
				ids.append(reqinfo[w]['inputdataset']['name'])
			if 'tfpath' in campaignconfig[reqinfo[w]['campaign']].keys():	
				tfpath = campaignconfig[reqinfo[w]['campaign']]['tfpath']
			else:
				tfpath = 'mc'
			if 'tiers' in campaignconfig[reqinfo[w]['campaign']].keys():	
				tiers = campaignconfig[reqinfo[w]['campaign']]['tiers']
			else:
				tiers = ['GEN-SIM','GEN-SIM-RECO','DQM','AODSIM']

			for tier in tiers:
				a = "/store/%s/%s/%s/%s" % (tfpath,acqera,reqinfo[w]['primaryds'],tier)
				if a not in tf:
					tf.append(a)

		tf.sort()
		print "[*]"
		for i in tf:
			print "%s" % i

		if ids:
			print "\nYou may want to replicate the following input datasets if needed:\n"
			for i in ids:
				print "%s" % i

		print "\n\nThanks!\n Vincenzo & Ajit.\n\n"
		print "TOTAL EVENTS: %s" % human(totalevents)
		print "\nPREPIDs: %s\n" % (",".join(reqinfo[x]['prepid'] for x in reqinfo.keys()))
		sys.exit(0)
			

	print "Preparing requests:\n"
	assign_data = {}
	datasets = {}
	for w in list:
		reqinfo[w] = getWorkflowInfo(w)
		if '_extension_' in w or options.ext:
			ext = extstring
		else:
			ext = ''

		if reqinfo[w]['campaign'] not in campaignconfig.keys():
			print "\nUnknown campaign %s for %s\n" % (reqinfo[w]['campaign'],w)
			sys.exit(1)

		if reqinfo[w]['type'] == 'MonteCarloFromGEN':
			if reqinfo[w]['inputdataset']['events'] == 0:
				print "\n%s input dataset is empty!\n" % w
				sys.exit(1)
		
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
		prepmemory = reqinfo[w]['prepmemory']
		#hi = False
		himem = False
		blockclosemaxevents = 250000000
		maxmergeevents = 50000
		noncustodialsites = []
		custodialsubtype = "Move"
		autoapprovesubscriptionsites = []
		if reqinfo[w]['type'] == 'LHEStepZero':
			autoapprovesubscriptionsites = autoapprovelist
			if 'T1_US_FNAL' not in autoapprovesubscriptionsites:
				autoapprovesubscriptionsites.append('T1_US_FNAL')
			noncustodialsites = gensubscriptionsites
			custodialsubtype = "Replica"
			blockclosemaxevents = 20000000
			maxmergeevents = 4000000
			minmergesize = 1000000000
			softtimeout = 129600*2
			maxRSS = 2294967
			if '_STEP0ATCERN' in w:
				# STEP0 @ CERN
				print '%s (%s, LHEStepZero step0)' % (w,reqinfo[w]['campaign'])
				team = 'step0'
			else:
				# full GEN
				print '%s (%s, LHEStepZero full GEN)' % (w,reqinfo[w]['campaign'])
				team = 'mc_highprio'
				
		#elif options.hi:
		#	print '%s (%s Heavy Ion)' % (w,reqinfo[w]['campaign'])
		#	hi = True
		#	team = getteam(team,reqinfo[w])
		#	softtimeout = 129600
		#	minmergesize = 2147483648
		#	#maxRSS = 3500000
		#	maxRSS = 2294967
		elif options.himem:
			himem = True
			print '%s (%s High Memory)' % (w,reqinfo[w]['campaign'])
			team = getteam(team,reqinfo[w])
			softtimeout = 129600
			minmergesize = 2147483648
			maxRSS = 3500000
		else:
			print '%s (%s)' % (w,reqinfo[w]['campaign'])
			team = getteam(team,reqinfo[w])
			softtimeout = 129600
			minmergesize = 2147483648
			maxRSS = 2294967

		

		if custodialt1 == '':
			if options.himem:
				custodialt1 = 'T1_FR_CCIN2P3'
			elif reqinfo[w]['type'] == 'LHEStepZero':
				custodialt1 = 'T1_US_FNAL'
			else:
				print "Cannot guess the custodial T1. Please use -c <site>."
				sys.exit(1)

		# LFN path
		if reqinfo[w]['type'] == 'LHEStepZero':
			mergedlfnbase = '/store/generator'
		elif 'tfpath' in campaignconfig[reqinfo[w]['campaign']].keys():
			mergedlfnbase = "/store/%s" % campaignconfig[reqinfo[w]['campaign']]['tfpath']
		else:
			mergedlfnbase = '/store/mc'

		# sitelist adjustment
		linkedt2list = get_linkedt2s(custodialt1)
		if sites == 'auto':
			if reqinfo[w]['type'] == 'LHEStepZero' and '_STEP0ATCERN' in w:
				newsitelist = ['T2_CH_CERN']
			else:
				newsitelist = []
				newsitelist.append('T1_UK_RAL')
				newsitelist.append('T1_IT_CNAF')
				newsitelist.append('T1_ES_PIC')
				if custodialt1 not in newsitelist:
					newsitelist.append(custodialt1)
				newsitelist.extend(linkedt2list)
				#TODO: add new flag for special behaviours? or white/blacklists?
				if mergedlfnbase == '/store/himc':
					newsitelist.remove('T1_UK_RAL')
					newsitelist.remove('T1_IT_CNAF')
					newsitelist.remove('T1_ES_PIC')
				else:
					newsitelist.remove('T2_US_Vanderbilt')
		else:
			# explicit sitelist, no guesses
			newsitelist = sites.split(',')
			for i in newsitelist:
				if 'T2_' in i:
					if not i in linkedt2list:
						print "%s has no PhEDEx uplink to %s" % (i,custodialt1)
						sys.exit(1)

		"""
		if options.hi:
			oldsitelist = newsitelist[:]
			newsitelist = []
			for i in oldsitelist:
				if 'T1_' in i:
					newsitelist.append(i)
				elif i in sitelisthirequests:
					newsitelist.append(i)
		"""
		if options.himem:
			oldsitelist = newsitelist[:]
			newsitelist = []
			for i in oldsitelist:
				if 'T1_' in i:
					newsitelist.append(i)
				elif i in sitelisthimemrequests:
					newsitelist.append(i)
		#elif reqinfo[w]['priority'] >= 100000 or options.small:
		elif options.small:
			oldsitelist = newsitelist[:]
			newsitelist = []
			for i in oldsitelist:
				if 'T1_' in i:
					newsitelist.append(i)
				elif i in sitelistsmallrequests:
					newsitelist.append(i)

		if reqinfo[w]['type'] == 'MonteCarloFromGEN' and reqinfo[w]['campaign'] in campaignconfig.keys():
			if 'lumisperjob' in campaignconfig[reqinfo[w]['campaign']].keys():
				setSplit(url,w,reqinfo[w]['type'],campaignconfig[reqinfo[w]['campaign']]['lumisperjob'])
				reqinfo[w]['lumis_per_job'] = campaignconfig[reqinfo[w]['campaign']]['lumisperjob']
		elif reqinfo[w]['type'] == 'MonteCarlo':
			pass
			#if reqinfo[w]['events_per_job'] > max_events_per_job:
				#setSplit(url,w,reqinfo[w]['type'],max_events_per_job)

		# processing string and processing version
		if acqera != 'auto':
			newacqera = acqera
		else:
			if reqinfo[w]['campaign'] in campaignconfig.keys():
				if 'acqera' in campaignconfig[reqinfo[w]['campaign']].keys():
					newacqera = campaignconfig[reqinfo[w]['campaign']]['acqera']
				else:
					newacqera = reqinfo[w]['campaign']
			else:
				newacqera = reqinfo[w]['campaign']

		if processingstring != 'auto':
			newprocessingstring = processingstring
		else:
			if reqinfo[w]['campaign'] in campaignconfig.keys():
				if reqinfo[w]['campaign'] == 'Summer12_FS53' and reqinfo[w]['prepid'].split()[0] == 'TOP':
					# if PU is 2012_Startup_inTimeOnly (default) no additional string is needed; 
					# if PU is 2012_Summer_inTimeOnly then "PU_S12" must be added to the dataset name
					for i in getWorkflowConfig(w):
						if 'PileUpProducer.PileUpSimulator' in i:
							if '2012_Summer_inTimeOnly' in i:
								newprocessingstring = campaignconfig[reqinfo[w]['campaign']]['procstr']
								newprocessingstring = newprocessingstring.replace('GLOBALTAG',"%s_PU_S12" % reqinfo[w]['globaltag'])
				if 'procstr' in campaignconfig[reqinfo[w]['campaign']].keys():
					newprocessingstring = campaignconfig[reqinfo[w]['campaign']]['procstr']
					newprocessingstring = newprocessingstring.replace('GLOBALTAG',reqinfo[w]['globaltag'])
				else:
					newprocessingstring = reqinfo[w]['globaltag']
			else:
				newprocessingstring = reqinfo[w]['globaltag']
		
		newprocessingstring = "%s%s" % (newprocessingstring,ext)
                if options.specialprocstringextn:
                    newprocessingstring = "%s-%s" % (newprocessingstring,specialprocstringextn)
				
		dataset = '/%s/%s-%s-v%s/%s' % (reqinfo[w]['primaryds'],newacqera,newprocessingstring,processingversion,reqinfo[w]['outputtier'])
		if isInDBS(dataset):
			print "Dataset already exists in DBS: %s -> %s" % (w,dataset)
			sys.exit(1)
		if dataset in datasets.values():
			print "Requests having same output datasets:\n%s -> %s" % (w,dataset)
			for i in datasets.keys():
				if datasets[i] == dataset:
					print "%s -> %s" % (i,datasets[i])
			sys.exit(1)
		datasets[w] = dataset

		# save the parameters for assignment of request 'w'
		assign_data[w] = {}
		assign_data[w]['team'] = team
		assign_data[w]['priority'] = priority
		assign_data[w]['events'] = reqinfo[w]['expectedevents']
		assign_data[w]['cpuhours'] = reqinfo[w]['cpuhours']
		newsitelist.sort()
		assign_data[w]['whitelist'] = newsitelist
		assign_data[w]['acqera'] = newacqera
		assign_data[w]['processingstring'] = newprocessingstring
		assign_data[w]['processingversion'] = processingversion
		assign_data[w]['custodialsites'] = [custodialt1]
		noncustodialsites.sort()
		assign_data[w]['noncustodialsites'] = noncustodialsites
		assign_data[w]['custodialsubtype'] = custodialsubtype
		autoapprovesubscriptionsites.sort()
		assign_data[w]['autoapprovesubscriptionsites'] = autoapprovesubscriptionsites
		assign_data[w]['dataset'] = dataset
		assign_data[w]['mergedlfnbase'] = mergedlfnbase
		assign_data[w]['prepmemory'] = prepmemory
		assign_data[w]['maxRSS'] = maxRSS
		assign_data[w]['minmergesize'] = minmergesize
		assign_data[w]['softtimeout'] = softtimeout
		assign_data[w]['blockclosemaxevents'] = blockclosemaxevents
		assign_data[w]['maxmergeevents'] = maxmergeevents
		if reqinfo[w]['type'] == 'MonteCarlo':
			assign_data[w]['split'] = reqinfo[w]['events_per_job']
			splitstring = "events_per_job"
		elif reqinfo[w]['type'] == 'MonteCarloFromGEN':
			assign_data[w]['split'] = reqinfo[w]['lumis_per_job']
			splitstring = "lumis_per_job"
		elif reqinfo[w]['type'] == 'LHEStepZero':
			assign_data[w]['split'] = ""
			splitstring = ""
		else:
			print "Cannot determine splitting for type %s" % reqinfo[w]['type']
			sys.exit(1)

	print "\n----------------------------------------------\n"

	print "Command line options:\n"
	for ky in options.__dict__.keys():
		if options.__dict__[ky]:
			print "\t%s:\t%s" % (ky,options.__dict__[ky])
	print

	if not options.test: 
		print "Assignment:"
		print

	for w in list:
		suminfo = "%s\n\tcampaign: %s prio:%s events:%s cpuhours:%s %s:%s\n\tteam:%s era:%s procstr: %s procvs:%s\n\tLFNBase:%s PREPmem: %s maxRSS:%s MinMerge:%s SoftTimeout:%s BlockCloseMaxEvents:%s MaxMergeEvents:%s\n\tOutputDataset: %s\n\tCustodialSites: %s\n\tNonCustodialSites: %s\n\tCustodialSubType: %s\n\tAutoApprove: %s\n\tWhitelist: %s" % (w,reqinfo[w]['campaign'],assign_data[w]['priority'],assign_data[w]['events'],assign_data[w]['cpuhours'],splitstring,assign_data[w]['split'],assign_data[w]['team'],assign_data[w]['acqera'],assign_data[w]['processingstring'],assign_data[w]['processingversion'],assign_data[w]['mergedlfnbase'],assign_data[w]['prepmemory'],assign_data[w]['maxRSS'],assign_data[w]['minmergesize'],assign_data[w]['softtimeout'],assign_data[w]['blockclosemaxevents'],assign_data[w]['maxmergeevents'],assign_data[w]['dataset'],assign_data[w]['custodialsites'],assign_data[w]['noncustodialsites'],assign_data[w]['custodialsubtype'],assign_data[w]['autoapprovesubscriptionsites'],",".join(x for x in assign_data[w]['whitelist']))
		if options.test:
			print "TEST:\t%s\n" % suminfo
		if not options.test:
			print "ASSIGN:\t%s\n" % suminfo
			assignMCRequest(url,w,assign_data[w]['team'],assign_data[w]['whitelist'],assign_data[w]['acqera'],assign_data[w]['processingstring'],assign_data[w]['processingversion'],assign_data[w]['mergedlfnbase'],assign_data[w]['minmergesize'],assign_data[w]['maxRSS'],assign_data[w]['custodialsites'],assign_data[w]['noncustodialsites'],assign_data[w]['custodialsubtype'],assign_data[w]['autoapprovesubscriptionsites'],assign_data[w]['softtimeout'],assign_data[w]['blockclosemaxevents'],assign_data[w]['maxmergeevents'])
	
	sys.exit(0)

if __name__ == "__main__":
	main()
