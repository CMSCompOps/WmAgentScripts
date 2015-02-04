#!/usr/bin/env python
#TODO https://github.com/dmwm/WMCore/blob/master/test/data/ReqMgr/requests/ReReco.json
#TODO use reqmgr.py 
#TODO config, add split events/job for MonteCarlo
#TODO check for duplicated dataset names in the known requests with the same prepid
#TODO add --use-testbed
import urllib2,urllib, httplib, sys, re, os
import optparse
import time
import datetime

try:
    import json
except ImportError:
    import simplejson as json

defaulteventsperlumi = 300
max_events_per_job = 1000
max_jobduration = 30
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

def checkdups(r):
	global overview
	reqs = []
	for rr in overview:
		if r['prepid'] not in rr['request_name'] or r['requestname'] == rr['request_name'] or r['type'] != rr['type']:
			continue
		rrr = getWorkflowInfo(rr['request_name'])
		if r['prepid'] == rrr['prepid'] and rrr['status'] in ['assigned','acquired','running','running-open','running-closed','completed','closed-out','announced','aborted']:
			reqs.append(rrr['requestname'])
	return reqs

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
			elif dict['from'] in siteblacklist:
				print "%s blacklisted" % dict['from']

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
	lheinputfiles = 0
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
		elif 'schema.LheInputFiles' in raw:
			lheinputfiles = raw[raw.find("'")+1:]
			lheinputfiles = lheinputfiles[0:lheinputfiles.find("'")]
			if lheinputfiles == 'True':
				lheinputfiles = 1
			else:
				lheinputfiles = 0
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
	else:
		expectedevents = -1
		expectedjobs = -1
	
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
		if nodbs:
			[oe,ost] = [0,'']
		else:
			[oe,ost] = getdsdetail(o)
		oel['events'] = oe
		oel['status'] = ost
		outputdataset.append(oel)
		
	ret = {'requestname':workflow,'type':typ,'status':status,'campaign':campaign,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'priority':priority,'sites':sites,'custodialt1':custodialt1,'outputdataset':outputdataset,'team':team,'acquisitionEra':acquisitionEra,'requestdays':requestdays,'processingVersion':processingVersion,'events_per_job':events_per_job,'lumis_per_job':lumis_per_job,'expectedjobs':expectedjobs,'cmssw':cmssw,'outputtier':outputtier,'prepmemory':prepmemory,'lheinputfiles':lheinputfiles,'filtereff':filtereff}
	#print ret
	return ret

def main():
	global overview,forceoverview,sum,nodbs,siteblacklist,teams

	campaignconfig = loadcampaignconfig('/afs/cern.ch/user/c/cmst2/public/MCCONFIG/campaign.cfg')
	overview = getoverview()
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-w', '--workflow', help='workflow name',dest='workflow')
	parser.add_option('-l', '--workflowlist', help='workflow list in textfile',dest='list')
	parser.add_option('-e', '--eventsperjob', help='set events_per_job=E directly (no filter efficiency considered)' ,dest='e')
	parser.add_option('-f', '--eventsperlumidr', help='set events_per_job=EDR/filtereff (filter efficiency is considered)' ,dest='edr')
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

	reqinfo = {}
	print
	for w in list:
		reqinfo[w] = getWorkflowInfo(w)
		if reqinfo[w]['type'] != 'MonteCarlo':
			print "%s: type %s not supported" % (w,reqinfo[w]['type'])
			continue
		if options.edr:
			feff = reqinfo[w]['filtereff']
			old_events_per_job = reqinfo[w]['events_per_job']
			print "%s: events_per_job=%s filtereff=%s events/job(GEN-SIM)=%s" % (w,old_events_per_job,feff,int(old_events_per_job*feff))
			edr = float(options.edr)
			events_per_job = int(edr/feff)
			print "Setting events_per_job=%s to get %s events/job in the GEN-SIM (filtereff=%s)" % (events_per_job,edr,feff)
			setSplit(url,w,reqinfo[w]['type'],events_per_job)
	
		elif options.e:
			feff = reqinfo[w]['filtereff']
			old_events_per_job = reqinfo[w]['events_per_job']
			print "%s: events_per_job=%s filtereff=%s events/job(GEN-SIM)=%s" % (w,old_events_per_job,feff,int(old_events_per_job*feff))
			events_per_job = float(options.e)
			edr = events_per_job*feff
			print "Setting events_per_job=%s, we'll get %s events/job in the GEN-SIM (filtereff=%s)" % (events_per_job,edr,feff)
			setSplit(url,w,reqinfo[w]['type'],events_per_job)
	
	print
	sys.exit(0)

if __name__ == "__main__":
	main()
