#!/usr/bin/env python
#TODO https://cmsweb.cern.ch/wmstats/_design/WMStats/_view/requestByStatusAndType?stale=update_after
#TODO https://github.com/dmwm/WMCore/blob/master/test/data/ReqMgr/requests/ReReco.json
#TODO use reqmgr.py 
#TODO config, add split events/job for MonteCarlo
#TODO http://dashb-cms-sum.cern.ch/dashboard/request.py/latestresultssmry-sum#profile=CMS_CRITICAL_FULL&group=AllGroups&site[]=All+Sites&flavour[]=All+Service+Flavours&metric[]=org.cms.WN-xrootd-fallback&status[]=OK
import urllib2,urllib, httplib, sys, re, os
import optparse
import time
import datetime
import math
from dbs.apis.dbsClient import DbsApi

try:
    import json
except ImportError:
    import simplejson as json

max_events_per_job = 500
extstring = '_ext'
teams = ['production']
t1s = {'FNAL':'T1_US_FNAL','CNAF':'T1_IT_CNAF','IN2P3':'T1_FR_CCIN2P3','RAL':'T1_UK_RAL','PIC':'T1_ES_PIC','KIT':'T1_DE_KIT'}

siteblacklist = []
siteblacklist = ['T2_TH_CUNSTDA','T1_TW_ASGC','T2_TW_Taiwan']

sitelistsmallrequests = ['T2_DE_DESY','T2_FR_CCIN2P3','T2_IT_Bari','T2_US_Purdue','T2_DE_RWTH','T2_IT_Legnaro','T2_IT_Rome','T2_US_Florida','T2_US_MIT','T2_US_UCSD','T2_US_Vanderbilt','T2_CH_CERN','T2_US_Nebraska','T2_US_Caltech','T2_US_Wisconsin','T2_UK_London_IC']
sitelisthimemrequests = ['T1_FR_CCIN2P3','T1_RU_JINR','T2_ES_IFCA','T2_FR_CCIN2P3','T2_US_Florida','T2_US_Nebraska','T2_US_Vanderbilt']


#siteliststep0long = ['T2_US_Purdue','T2_US_Nebraska','T3_US_Omaha']
#gensubscriptionsites = ['T2_CH_CERN','T2_IT_Bari' ,'T2_IT_Legnaro' ,'T2_IT_Pisa' ,'T2_IT_Rome' ,'T1_IT_CNAF','T2_ES_CIEMAT','T2_ES_IFCA','T2_EE_Estonia','T2_US_Wisconsin','T1_DE_KIT' ,'T1_ES_PIC' ,'T1_FR_CCIN2P3','T1_UK_RAL' ,'T2_BE_IIHE' ,'T2_BE_UCL' ,'T2_BR_SPRACE' ,'T2_CH_CSCS' ,'T2_CN_Beijing' ,'T2_DE_DESY' ,'T2_DE_RWTH' ,'T2_FI_HIP' ,'T2_FR_CCIN2P3' ,'T2_FR_GRIF_LLR' ,'T2_FR_IPHC' ,'T2_HU_Budapest' ,'T2_IN_TIFR' ,'T2_PT_NCG_Lisbon','T1_RU_JINR' ,'T2_RU_SINP' ,'T2_TW_Taiwan' ,'T2_UK_London_Brunel' ,'T2_UK_London_IC' ,'T2_UK_SGrid_RALPP' ,'T2_US_Caltech' ,'T2_US_Florida' ,'T2_US_MIT' ,'T2_US_Nebraska' ,'T2_US_Purdue' ,'T2_US_UCSD' ,'T3_US_Colorado','T2_RU_IHEP','T2_RU_ITEP']
gensubscriptionsites = ['T2_CH_CERN','T2_IT_Bari' ,'T2_IT_Legnaro' ,'T2_IT_Pisa' ,'T2_IT_Rome' ,'T2_ES_CIEMAT','T2_ES_IFCA','T2_EE_Estonia','T2_US_Wisconsin','T2_BE_IIHE' ,'T2_BE_UCL' ,'T2_BR_SPRACE' ,'T2_CH_CSCS' ,'T2_CN_Beijing' ,'T2_DE_DESY' ,'T2_DE_RWTH' ,'T2_FI_HIP' ,'T2_FR_CCIN2P3' ,'T2_FR_GRIF_LLR' ,'T2_FR_IPHC' ,'T2_HU_Budapest' ,'T2_IN_TIFR' ,'T2_PT_NCG_Lisbon','T2_RU_SINP' ,'T2_TW_Taiwan' ,'T2_UK_London_Brunel' ,'T2_UK_London_IC' ,'T2_UK_SGrid_RALPP' ,'T2_US_Caltech' ,'T2_US_Florida' ,'T2_US_MIT' ,'T2_US_Nebraska' ,'T2_US_Purdue' ,'T2_US_UCSD' ,'T3_US_Colorado','T2_RU_IHEP','T2_RU_ITEP']

autoapprovelist = ['T2_CH_CERN','T2_IT_Bari' ,'T2_IT_Legnaro' ,'T2_IT_Pisa' ,'T2_IT_Rome' ,'T1_IT_CNAF','T2_ES_CIEMAT','T2_ES_IFCA','T2_EE_Estonia','T2_US_Wisconsin','T1_UK_RAL','T3_US_Colorado']

cachedoverview = '/afs/cern.ch/user/c/cmst2/public/overview.cache'
forceoverview = 0

def getwhitelist():
	global siteblacklist
	d=open('/afs/cern.ch/user/c/cmst2/www/mc/whitelist.json').read()
	list = []
	for i in json.loads(d):
		if i not in siteblacklist:
			list.append(i)
	return list

def getRequestsByPREPID(prepid):
        r = []
        for i in overview:
                if prepid in i['request_name']:
                        r.append(i['request_name'])
        return r

def getCustodialFromClones(reqlist):
	ret = []
	for i in reqlist:
		r=getWorkflowInfo(i,nodbs=1)
		if r['custodialt1'] != '?' and r['custodialt1'] not in ret:
			ret.append(r['custodialt1'])
	return ret

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

def setParam(url, workflow, params,debug):
	#print "Set Param for %s" % (workflow)
	#print json.dumps(params,indent=4)

        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

        headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
        encodedParams = urllib.urlencode(params)
        conn.request("POST", "/reqmgr/view/handleSplittingPage", encodedParams, headers)
        response = conn.getresponse()
        data = response.read()
	if response.status != 200:
        	print response.status, response.reason
        	print data
		sys.exit(1)
        conn.close()
	print "Done."

def setSplit(url, workflow, typ, split):
	return
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

def get_linkedsites(custodialt1):
	global siteblacklist
	list = []
	if custodialt1 == '':
		return []

	url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/links?status=ok&to=%s_Buffer&from=T2_*" % custodialt1
	try:
		response = urllib2.urlopen(url)
	except:
		print "Cannot get list of linked T2s for %s" % custodialt1
		print "url = %s" % url
        	print 'Status:',response.status,'Reason:',response.reason
        	print sys.exc_info()
		sys.exit(1)

	j = json.load(response)["phedex"]
	for dict in j['link']:
		if dict['from'] not in siteblacklist:
			list.append(dict['from'])

	url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/links?status=ok&to=%s_Buffer&from=T1_*_Disk" % custodialt1
	try:
		response = urllib2.urlopen(url)
	except:
		print "Cannot get list of linked T1s for %s" % custodialt1
		print "url = %s" % url
        	print 'Status:',response.status,'Reason:',response.reason
        	print sys.exc_info()
		sys.exit(1)

	j = json.load(response)["phedex"]
	for dict in j['link']:
		t1 = dict['from'].replace('_Disk','')
		if t1 not in siteblacklist and t1 not in ['T1_US_FNAL','T1_FR_CCIN2P3','T1_ES_PIC','T1_IT_CNAF']:
			list.append(t1)

	url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/links?status=ok&to=%s_Buffer&from=T3_*" % custodialt1
	try:
		response = urllib2.urlopen(url)
	except	Exception:
        	print 'Status:',response.status,'Reason:',response.reason
        	print sys.exc_info()
		sys.exit(1)
	j = json.load(response)["phedex"]
	for dict in j['link']:
		if dict['from'] in ['T3_US_Omaha','T3_US_Colorado'] and dict['from'] not in siteblacklist:
			list.append(dict['from'])

	list.sort()
	#print list
	return list

def assignMCRequest(url,workflow,team,sitelist,era,processingstring,processingversion,mergedlfnbase,minmergesize,maxRSS,custodialsites,noncustodialsites,custodialsubtype,autoapprovesubscriptionsites,softtimeout,blockclosemaxevents,maxmergeevents):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": sitelist,
              "SiteBlacklist": [],
              "useSiteListAsLocation": True,
              "MergedLFNBase": mergedlfnbase,
              "UnmergedLFNBase": "/store/unmerged",
	      "SoftTimeout": softtimeout,
	      "BlockCloseMaxEvents": blockclosemaxevents,
              "MinMergeSize": minmergesize,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": maxmergeevents,
	      "MaxRSS": maxRSS,
	      "maxRSS": maxRSS,
              "MaxVSize": 4394967000,
              "AcquisitionEra": era,
	      "Dashboard": "production",
	      "dashboard": "production",
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
        #print data
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
    [e,st,evperlumi] = dbs3_get_data(dataset)
    if e == -1:
        return [0,'',0]
    else:
        return [e,st,evperlumi]

dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
def dbs3_get_data(dataset,timestamps=1):
    
    #q = "/afs/cern.ch/user/s/spinoso/public/dbs3wrapper.sh /afs/cern.ch/user/c/cmst2/mc/scripts/datasetinfo.py --dataset %s --json" % dataset
    #output=os.popen(q).read()
    #s = json.loads(output)
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    try:
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*',detail=True)
        #print reply
        if len(reply):
            status=reply[0]['dataset_access_type']
            reply = dbsapi.listBlockSummaries(dataset=dataset,detail=True)
            cnt=0
            for block in reply:
                cnt += int(block['num_event'])
            return [cnt,status,int(cnt/100.)]
        else:
            print dataset,"not exsiting"
            return [0,'',0]

    except:
        print "crash dbs3"
        return [0,'',0]            
    #if 'num_event' in s.keys():
    #    return [s['num_event'],s['dataset_access_type'],int(s['num_event']/s['num_lumi'])]
    #else:



def das_get_data(dataset):
	#das_hosts = ['https://cmsweb-testbed.cern.ch','https://cmsweb.cern.ch']
	das_hosts = ['https://cmsweb.cern.ch','https://cmsweb-testbed.cern.ch']
	count = 20
	c = 0
	while c < count:
		das_host = das_hosts[c % 2]
        	q = 'python26 /afs/cern.ch/user/c/cmst2/das_cli.py --host="%s" --query "dataset dataset=%s status=*|grep dataset.status,dataset.nevents" --format=json' % (das_host,dataset)
		#print "Querying DAS[1] try %s q=%s" % (c,q)
        	output=os.popen(q).read()
		#print output
        	output = output.rstrip()
		if '{"status": "fail"' in output:
                        c=c+1
                        print "FAIL-%s: %s" %(c,output)
                        time.sleep(10*c)
                        continue
                else:
                        break
	if c == count:
		print "Access to DAS failed"
                print "q= %s" % q
                return [0, '', 0, 0]
		sys.exit(1)
	if output == "[]":
                return [0, '', 0, 0] # dataset is not in DBS
	#print ">%s<" % output
        tmp = eval(output)
        if type(tmp) == list:
                if 'dataset' in tmp[0].keys():
                        for i in tmp[0]['dataset']:
                                if i:
                                        break
                        events = i['nevents']
                        status = i['status']
	count = 20
	c = 0
	while c < count:
		das_host = das_hosts[c % 2]
        	q = "python26 /afs/cern.ch/user/c/cmst2/das_cli.py --host=\"%s\" --query \"run lumi dataset=%s | count(lumi)\" | uniq | awk -F '=' '/count\(lumi\)/{print $2}'" % (das_host,dataset)
		#print "Querying DAS[2] try %s q=%s" % (c,q)
		output=os.popen(q).read()
        	output = output.rstrip()
        	#if output == '' or (type(output) == dict and 'status' in output.keys() and output['status']=='fail'):
		if '{"status": "fail"' in output or output=='':
			print "DAS Query[%s] FAILED: q=\"%s\"" % (c,q)
			print "RETURN: %s" % output
			c = c + 1
			time.sleep(10*c)
			continue
		else:
			break
	if c == count:
		print "Access to DAS failed"
		sys.exit(1)
	if output == "[]":
                return [0, '',0] # dataset is not in DBS
	#print ">%s<" % output
	evperlumi = int(events / eval(output))
	
        ret = [int(events),status,evperlumi]
	print "%s %s" % (dataset,ret)
        return ret

def getWorkflowCouchConfigFile(configurl):
	if configurl == '':
		return []
	conn = httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET',configurl.split('cmsweb.cern.ch')[1])
	r2=conn.getresponse()
	data = r2.read()
	conn.close()
	list = data.split('\n')
	list2 = []
	for i in list:
		list2.append(i.strip())
	return list2	

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

	custodialt1 = '?'
	primaryds = ''
	priority = -1
	timeev = -1
	prepid = ''
	globaltag = ''
	sites = []
	events_per_job = None
	events_per_lumi = None
	lumis_per_job = None
	acquisitionEra = None
	processingVersion = None
	outputtier = None
	lheInputFiles = False
	reqevts = 0
	prepmemory = 0
	requestdays=0
	campaign = ''
	configurl = ''
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
		elif 'processingString' in raw:
			if '= None' in raw:
				processingString = ''
			else:
				processingString = raw[raw.find("'")+1:]
				processingString = processingString[0:processingString.find("'")]
		elif 'primaryDataset' in raw:
			primaryds = raw[raw.find("'")+1:]
			primaryds = primaryds[0:primaryds.find("'")]
		elif 'retrieveConfigUrl' in raw:
			configurl = raw[raw.find("'")+1:]
			configurl = configurl[0:configurl.find("'")]
		elif 'output.dataTier' in raw:
			outputtier = raw[raw.find("'")+1:]
			outputtier = outputtier[0:outputtier.find("'")]
		elif 'cmsswVersion' in raw:
			cmssw = raw[raw.find("'")+1:]
			cmssw = cmssw[0:cmssw.find("'")]
		elif 'PrepID' in raw:
			prepid = raw[raw.find("'")+1:]
			prepid = prepid[0:prepid.find("'")]
		elif '.splitting.lheInputFiles' in raw:
			a = raw.find(" =")
			b = raw.find('<br')
			if 'True' in raw[a+3:b]:
				lheInputFiles = True
		elif 'lumis_per_job' in raw:
			a = raw.find(" =")
			b = raw.find('<br')
			lumis_per_job = int(raw[a+3:b])
                elif '.schema.Campaign' in raw:
                        campaign = raw[raw.find("'")+1:]
                        campaign = campaign[0:campaign.find("'")]
		elif 'splitting.events_per_lumi' in raw:
			a = raw.find(" =")
			b = raw.find('<br')
			events_per_lumi = int(raw[a+3:b])
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
                                timeev = float(raw[a+1:b])
                        else:
                                a = raw.find(" =")
                                b = raw.find('<br')
                                timeev = float(float(raw[a+3:b]))
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
		elif 't.custodialSites' in raw:
			custodialt1 = '['+raw[raw.find("[")+1:raw.find("]")]+']'	
			custodialt1 = eval(custodialt1)
			if type(custodialt1) == list and len(custodialt1)>0:
				custodialt1 = custodialt1[0]
			else:
				custodialt1 = '?'
		elif 'request.schema.GlobalTag' in raw:
			globaltag = raw[raw.find("'")+1:]
			globaltag = globaltag[0:globaltag.find(":")]

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
	
	if typ in ['MonteCarlo']:
		expectedevents = int(reqevts)
	elif typ in ['MonteCarloFromGEN']:
		if nodbs:
			[inputdataset['events'],inputdataset['status']] = [0,'']
		else:
                    [inputdataset['events'],inputdataset['status'],inputdataset['evperlumi']] = getdsdetail(inputdataset['name'])
                    pass
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
	if type(s)==dict:
		if "exception" in s.keys():
			print "ERROR: %s" % data
			sys.exit(1)
	conn.close()
	ods = s
        if len(ods)==0:
                print "No Outpudatasets for this workflow: "+workflow
	outputdataset = []
	eventsdone = 0
	for o in ods:
		oel = {}
		oel['name'] = o
		if nodbs or 'None-v0' in o:
			[oe,ost,oevperlumi] = [0,'',0]
		else:
                    [oe,ost,oevperlumi] = getdsdetail(o)
                    pass
		oel['events'] = oe
		oel['status'] = ost
		
		eventsdone = eventsdone + oe

	return {'requestname':workflow,'type':typ,'status':status,'campaign':campaign,'expectedevents':expectedevents,'inputdataset':inputdataset,'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,'priority':priority,'sites':sites,'custodialt1':custodialt1,'js':j,'outputdataset':outputdataset,'team':team,'acquisitionEra':acquisitionEra,'requestdays':requestdays,'processingVersion':processingVersion,'events_per_job':events_per_job,'events_per_lumi':events_per_lumi,'lumis_per_job':lumis_per_job,'cmssw':cmssw,'outputtier':outputtier,'prepmemory':prepmemory,'configurl':configurl,'lheInputFiles':lheInputFiles,'filtereff':filtereff,'processingstring':processingString}

def isInDBS(dataset):
    
	print "Check if %s is already in DBS" % dataset
	[oe,ost,oevperlumi] = getdsdetail(dataset)
	if oe>0:
		return True
	else:
		return False

def issmall(r):
	if '/SMS' in r['primaryds']:
		ret = True
	elif r['expectedevents'] > 1000000:
		ret = False
	elif r['priority'] >= 50000:
		ret = True
	elif r['expectedevents'] < 200000:
		ret = True
	else:
		ret = False
	return ret

def main():
	global overview,forceoverview,sum,nodbs,siteblacklist,teams

	campaignconfig = loadcampaignconfig('/afs/cern.ch/user/c/cmst2/mc/config/campaign.cfg')
	overview = getoverview()
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-w', '--workflow', help='workflow name',dest='workflow')
	parser.add_option('--debug', help='add debug info',dest='debug',default=False,action="store_true")
	parser.add_option('-l', '--workflowlist', help='workflow list in textfile',dest='list')
	parser.add_option('--rss', help='set max RSS (kB)',dest='maxRSS')
	parser.add_option('--hoursperjob', help='set job length (hours)',dest='jobh')
	parser.add_option('--softtimeout', help='set SoftTimeout(s)',dest='softtimeout')
	parser.add_option('-t', '--team', help='team (one of %s)' % (",".join(x for x in teams)),dest='team')
	parser.add_option('--priority', help='assign only where priority>=PRIORITY',dest='priority')
	parser.add_option('--test', action="store_true",default=True,help='test mode: don\'treally assign at the end',dest='test')
	parser.add_option('--assign', action="store_false",default=True,help='assign mode',dest='test')
	parser.add_option('--small', action="store_true",default=False,help='assign requests considering them small',dest='small')
	parser.add_option('--hi', action="store_true",default=False,help='heavy ion request (add Vanderbilt to the whitelist, use /store/himc)',dest='hi')
	parser.add_option('--himem', action="store_true",default=False,help='high memory request (use sites allowing 3GB/job, increase maxRSS)',dest='himem')
	parser.add_option('-e','--extension', help='extension (add -ext to the processing string)',dest='ext')
	parser.add_option('-c', '--custodial', help='Custodial',dest='custodialt1')
	parser.add_option('-s', '--sites', help='Single site or comma-separated list (i.e. T1_US_FNAL,T2_FR_CCIN2P3,T2_DE_DESY)',dest='sites')
	parser.add_option('-b', '--blacklist', help='Single site or comma-separated blacklist (i.e. T1_US_FNAL,T2_FR_CCIN2P3,T2_DE_DESY)',dest='blacklist')
	parser.add_option('-a', '--acqera', help='<AcquisitionEra> in <AcquisitionEra>-<ProcString>-v<ProcVer>',dest='acqera',default='')
	parser.add_option('-p', '--processingstring', help='<ProcString> in <AcquisitionEra>-<ProcString>-v<ProcVer>, default=GlobalTag-vX',dest='processingstring')
	parser.add_option('-v', '--processingversion', help='<ProcVer> in <AcquisitionEra>-<ProcString>-v<ProcVer>), default=1',dest='processingversion')
	(options,args) = parser.parse_args()

	list = []
	print
	if options.list:
		list = open(options.list).read().splitlines()
		for i, item in enumerate(list):
			list[i] = item.rstrip()
			list[i] = list[i].lstrip()
		while '' in list:
			list.remove('')
	elif options.workflow:
		list = [options.workflow]
	else:
		print "Please provide at least one workflow to assign!"
		sys.exit(1)

	if options.jobh:
		jobh = int(options.jobh)
	else:
		jobh = 8

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
				print "%s is not a custodial site." % custodialt1
				sys.exit(1)
	else:
		custodialt1 = ''
	if options.sites:
		sites = options.sites
	#else:
	#	if custodialt1 == '':
	#		print "Cannot derive whitelist from empty custodial site. Provide either custodial site or explicit whitelist."
	#		sys.exit(1)
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
		
	reqinfo = {}

	if options.softtimeout:
		softtimeout = options.softtimeout
	else:
		softtimeout = 0

	if options.maxRSS:
		maxRSS = options.maxRSS
	else:
		maxRSS = 0

        acqera = options.acqera

	siteblacklist.sort()
	print "Default site blacklist: %s\n" % (",".join(x for x in siteblacklist))
	if options.blacklist:
		newblacklist = options.blacklist.split(',')
		for i in newblacklist:
			if i not in siteblacklist:
				siteblacklist.append(i)
		print "New site blacklist: %s\n" % (",".join(x for x in siteblacklist))
	print "T1s: %s" % (t1s.keys())
		
	if options.himem:
		print "High memory flag is set, parameters will be configured accordingly\n"

	print "Preparing requests:\n"
	assign_data = {}
	datasets = {}
	prepids = []
	pds = []
	linkedsites = {}
	for w in list:
		if options.debug:
			print "Get info for %s" % w
		reqinfo[w] = getWorkflowInfo(w)
		if options.debug:
			print "Done"
	
		if reqinfo[w]['prepid'] in prepids:
			print "%s is duplicated" % reqinfo[w]['prepid']
			sys.exit(1)
		else:
			prepids.append(reqinfo[w]['prepid'])
		if reqinfo[w]['primaryds'] in pds:
			print "%s is duplicated" % reqinfo[w]['primaryds']
			sys.exit(1)
		else:
			pds.append(reqinfo[w]['primaryds'])

		if reqinfo[w]['campaign'] not in campaignconfig.keys():
			print "\nUnknown campaign %s for %s, using defaults\n" % (reqinfo[w]['campaign'],w)

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
			if not options.test:
				sys.exit(1)
	
		# type
		if not 'MonteCarlo' in reqinfo[w]['type']:
			print "%s: not a MonteCarlo/MonteCarloFromGEN request!" % w
			sys.exit(1)

		# priority
		priority = reqinfo[w]['priority']
		
		# internal assignment parameters
		prepmemory = reqinfo[w]['prepmemory']
		himem = False
		blockclosemaxevents = 2000000
		minmergesize = 2147483648
		maxmergeevents = 50000
		noncustodialsites = []
		custodialsubtype = "Move"
		autoapprovesubscriptionsites = []
		if reqinfo[w]['type'] == 'MonteCarlo' and reqinfo[w]['outputtier'] == 'GEN':
			autoapprovesubscriptionsites = autoapprovelist
			if 'T1_US_FNAL' not in autoapprovesubscriptionsites:
				# patch: FNAL is down
				autoapprovesubscriptionsites.append('T1_US_FNAL')
			#noncustodialsites = gensubscriptionsites
			custodialsubtype = "Replica"
			blockclosemaxevents = 2000000
			if '_STEP0ATCERN' in w:
				# STEP0 @ CERN
				print '%s (%s, MonteCarlo step0 GEN)' % (w,reqinfo[w]['campaign'])
				if team == 'auto':
					team = 'production'
				if maxRSS == 0:
					maxRSS = 3000000
				if softtimeout == 0:
					softtimeout = 129600
				maxmergeevents = 4000000
			else:
				# full GEN
				print '%s (%s, MonteCarlo full GEN)' % (w,reqinfo[w]['campaign'])
				if team == 'auto':
					#team = 'mc_highprio'
					team = 'production'
				if maxRSS == 0:
					maxRSS = 2294967
				if softtimeout == 0:
					softtimeout = 129600
				maxmergeevents = 200000
				
		elif options.himem or reqinfo[w]['prepmemory']>2300:
			himem = True
			print '%s (%s High Memory)' % (w,reqinfo[w]['campaign'])
			if team == 'auto':
				team = 'production'
			softtimeout = 159600
			minmergesize = 2147483648
			if maxRSS == 0:
				maxRSS = 3500000
		else:
			print '%s (%s %s)' % (w,reqinfo[w]['campaign'],reqinfo[w]['type'])
			if team == 'auto':
				team = 'production'
			minmergesize = 2147483648
			if maxRSS == 0:
				maxRSS = 2294967
			if softtimeout == 0:
				softtimeout = 159600

		oldcustodials = getCustodialFromClones(getRequestsByPREPID(reqinfo[w]['prepid']))
		if oldcustodials:
			print "\t%s has resubmissions custodial at %s" % (w,",".join(x for x in oldcustodials))
		if custodialt1 == '':
			if options.himem:
				custodialt1 = 'T1_FR_CCIN2P3'

		# LFN path
		if reqinfo[w]['type'] == 'MonteCarlo' and reqinfo[w]['outputtier'] == 'GEN':
			mergedlfnbase = '/store/generator'
		elif reqinfo[w]['campaign'] in campaignconfig.keys() and 'tfpath' in campaignconfig[reqinfo[w]['campaign']].keys():
			mergedlfnbase = "/store/%s" % campaignconfig[reqinfo[w]['campaign']]['tfpath']
		else:
			mergedlfnbase = '/store/mc'

		if sites == 'auto':
			newsitelist = []
			if reqinfo[w]['type'] == 'MonteCarlo' and reqinfo[w]['outputtier'] == 'GEN' and '_STEP0ATCERN' in w:
				newsitelist = ['T2_CH_CERN']
			else:
				newsitelist = getwhitelist()
				if custodialt1 != '' and custodialt1 not in newsitelist:
					newsitelist.append(custodialt1)
				if mergedlfnbase == '/store/himc':
					for rem in ['T1_UK_RAL','T1_IT_CNAF','T1_ES_PIC','T1_TW_ASGC','T1_DE_KIT','T1_RU_JINR','T1_US_FNAL']:
						if rem in newsitelist:
							newsitelist.remove(rem)
				else:
					if 'T2_US_Vanderbilt' in newsitelist:
						newsitelist.remove('T2_US_Vanderbilt')
		else:
			# explicit sitelist, no guesses
			if sites:
				newsitelist = sites.split(',')
				#for i in newsitelist:
				#	if not i in linkedsites[custodialt1] and i != custodialt1:
				#		print "%s has no PhEDEx uplink to %s" % (i,custodialt1)
				#		sys.exit(1)
			else:
				newsitelist = []

		if options.himem or reqinfo[w]['prepmemory']>2300:
			print "Memory requirement is %s, restricting to %s" % (reqinfo[w]['prepmemory'],",".join(x for x in sitelisthimemrequests))
			oldsitelist = newsitelist[:]
			newsitelist = []
			for i in oldsitelist:
				if i in sitelisthimemrequests:
					newsitelist.append(i)

		elif options.small or issmall(reqinfo[w]):
			oldsitelist = newsitelist[:]
			newsitelist = []
			for i in oldsitelist:
				#if 'T1_' in i:
				#	newsitelist.append(i)
				if i in sitelistsmallrequests:
					newsitelist.append(i)

		if newsitelist==[]:
			newsitelist = getwhitelist()

		# T2_CH_CERN, T2_CH_CERN_HLT, T2_CH_CERN_T0, T2_CH_CERN_AI
		if 'T2_CH_CERN' in newsitelist:
			#newsitelist.extend(['T2_CH_CERN_HLT','T2_CH_CERN_T0'])
			#newsitelist.extend(['T2_CH_CERN_HLT'])
			pass

		# set splitting parameters
                if reqinfo[w]['type'] == 'MonteCarloFrom' and reqinfo[w]['lheInputFiles'] == False:
                    if reqinfo[w]['campaign'] in campaignconfig.keys() and 'events_per_lumi' in campaignconfig[reqinfo[w]['campaign']].keys():
                        base_events_per_lumi = campaignconfig[reqinfo[w]['campaign']]['events_per_lumi']
                        params = {"requestName":w,"splittingTask" : '/'+w+"/Production", "splittingAlgo":"EventBased", "events_per_lumi":events_per_lumi}
                        setParam(url,w,params,options.debug)


		# processing string and processing version
		if acqera != 'auto':
			newacqera = acqera
		elif reqinfo[w]['campaign'] in campaignconfig.keys() and 'acqera' in campaignconfig[reqinfo[w]['campaign']]:
			newacqera = campaignconfig[reqinfo[w]['campaign']]['acqera']
		else:
			newacqera = reqinfo[w]['campaign']

		if processingstring != 'auto':
			newprocessingstring = "%s_%s" % (reqinfo[w]['globaltag'],processingstring)
		else:
			newprocessingstring = reqinfo[w]['processingstring']
		
		if options.ext:
			newprocessingstring = "%s_ext%s" % (newprocessingstring,options.ext)

		#print "Processing String = %s" % newprocessingstring
				
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

		newsitelist.sort()
		noncustodialsites.sort()
		autoapprovesubscriptionsites.sort()
		# save the parameters for assignment of request 'w'
		assign_data[w] = {}
		assign_data[w]['team'] = team
		assign_data[w]['priority'] = priority
		assign_data[w]['events'] = reqinfo[w]['expectedevents']
		assign_data[w]['filtereff'] = reqinfo[w]['filtereff']
		assign_data[w]['timeev'] = reqinfo[w]['timeev']
		try:
			assign_data[w]['lumis_per_job'] = lumis_per_job
		except:
			assign_data[w]['lumis_per_job'] = 0
		try:
			assign_data[w]['events_per_job'] = events_per_job
		except:
			assign_data[w]['events_per_job'] = 0
		try:
			assign_data[w]['events_per_lumi'] = events_per_lumi
		except:
			assign_data[w]['events_per_lumi'] = 0
		try:
			assign_data[w]['max_events_per_lumi'] = max_events_per_lumi
		except:
			assign_data[w]['max_events_per_lumi'] = 0
		assign_data[w]['whitelist'] = newsitelist
		assign_data[w]['acqera'] = newacqera
		assign_data[w]['processingstring'] = newprocessingstring
		assign_data[w]['processingversion'] = processingversion
		if custodialt1 != '':
			assign_data[w]['custodialsites'] = [custodialt1]
		else:
			assign_data[w]['custodialsites'] = []
		assign_data[w]['noncustodialsites'] = noncustodialsites
		assign_data[w]['custodialsubtype'] = custodialsubtype
		assign_data[w]['autoapprovesubscriptionsites'] = autoapprovesubscriptionsites
		assign_data[w]['dataset'] = dataset
		assign_data[w]['mergedlfnbase'] = mergedlfnbase
		assign_data[w]['prepmemory'] = prepmemory
		assign_data[w]['maxRSS'] = maxRSS
		assign_data[w]['minmergesize'] = minmergesize
		assign_data[w]['softtimeout'] = softtimeout
		assign_data[w]['blockclosemaxevents'] = blockclosemaxevents
		assign_data[w]['maxmergeevents'] = maxmergeevents

	print "\n----------------------------------------------\n"

	print "Command line options:\n"
	for ky in options.__dict__.keys():
		if options.__dict__[ky]:
			print "\t%s:\t%s" % (ky,options.__dict__[ky])
	print

	if not options.test: 
		print "Assignment:"
		print

	assign_counter = 0
	for w in list:
		suminfo = "%s\n\tcampaign: %s prio:%s events:%s\n\ttimeev:%s events_per_job:%s events_per_lumi:%s lumis_per_job:%s max_events_per_lumi:%s filtereff:%s\n\tteam:%s era:%s procstr: %s procvs:%s\n\tLFNBase:%s PREPmem: %s maxRSS:%s MinMerge:%s SoftTimeout:%s BlockCloseMaxEvents:%s MaxMergeEvents:%s\n\tOutputDataset: %s\n\tCustodialSites: %s\n\tNonCustodialSites: %s\n\tCustodialSubType: %s\n\tAutoApprove: %s\n\tWhitelist: %s" % (w,reqinfo[w]['campaign'],assign_data[w]['priority'],assign_data[w]['events'],assign_data[w]['timeev'],assign_data[w]['events_per_job'],assign_data[w]['events_per_lumi'],assign_data[w]['lumis_per_job'],assign_data[w]['max_events_per_lumi'],assign_data[w]['filtereff'],assign_data[w]['team'],assign_data[w]['acqera'],assign_data[w]['processingstring'],assign_data[w]['processingversion'],assign_data[w]['mergedlfnbase'],assign_data[w]['prepmemory'],assign_data[w]['maxRSS'],assign_data[w]['minmergesize'],assign_data[w]['softtimeout'],assign_data[w]['blockclosemaxevents'],assign_data[w]['maxmergeevents'],assign_data[w]['dataset'],assign_data[w]['custodialsites'],assign_data[w]['noncustodialsites'],assign_data[w]['custodialsubtype'],assign_data[w]['autoapprovesubscriptionsites'],",".join(x for x in assign_data[w]['whitelist']))
		if options.test:
			print "TEST:\t%s\n" % suminfo
			if options.priority:
				if int(assign_data[w]['priority']) >= int(options.priority):
					print "%s will be assigned" % w
				else:
					print "%s will NOT be assigned" % w
		if not options.test:
			if options.priority:
				if int(assign_data[w]['priority']) < int(options.priority):
					continue
			print "ASSIGN:\t%s\n" % suminfo
			assign_counter = assign_counter + 1
			if assign_counter == 5:
				#print "Wait please..."
				print "..."
				time.sleep(30)
				assign_counter = 0
			else:
				time.sleep(10)
			assignMCRequest(url,w,assign_data[w]['team'],
                                        assign_data[w]['whitelist'],
                                        assign_data[w]['acqera'],
                                        assign_data[w]['processingstring'],
                                        assign_data[w]['processingversion'],
                                        assign_data[w]['mergedlfnbase'],
                                        assign_data[w]['minmergesize'],
                                        assign_data[w]['maxRSS'],
                                        assign_data[w]['custodialsites'],
                                        assign_data[w]['noncustodialsites'],
                                        assign_data[w]['custodialsubtype'],
                                        assign_data[w]['autoapprovesubscriptionsites'],
                                        assign_data[w]['softtimeout'],
                                        assign_data[w]['blockclosemaxevents'],
                                        assign_data[w]['maxmergeevents'])
	
	sys.exit(0)

if __name__ == "__main__":
	main()
