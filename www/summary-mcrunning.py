#!/usr/bin/env python -w
import sys,urllib,urllib2,re,time,os
import simplejson as json
import optparse
import httplib
import datetime
import shutil

team2type = { 'mc' : ['MonteCarlo','MonteCarloFromGEN'], 't1' : ['ReDigi','ReReco'] ,'all' :['MonteCarlo','MonteCarloFromGEN','ReDigi','ReReco'] }
reqmgrsocket='vocms204.cern.ch'
dashost = 'https://cmsweb.cern.ch'
cachedasage=60
outputfile = '/afs/cern.ch/user/s/spinoso/www/mcrunning.html'
tmpoutputfile = outputfile + '.tmp'
setclosedfile = '/afs/cern.ch/user/s/spinoso/www/setclosed.txt'
setclosedhtml = 'https://spinoso.web.cern.ch/spinoso/setclosed.txt'
tmpsetclosedfile = setclosedfile + '.tmp'
overview = ''
count = 1

def getPrepID(workflow):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/view/showWorkload?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	conn.close()
	list = data.split('\n')
	for raw in list:
		if 'PrepID' in raw:
			break
	prepid = raw[raw.find("'")+1:]
	prepid = prepid[0:prepid.find("'")]
	#prepid = eval(prepid)
	return prepid

def getWhiteList(workflow):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/view/showWorkload?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	conn.close()
	list = data.split('\n')
	for white in list:
		if 'white' in white and not '[]' in white:
			break
	sites = '['+white[white.find("[")+1:white.find("]")]+']'
	sites = eval(sites)
	return sites

def getCustodialTier(workflow):
	s = None
	for i in getWhiteList(workflow):
		if 'T1_' in i:
			s = i
			break
	s = shortT1Name(s)
	return s

def shortT1Name(s):
	if not s:
		return ''
	t1list = {'T1_FR_CCIN2P3':'IN2P3','T1_TW_ASGC':'ASGC','T1_IT_CNAF':'CNAF','T1_US_FNAL':'FNAL','T1_DE_KIT':'KIT','T1_ES_PIC':'PIC','T1_UK_RAL':'RAL'}
	for i in t1list.keys():
		if i in s:
			return t1list[i]
	return s

def getoverview():
	global overview
	return overview

def getnewoverview():
        c = 0
        s = ''
        while c < 5:
                conn  =  httplib.HTTPSConnection(reqmgrsocket, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                r1=conn.request("GET",'/reqmgr/monitorSvc/requestmonitor')
                r2=conn.getresponse()
                print r2.status, r2.reason
                if r2.status == 500: 
                        c = c + 1
                        print "retrying"
                else:   
                        c = 100 
                        s = json.loads(r2.read())
                        conn.close()
        return s

def getRequestsByPREPID(prepid):
	s = getoverview()
	r = []
	for i in s:
		if prepid in i['request_name']:
			r.append(i['request_name'])
	return r
	
def getRequestsByTeamStatus(team,status):
	typelist = team2type[team]
	s = getoverview()
	r = []
	for i in s:
		t = ''
		if 'type' in i.keys():
			t = i['type']
		if 'status' in i.keys():
			st = i['status']
		if t in typelist and st in status:
			r.append(i['request_name'])
	return r
	
def getPhEDExRequestInfo(datasetName):
	info = {}
        url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/RequestList?dataset=' + datasetName
	try:
        	result = json.load(urllib.urlopen(url))
	except:
		print "Cannot get subscription status from PhEDEx"
		return None

	try:
		r = result['phedex']['request']
	except:
		return None
	for i in range(0,len(r)):
        	approval = r[i]['approval']
        	requested_by = r[i]['requested_by']
		custodialsite = r[i]['node'][0]['name']
		id = r[i]['id']
		if 'T1_' in custodialsite:
			info['custodialsite'] = custodialsite
			info['requested_by'] = requested_by
			info['approval'] = approval
			info['id'] = id
			return info
	return None
			
def getPhEDExTransferInfo(datasetName):
	info = {}
	url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions?dataset=' + datasetName
	try:
        	result = json.load(urllib.urlopen(url))
	except:
		print "Cannot get transfer status from PhEDEx"
		return None
	try:
		r = result['phedex']['dataset'][0]['subscription']
	except:
		return None
	for i in  r:
		node = i['node']
		custodial = i['custodial']
		if 'T1_' in node and custodial == 'y': 
			if i['move'] == 'n':
				type = 'Replica'
			else:
				type = 'Move'
			info['node'] = node
			info['perc'] = int(float(i['percent_bytes']))
			info['type'] = type
			return info
	return None

def das_get_data(query):
    params  = {'input':query, 'idx':0, 'limit':0}
    path    = '/das/cache'
    pat     = re.compile('http[s]{0,1}://')
    if  not pat.match(dashost):
        msg = 'Invalid hostname: %s' % dashost
        raise Exception(msg)
    url = dashost + path
    headers = {"Accept": "application/json"}
    encoded_data = urllib.urlencode(params, doseq=True)
    url += '?%s' % encoded_data
    req  = urllib2.Request(url=url, headers=headers)
    opener = urllib2.build_opener()
    fdesc = opener.open(req)
    data = fdesc.read()
    fdesc.close()

    pat = re.compile(r'^[a-z0-9]{32}')
    if  data and isinstance(data, str) and pat.match(data) and len(data) == 32:
        pid = data
    else:
        pid = None
    count = 1  
    timeout = 30 
    while pid:
        params.update({'pid':data})
        encoded_data = urllib.urlencode(params, doseq=True)
        url  = dashost + path + '?%s' % encoded_data
        req  = urllib2.Request(url=url, headers=headers)
        try:
            fdesc = opener.open(req)
            data = fdesc.read()
            fdesc.close()
        except urllib2.HTTPError:
            print str(urllib2.HTTPError)
            return ""
        if  data and isinstance(data, str) and pat.match(data) and len(data) == 32:
            pid = data
        else:
            pid = None
        time.sleep(count)
        if  count < timeout:
            count *= 1
        else:
            count = timeout
    d = eval(data)
    try:
    	r = d['data'][0]['dataset']
    	if isinstance(r,list):
		return [r[0]['nevents'],r[0]['status']]
    	else:
		return [r['nevents'],r['status']]
    except:
	return [-1,'']

def getFilterEfficiency(workflow):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	s = json.loads(data)
	conn.close()
	return float(s['FilterEfficiency'])

def getRequestByrequestName(workflow):
	#try:
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	s = json.loads(data)
	conn.close()
	#except:
	#	print "Cannot get request (getRequestByrequestName) " 
	#	sys.exit(1)
	return s

def statusWorkflow(jreq,workflow):
	try:
		t = jreq['RequestStatus']
	except:
		print "Cannot get status for %s" % workflow
		sys.exit(1)
        return t

def typeWorkflow(jreq,workflow):
	try:
		t = jreq['RequestType']
	except:
		print "Cannot get type for %s" % workflow
		sys.exit(1)
        return t

def RequestSizeEvents(jreq,workflow):
	try:
		reqevts = jreq['RequestSizeEvents']
	except:
		print "No RequestSizeEvents for this workflow: "+workflow
		return ''
        return reqevts

def inputdatasetWorkflow(jreq,workflow):
	try:
		dataset = jreq['InputDatasets']
	except:
		print "No Inpudatasets for this workflow: "+workflow
		return ''
        return dataset[0]

def outputEventsWorkflow(workflow):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/view/showWorkload?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	conn.close()
	list = data.split('\n')
	datasets = s
        if len(datasets)==0:
                print "No Outpudatasets for this workflow: "+workflow
        return datasets

def outputdatasetsWorkflow(workflow):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	s = json.loads(data)
	conn.close()
	datasets = s
        if len(datasets)==0:
                print "No Outpudatasets for this workflow: "+workflow
        return datasets

def getdsdetail(dataset):
	global cachedasage
	query = 'dataset dataset=' + dataset + ' status=*|grep dataset.nevents,dataset.status'

	[e,st] = das_get_data(query)
	if e == -1:
		return [0,'']
	else:
		return [e,st]

def getjobsummary(workflow):
	s = getoverview()
	j = {}
	k = {'success':'success','failure':'failure','Pending':'pending','Running':'running','cooloff':'cooloff','pending':'queued','inWMBS':'inWMBS','total_jobs':'total_jobs'}
	for r in s:
		if r['request_name'] == workflow:
			break
	if r:
		for k1 in k.keys():
			k2 = k[k1]
			if k1 in r.keys():
				j[k2] = r[k1]
			else:
				j[k2] = 0
	else:
		print " getjobsummary error: No such request: %s" % workflow
		sys.exit(1)
	return j
def htmlhd():
	s = "<html><head><title>MC Running Summary</title></head><body style=\'font-family:sans-serif;\'><table border=1 style=\'border-width:1px;border-spacing:0px;border-collapse:collapse;font-size:14px;\'>"
	s +=  "<tr><td>#</td><td>OutputDataset (Custodial T1)</td><td>OEvts(OEvts%)</td>"
	s += "<td>WMArequest</td><td>JobStatus</td><td>Action</td><td>PrepID</td></tr>"
	return s

def htmlft():
	return "</table><hr><i>End update: %s</i></body></table>" % datetime.datetime.utcnow()

def htmlrw(action,workflow,type,status,prepid,custodial,dbsstatus,expectedevents,datasetName,oe,perc,phreqinfo,js):

	if 'CHECK' in action:
		color = '#FFCCCC'
	elif action == 'SUBSCRIBE':
		color = '#CCFFFF'
	elif action == 'CLOSE':
		color = '#CCFFCC'
	else:
		color = '#FFFFFF'
		
	s = "<tr bgcolor=%s><td>%s</td><td>%s<br/>(%s)</td><td>%s(%s%%)<br/>(expect %s)</td>" % (color,count,datasetName,custodial,oe,perc,expectedevents)
	s += "<td>%s<br/>(%s) is %s(<a target='_blank' href='https://cmsweb.cern.ch/reqmgr/view/details/%s'>req</a>,<a target='_blank' href='https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s'>sum</a>)</td>" % (workflow,type,status,workflow,workflow)
	s += "<td>Q:%s P:%s R:%s <br/>S:%s F:%s</td>" % (js['queued'],js['pending'],js['running'],js['success'],js['failure'])
	s += "<td>%s</td>" % action
	s += "<td><a href='http://cms.cern.ch/iCMS/prep/requestmanagement?code=%s' target='_blank'>%s</a></td>" % (prepid,prepid)
	s += "</tr>"
	return s

def printrw(workflow,type,status,js,prepid,custodial,ids,ie,ist,expectedevents,datasetName,oe,ost,perc):
	print "Output Dataset: %s Events: %s DBSStatus: %s Progress: %s%%" % (datasetName,oe,ost,perc)
	print "Expected events: %s " % expectedevents
	print "PrepID: %s" % prepid
	print "Workflow: %s" % workflow
	print "Type: %s" % type
	print "Status: %s" % status
	print "Job summary: %s" % js
	print "Custodial T1: %s" % custodial
	if type in ['MonteCarloFromGEN','ReReco','ReDigi']:
		print "Input Dataset: %s Events: %s DBSStatus: %s" % (ids,ie,ist)

def main():
	global overview,count
	overview = getnewoverview()
	#status = ['acquired','running','completed']
	status = ['running','completed']
	team = 'mc'
	list = getRequestsByTeamStatus(team,status)
	list.sort()
	print "Number of requests: %s" % len(list)

	print "Generating %s" % tmpoutputfile
	output = open(tmpoutputfile, 'w')
	output.write('Requests that can be moved to "closed-out": <a target="_blank" href="https://spinoso.web.cern.ch/spinoso/setclosed.txt">setclosed.txt</a><br/>')
	output.write("<i>Start update: %s [UTC]</i>" % str(datetime.datetime.utcnow()))
	output.write('<br/><br/>')
	output.write(htmlhd())
	output.close
	try:
		os.remove(tmpsetclosedfile)
	except:
		pass

	#list = ['spinoso_EXO-Summer11-01178_rq959_00_T1_CNAF_LHE_120222_210755']
	for workflow in list:
		print count,workflow
		jreq = getRequestByrequestName(workflow)
		type = typeWorkflow(jreq,workflow)
		status = statusWorkflow(jreq,workflow)
		ods = outputdatasetsWorkflow(workflow)
		ods_close = []
		prepid = getPrepID(workflow)
		custodial =  getCustodialTier(workflow)
		js = getjobsummary(workflow)
		
		ids = ''
		ie = -1
		ist = ''
		expectedevents = -1
		if type in ['MonteCarloFromGEN','ReReco','ReDigi']:
			ids = inputdatasetWorkflow(jreq,workflow)
			[ie,ist] = getdsdetail(ids)
			if type in ['MonteCarloFromGEN','ReReco']:
				filterefficiency = getFilterEfficiency(workflow)
				expectedevents = int(filterefficiency * ie)
			else:
				expectedevents = ie
		elif type in ['MonteCarlo']:
			ie = RequestSizeEvents(jreq,workflow)
			expectedevents = ie
		else:
			pass
        	for datasetName in ods:
			[oe,ost] = getdsdetail(datasetName)
			phreqinfo = getPhEDExRequestInfo(datasetName)
			try:
				req = phreqinfo['id']
			except:
				req = ''
			
			if expectedevents > 0:
				perc = 100*oe/expectedevents
			else:
				perc = 0
			if status == 'completed' and perc < 95:
				action = 'CHECK_FEWEVTS'
			elif perc > 80 and not req:
				action = 'SUBSCRIBE'
			elif perc > 95:
				action = 'CLOSE'
				ods_close.append(datasetName)
			elif perc == 0 and (js['success']+js['failure']>0):
				action = 'CHECK_NOEVTS'
			else:
				action = ''
			
			output = open(tmpoutputfile, 'a')
			output.write(htmlrw(action,workflow,type,status,prepid,custodial,ost,expectedevents,datasetName,oe,perc,phreqinfo,js))
			output.close

		setclosed = 1
		for ds in ods:
			if not ds in ods_close:
				setclosed = 0
				break
				
		if setclosed == 1:
			f = open(tmpsetclosedfile,'a')
			f.write(workflow+'\n')
			f.close
		count = count + 1
	
	output = open(tmpoutputfile, 'a')
	output.write(htmlft())
	output.close
	
	shutil.move(tmpoutputfile,outputfile)
	try:
		shutil.move(tmpsetclosedfile,setclosedfile)
	except:
		pass
	
	print "END_OF_SCRIPT"
        sys.exit(0)

if __name__ == "__main__":
        main()
