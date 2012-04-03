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
outputfile = '/afs/cern.ch/user/s/spinoso/www/mcclosedout.html'
tmpoutputfile = outputfile + '.tmp'
setannouncedfile = '/afs/cern.ch/user/s/spinoso/www/setannounced.txt'
setannouncedhtml = 'https://spinoso.web.cern.ch/spinoso/setannounced.txt'
tmpsetannouncedfile = setannouncedfile + '.tmp'
setvalidfile = '/afs/cern.ch/user/s/spinoso/www/setvalid.txt'
setvalidhtml = 'https://spinoso.web.cern.ch/spinoso/setvalid.txt'
tmpsetvalidfile = setvalidfile + '.tmp'
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
	while c < 2:
		try:
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
		except :
			print "Cannot get request (getoverview) " 
			sys.exit(1)
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

def dbs_get_data(dataset):
    output=os.popen("/afs/cern.ch/user/s/spinoso/public/dbssql --input='find sum(block.numevents),dataset.status where dataset="+dataset+"'"+ "|grep '[0-9]\{1,\}'").read()
    ret = output.split(' ')
    ret[0] = int(ret[0])
    ret[1] = ret[1].rstrip()
    return ret

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

def RequestNumEvents(jreq,workflow):
	try:
		reqevts = jreq['RequestSizeEvents']
	except:
		try:
			reqevts = jreq['RequestNumEvents']
		except:
			print "No RequestNumEvents for this workflow: "+workflow
			return ''
        return int(reqevts)

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

	[e,st] = dbs_get_data(dataset)
	if e == -1:
		return [0,'']
	else:
		return [e,st]

def htmlhd():
	s = "<html><head><title>MC Closed-out Summary</title></head><body style=\'font-family:sans-serif;\'><table border=1 style=\'border-width:1px;border-spacing:0px;border-collapse:collapse;font-size:14px;\'>"
	s +=  "<tr><td>#</td><td>OutputDataset (Custodial T1)</td><td>OEvts(OEvts%)</td><td>Expect</td><td>DBSStatus</td>"
	s += "<td>WMArequest</td><td>Action</td><td>PrepID</td><td>WMAtype</td><td>PhEDExRqst</td><td>PhEDExTransf</td></tr>"
	return s

def htmlft():
	return "</table><hr><i>End update: %s</i></body></table>" % datetime.datetime.utcnow()

def htmlrw(action,workflow,type,status,prepid,custodial,dbsstatus,expectedevents,datasetName,oe,perc,phreqinfo,phtrinfo):

	if 'CHECK' in action:
		color = '#FFCCCC'
	elif action == 'SET_VALID':
		color = '#FFFFCC'
	elif action == 'ON_TRANSFER':
		color = '#F5DA81'
	elif action == 'SET_ANN':
		color = '#CCFFCC'
	else:
		color = '#FFFFFF'
		
	s = "<tr bgcolor=%s><td>%s</td><td>%s<br/>(%s)</td><td>%s(%s%%)</td><td>%s</td>" % (color,count,datasetName,custodial,oe,perc,expectedevents)
	s += "<td>%s</td>" % dbsstatus
	s += "<td>%s<br/>(<a target='_blank' href='https://cmsweb.cern.ch/reqmgr/view/details/%s'>req</a>,<a target='_blank' href='https://cmsweb.cern.ch/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/%s'>sum</a>)</td>" % (workflow,workflow,workflow)
	s += "<td>%s</td>" % action
	s += "<td><a href='http://cms.cern.ch/iCMS/prep/requestmanagement?code=%s' target='_blank'>%s</a></td>" % (prepid,prepid)
	s += "<td>%s</td>" % type
	if phreqinfo:
		s += "<td><a href='https://cmsweb.cern.ch/phedex/prod/Request::View?request=%s' target='_blank'>%s</a></td>" % (phreqinfo['id'],phreqinfo['id'])
	else:
		s += "<td>???</td>"
	if phtrinfo:
		s += "<td>%s%%(%s)</td>" % (phtrinfo['perc'],shortT1Name(phtrinfo['node']))
	else:
		s += "<td>???</td>"
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
	status = ['closed-out']
	team = 'mc'
	#print "Considering requests for team %s in status %s" % (team,status)
	list = getRequestsByTeamStatus(team,status)
	list.sort()
	print "Number of requests: %s" % len(list)

	print "Generating %s" % tmpoutputfile
	output = open(tmpoutputfile, 'w')
	output.write('Datasets in PRODUCTION and to be set as VALID: <a target="_blank" href="https://spinoso.web.cern.ch/spinoso/setvalid.txt">setvalid.txt</a><br/>')
	output.write('Requests to be moved from "closed-out" to "announced": <a target="_blank" href="https://spinoso.web.cern.ch/spinoso/setannounced.txt">setannounced.txt</a><br/>')
	output.write("<i>Start update: %s [UTC]</i>" % str(datetime.datetime.utcnow()))
	output.write('<br/><br/>')
	output.write(htmlhd())
	output.close
	try:
		os.remove(tmpsetvalidfile)
		f = open(tmpsetvalidfile,'w')
		f.write('')
		f.close
	except:
		pass
	try:
		os.remove(tmpsetannouncedfile)
		f = open(tmpsetannouncedfile,'w')
		f.write('')
		f.close
	except:
		pass

	#list = ['spinoso_EXO-Summer11-01178_rq959_00_T1_CNAF_LHE_120222_210755']
	if list == []:
		output.write('<b>There are <u>no requests</u> in "closed-out" status!</b><br/><br/>')
		#sys.exit(0)
	else:
		for workflow in list:
			print count,workflow
			jreq = getRequestByrequestName(workflow)
			type = typeWorkflow(jreq,workflow)
			status = statusWorkflow(jreq,workflow)
			ods = outputdatasetsWorkflow(workflow)
			ods_valid = []
			prepid = getPrepID(workflow)
			custodial =  getCustodialTier(workflow)
		
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
				ie = RequestNumEvents(jreq,workflow)
				expectedevents = ie
			else:
				pass
	       	 	for datasetName in ods:
				[oe,ost] = getdsdetail(datasetName)
				phreqinfo = getPhEDExRequestInfo(datasetName)
				phtrinfo = getPhEDExTransferInfo(datasetName)
			
				if expectedevents > 0:
					perc = 100*oe/expectedevents
				else:
					perc = 0
				if perc > 101:
					action = 'CHECK_DBS'
				elif phtrinfo == None or phreqinfo == None:
					action = 'CHECK_TR'
				elif phtrinfo['perc'] == 100 and ost == 'PRODUCTION':
					action = 'SET_VALID'
				elif phtrinfo['perc'] == 100 and ost == 'VALID':
					action = 'SET_ANN'
					ods_valid.append(datasetName)
				else:
					action = '?CHECK'
			
				if action == 'SET_VALID':
					f = open(tmpsetvalidfile, 'a')
					f.write(datasetName+"\n")
					f.close
			
				output = open(tmpoutputfile, 'a')
				output.write(htmlrw(action,workflow,type,status,prepid,custodial,ost,expectedevents,datasetName,oe,perc,phreqinfo,phtrinfo))
				output.close

			setannounced = 1
			for ds in ods:
				if not ds in ods_valid:
					setannounced = 0
					break
				
			if setannounced == 1:
				f = open(tmpsetannouncedfile,'a')
				f.write(workflow+'\n')
				f.close
			count = count + 1
	
		output = open(tmpoutputfile, 'a')
		output.write(htmlft())
		output.close
	
	try:
		shutil.move(tmpoutputfile,outputfile)
	except:
		pass
	try:
		os.remove(setvalidfile)
	except:
		pass
	try:
		os.remove(setannouncedfile)
	except:
		pass
	try:
		shutil.move(tmpsetvalidfile,setvalidfile)
	except:
		pass
	try:
		shutil.move(tmpsetannouncedfile,setannouncedfile)
	except:
		pass
	
	print "END_OF_SCRIPT"
        sys.exit(0)

if __name__ == "__main__":
        main()
