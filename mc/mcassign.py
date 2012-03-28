#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os
import optparse
try:
    import json
except ImportError:
    import simplejson as json

# TODO suggest TeamList 
# TODO automatic acqera
# TODO guess procversion

legal_eras = ['Summer11','Summer12']
zones = ['FNAL','CNAF','ASGC','IN2P3','RAL','PIC','KIT']
zone2t1 = {'FNAL':'T1_US_FNAL','CNAF':'T1_IT_CNAF','ASGC':'T1_TW_ASGC','IN2P3':'T1_FR_CCIN2P3','RAL':'T1_UK_RAL','PIC':'T1_ES_PIC','KIT':'T1_DE_KIT'}
siteblacklist = ['T2_AT_Vienna','T2_BR_UERJ','T2_FI_HIP','T2_FR_GRIF_IRFU','T2_KR_KNU','T2_PK_NCP','T2_PT_LIP_Lisbon','T2_RU_IHEP','T2_RU_ITEP','T2_RU_RRC_KI','T2_TR_METU','T2_UK_SGrid_Bristol','T2_US_Vanderbilt','T2_CH_CERN']

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
		print "Zone: %s" % zone
		print "Custodial T1 is %s" % zone2t1[zone]
		t2list = get_linkedt2s(zone2t1[zone])
		for i in t2list:
			if not i in siteblacklist:
				sitelist.append(i)
		return sitelist
	else:
		sitelist = zone.split(',')
		t1count = 0
		for i in sitelist:
			if 'T1_' in i:
				custodialT1 = i
				t1count = t1count + 1
		if t1count > 1:
			print "More than 1 T1 has been specified in %s" % (sitelist)
			sys.exit(1)
		t2list = get_linkedt2s(custodialT1)
		if t1count == 1:
			for i in sitelist:
				if 'T2_' in i:
					if not i in t2list:
						print "%s has no PhEDEx uplink to %s" % (i,custodialT1)
						sys.exit(1)
		return sitelist

def getacqera(r):
	global legal_eras
        prepid = r['prepid']
        era = prepid.split('-')[1]
	if era in legal_eras:
		return era
	else:
		print "'%s' is not a known era, use one of %s" % (era,legal_eras)
		sys.exit(1)

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
	      "maxRSS": 4294967296,
              "maxVSize": 4294967296,
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
		if 'PrepID' in raw:
			prepid = raw[raw.find("'")+1:]
			prepid = prepid[0:prepid.find("'")]
		elif 'request.priority' in raw:
			a = raw.find("'")
			if a >= 0:
				b = raw.find("'",a+1)
				priority = int(raw[a+1:b])
			else:
				a = raw.find(" =")
				b = raw.find('<br')
				priority = int(raw[a+3:b])
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
		type = s['RequestType']
	except:
		type = ''
	try:
		status = s['RequestStatus']
	except:
		status = ''
	
	return {'type':type,'status':status,'prepid':prepid,'globaltag':globaltag,'priority':priority}

def main():
	global legal_eras,zones

	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-w', '--workflow', help='workflow name',dest='workflow')
	parser.add_option('-l', '--workflowlist', help='workflow list in textfile',dest='list')
	parser.add_option('-t', '--team', help='team',dest='team')
	parser.add_option('--test', action="store_true",default=True,help='test mode: don\'treally assign at the end',dest='test')
	parser.add_option('--assign', action="store_false",default=True,help='assign mode',dest='test')
	parser.add_option('-z', '--zone', help='Zone %s or single site or comma-separated list (i.e. T1_US_FNAL,T2_FR_CCIN2P3,T2_DE_DESY)' % zones,dest='zone')
	parser.add_option('-a', '--acqera', help='Acquisition era: one of %s' % legal_eras,dest='acqera')
	parser.add_option('-p', '--procversion', help='Processing Version',dest='procversion')
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
		team=options.team
	else:
		print "Please provide a team!"
		sys.exit(1)
	if options.zone:
		sitelist = getsitelist(options.zone)
		print "Sitelist: %s" % sitelist
	else:
		print "Please provide a zone/site/sitelist!"
		sys.exit(1)

	procversion=options.procversion
	if options.acqera:
		acqera = options.acqera
	else:
		print "Acquisition Era not provided, please provide one among %s" % legal_eras
		sys.exit(1)
	if options.procversion:
		procversion = options.procversion
	else:
		print "Please provide a ProcessingVersion!"
		sys.exit(1)
		
	reqinfo = {}
	
	print "Team is %s" % team
	for w in list:
		reqinfo[w] = getWorkflowInfo(w)
		if reqinfo[w]['status'] == '':
			print "Cannot get information for %s!" % w
			sys.exit(1)
		if not 'MonteCarlo' in reqinfo[w]['type']:
			print "%s: not a MonteCarlo/MonteCarloFromGEN request!" % w
			sys.exit(1)
		print "* CHECKING %s" % w
		for i in reqinfo[w].keys():
			print "\t%s: %s" % (i,reqinfo[w][i])
		if reqinfo[w]['status'] != 'assignment-approved':
			print "%s: not in status assignment-approved! (status is '%s')" % (w,reqinfo[w]['status'])
			sys.exit(1)
		guess_acqera = getacqera(reqinfo[w])
		if guess_acqera != acqera:
			print "%s: PREPID '%s' is not consistent with the provided acquisition era (%s)" % (w,reqinfo[w]['prepid'],acqera)
			sys.exit(1)
		else:
			print "\tacqera: %s" % acqera
		if not reqinfo[w]['globaltag'] in procversion:
			print "%s: GlobalTag %s not consistent with the provided ProcessingVersion (%s)" % (w,reqinfo[w]['globaltag'],procversion)
			sys.exit(1)
		else:
			print "\tprocessingversion: %s" % procversion
		print

	for w in list:
		if options.test:
			print "TEST: %s\n\tteam: %s\n\tacquisition era: %s\n\tProcessingVersion: %s\n\tWhitelist: %s\n" % (w,team,acqera,procversion,sitelist)
		else:
			print "ASSIGN: %s\n\tteam: %s\n\tacquisition era: %s\n\tProcessingVersion: %s\n\tWhitelist: %s\n" % (w,team,acqera,procversion,sitelist)
			assignMCRequest(url,w,team,sitelist,acqera,procversion)
	
	print "The following requests:\n"
	if options.test:
		ts = "TESTED"
	else:
		ts = "ASSIGNED"
	for w in list:
		print "\t%s" % w
	print "\nhave been %s:\n\n\tteam: %s\n\twhitelist: %s\n\tacqera: %s\n\tprocessingversion: %s" % (ts,team,sitelist,acqera,procversion)
	print "\n"
		
		

	sys.exit(0)

if __name__ == "__main__":
	main()
