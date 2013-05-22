#!/usr/bin/env python -w
import sys,urllib,urllib2,re,time,os
import simplejson as json
import optparse
import httplib
import datetime
import shutil
import sys

dashost = 'https://cmsweb.cern.ch'
reqmgrsocket='vocms204.cern.ch'
overview = ''
count = 1
#tiers = ['GEN-SIM','GEN-SIM-RECO','DQM','AODSIM','GEN-SIM-DIGI-RECO']
eras = ['Summer11','Summer12']
cachedoverview = '/afs/cern.ch/user/s/spinoso/public/overview.cache'
forceoverview = 0

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
        print "Configuration loaded successfully from %s" % f
        return s

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
	sizeev = -1
	prepid = ''
	sites = []
	for raw in list:
		if 'primaryDataset' in raw:
			primaryds = raw[raw.find("'")+1:]
			primaryds = primaryds[0:primaryds.find("'")]
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
				#print "*%s*" % raw[a+3:b]
				priority = int(raw[a+3:b])
		elif 'white' in raw and not '[]' in raw:
			sites = '['+raw[raw.find("[")+1:raw.find("]")]+']'	
			sites = eval(sites)		
		elif 'SizePerEvent' in raw:
                        a = raw.find("'")
                        if a >= 0:
                                b = raw.find("'",a+1)
                                sizeev = int(raw[a+1:b])
                        else:
                                a = raw.find(" =")
                                b = raw.find('<br')
                                sizeev = int(float(raw[a+3:b]))
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
		type = s['RequestType']
	except:
		type = ''
	try:
		status = s['RequestStatus']
	except:
		status = ''
	
	return {'type':type,'status':status,'primaryds':primaryds,'prepid':prepid,'sizeev':sizeev}


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

def getdsdetail(dataset):
	query = 'dataset dataset=' + dataset + ' status=*|grep dataset.nevents,dataset.status'

	[e,st] = das_get_data(query)
	if e == -1:
		return [0,'']
	else:
		return [e,st]

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

def main():
	global overview,count,jobcount

	campaignconfig = loadcampaignconfig('/afs/cern.ch/user/c/cmst2/public/MCCONFIG/campaign.cfg')

	parser = optparse.OptionParser()
	parser.add_option('-l', '--listfile', help='analyze workflows listed in textfile',dest='list')
	parser.add_option('-w', '--workflow', help='analyze specific workflow',dest='wf')
	parser.add_option('-b', '--batch', help='batch',dest='batch')
	parser.add_option('-c', '--custodialt1', help='custodial T1',dest='t1')

	(options,args) = parser.parse_args()

	if options.wf:
		list = [options.wf]
	elif options.list:
		list = open(options.list).read().splitlines()
	else:
		print "List not provided."
		sys.exit(1)

	if not options.batch:
		print "Please provide the batch parameter (RXXXX_BYYY)"
		sys.exit(1)
	else:
		batch = options.batch
		
	if not options.t1:
		print "Please provide the custodial T1"
		sys.exit(1)
	else:
		t1 = options.t1
		
	overview = getoverview()
	reqinfo = {}
	for workflow in list:
		reqinfo[workflow] = getWorkflowInfo(workflow)

	print
	print "Custodial LFNs for %s %s (%s)" % (campaign,batch,t1)
	print
	print "Dear admins,\n\nplease create the tape families below[*], needed for MC production.\n\nThanks!\n Vincenzo & Ajit.\n\n[*]"

	prepidlist = []
	paths = []
	for workflow in reqinfo.keys():
		prepid = reqinfo[workflow]['prepid']
		prepidlist.append(prepid)
		prids = reqinfo[workflow]['primaryds']
		if options.heavyion:
			path2='himc'
		else:
			path2='mc'
		for i in tiers:
			tf = "/store/"+path2+"/"+acqera+"/"+prids+"/"+i
			#print "%s" % (tf)
			if tf not in paths:
				paths.append(tf)
	for i in paths:
		print "%s" % (i)
	prepidlist.sort()

	print "\nPREPID: "+", ".join(prepidlist)
        sys.exit(0)

if __name__ == "__main__":
        main()
