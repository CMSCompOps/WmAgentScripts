#!/usr/bin/env python -w
import sys,urllib,urllib2,re,time,os
import optparse
import httplib
from xml.dom.minidom import getDOMImplementation
try:
    import json
except ImportError:
    import simplejson as json

url = 'cmsweb.cern.ch'

nodelist = ('T1_DE_KIT_MSS' ,'T1_ES_PIC_MSS' ,'T1_FR_CCIN2P3_MSS','T1_UK_RAL_Disk' ,'T2_BE_IIHE' ,'T2_BE_UCL' ,'T2_BR_SPRACE' ,'T2_CH_CSCS' ,'T2_CN_Beijing' ,'T2_DE_DESY' ,'T2_DE_RWTH' ,'T2_FI_HIP' ,'T2_FR_CCIN2P3' ,'T2_FR_GRIF_LLR' ,'T2_FR_IPHC' ,'T2_HU_Budapest' ,'T2_IN_TIFR' ,'T2_PT_NCG_Lisbon','T2_RU_JINR' ,'T2_RU_SINP' ,'T2_TR_METU' ,'T2_TW_Taiwan' ,'T2_UK_London_Brunel' ,'T2_UK_London_IC' ,'T2_UK_SGrid_RALPP' ,'T2_US_Caltech' ,'T2_US_Florida' ,'T2_US_MIT' ,'T2_US_Nebraska' ,'T2_US_Purdue' ,'T2_US_UCSD' ,'T2_BR_UERJ','T3_US_Colorado','T2_RU_IHEP','T2_RU_ITEP','T2_KR_KNU')
autonodelist = ('T2_CH_CERN','T2_IT_Bari' ,'T2_IT_Legnaro' ,'T2_IT_Pisa' ,'T2_IT_Rome' ,'T1_IT_CNAF_MSS','T2_ES_CIEMAT','T2_ES_IFCA','T2_EE_Estonia','T2_US_Wisconsin')
custodialt1 = 'T1_US_FNAL_MSS'

def createXML(datasets):
	# Create the minidom document
	impl=getDOMImplementation()
	doc=impl.createDocument(None, "data", None)
	result = doc.createElement("data")
	result.setAttribute('version', '2')
	# Create the <dbs> base element
	dbs = doc.createElement("dbs")
	dbs.setAttribute("name", "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet")
	result.appendChild(dbs)	
	#Create each of the <dataset> element			
	for datasetname in datasets:
		dataset=doc.createElement("dataset")
		dataset.setAttribute("is-open","y")
		dataset.setAttribute("is-transient","y")
		dataset.setAttribute("name",datasetname)
		dbs.appendChild(dataset)
   	return result.toprettyxml(indent="  ")

def main():
	global url,nodelist,custodialt1
	parser = optparse.OptionParser()
	parser.add_option('-l', '--listfile', help='subscribe GEN datasets listed in textfile',dest='list')
	parser.add_option('-w', '--workflow', help='subscribe specific GEN dataset',dest='wf')

	(options,args) = parser.parse_args()

	if options.wf:
		list = [options.wf]
	elif options.list:
		list = open(options.list).read().splitlines()
	else:
		print "List not provided."
		sys.exit(1)
		
	for i, item in enumerate(list):
		list[i] = item.rstrip() 
		list[i] = list[i].lstrip() 

	for i in list:
		print "Checking %s" % i
		if i[len(i)-3:len(i)] != 'GEN':
			print "Not a GEN dataset."
			sys.exit(1)

	datasetXML=createXML(list)
	print
	print "Custodial T1: %s" % custodialt1
	print

	params = urllib.urlencode({ "node" : custodialt1, "data" : datasetXML, "group": "DataOps", "priority":'low', "custodial":"y","request_only":"n" ,"move":"n","no_mail":"n", "comments":'Custodial subscription for GEN datasets needed for LHE MC production'})
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	conn.connect()
	print "SUBSCRIBING"
	conn.request("POST", "/phedex/datasvc/json/prod/subscribe", params)
	response = conn.getresponse()
	print response.status, response.reason
	print response.msg 
	a = response.read()
	if response.status != 200:
		print a
		sys.exit(1)
	s = json.loads(a)
	print s
	print
	print "Custodial replica request to T1: https://cmsweb.cern.ch/phedex/prod/Request::View?request=%s" % s['phedex']['request_created'][0]['id']
	conn.close()

	params = urllib.urlencode({ "node" : autonodelist,"data" : datasetXML, "group": "DataOps", "priority":'low', "custodial":"n","request_only":"n" ,"move":"n","no_mail":"n", "comments":"Input datasets needed for LHE MC production"},doseq=True)
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	conn.connect()
	print "SUBSCRIBING"
	conn.request("POST", "/phedex/datasvc/json/prod/subscribe", params)
	response = conn.getresponse()
	print response.status, response.reason
	print response.msg 
	a = response.read()
	if response.status != 200:
		print a
		sys.exit(1)
	s = json.loads(a)
	print s
	print
	print "Non-custodial auto-approved replica request to sites: https://cmsweb.cern.ch/phedex/prod/Request::View?request=%s" % s['phedex']['request_created'][0]['id']

	params = urllib.urlencode({ "node" : nodelist,"data" : datasetXML, "group": "DataOps", "priority":'low', "custodial":"n","request_only":"y" ,"move":"n","no_mail":"n", "comments":"Input datasets needed for LHE MC production"},doseq=True)
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	conn.connect()
	print "SUBSCRIBING"
	conn.request("POST", "/phedex/datasvc/json/prod/subscribe", params)
	response = conn.getresponse()
	print response.status, response.reason
	print response.msg 
	a = response.read()
	if response.status != 200:
		print a
		sys.exit(1)
	s = json.loads(a)
	print s
	print
	print "Non-custodial replica request to sites: https://cmsweb.cern.ch/phedex/prod/Request::View?request=%s" % s['phedex']['request_created'][0]['id']
	conn.close()

        sys.exit(0)

if __name__ == "__main__":
        main()
