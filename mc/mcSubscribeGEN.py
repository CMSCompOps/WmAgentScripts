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

nodelist = ('T1_DE_KIT_MSS' ,'T1_ES_PIC_MSS' ,'T1_FR_CCIN2P3_MSS' ,'T1_IT_CNAF_MSS' ,'T1_TW_ASGC_MSS' ,'T1_UK_RAL_MSS' ,'T1_US_FNAL_MSS' ,'T2_BE_IIHE' ,'T2_BE_UCL' ,'T2_BR_SPRACE' ,'T2_CH_CSCS' ,'T2_CN_Beijing' ,'T2_DE_DESY' ,'T2_DE_RWTH' ,'T2_EE_Estonia' ,'T2_ES_CIEMAT' ,'T2_ES_IFCA' ,'T2_FI_HIP' ,'T2_FR_CCIN2P3' ,'T2_FR_GRIF_LLR' ,'T2_FR_IPHC' ,'T2_HU_Budapest' ,'T2_IN_TIFR' ,'T2_IT_Bari' ,'T2_IT_Legnaro' ,'T2_IT_Pisa' ,'T2_IT_Rome' ,'T2_PL_Warsaw' ,'T2_PT_NCG_Lisbon' ,'T2_RU_INR' ,'T2_RU_JINR' ,'T2_RU_PNPI' ,'T2_RU_SINP' ,'T2_TR_METU' ,'T2_TW_Taiwan' ,'T2_UA_KIPT' ,'T2_UK_London_Brunel' ,'T2_UK_London_IC' ,'T2_UK_SGrid_RALPP' ,'T2_US_Caltech' ,'T2_US_Florida' ,'T2_US_MIT' ,'T2_US_Nebraska' ,'T2_US_Purdue' ,'T2_US_UCSD' ,'T2_US_Wisconsin')

comments = "Input datasets needed for LHE MC production"

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
	global url,nodelist,nodelist
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
		#print "Checking %s" % i
		if i[len(i)-3:len(i)] != 'GEN':
			print "Not a GEN dataset."
			sys.exit(1)

	datasetXML=createXML(list)
	params = urllib.urlencode({ "node" : nodelist,"data" : datasetXML, "group": "DataOps", "priority":'low', "custodial":"n","request_only":"y" ,"move":"n","no_mail":"n", "comments":comments},doseq=True)
	#print "URL: %s" % url
	#print "Nodelist: %s" % nodelist
	#print "Dataset list: %s" % list
	#print datasetXML
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	conn.connect()
	print "SUBSCRIBING"
	conn.request("POST", "/phedex/datasvc/json/prod/subscribe", params)
	response = conn.getresponse()
	print response.status, response.reason
	a = response.read()
	s = json.loads(a)
	print s
	print "Here the request URL: https://cmsweb.cern.ch/phedex/prod/Request::View?request=%s" % s['phedex']['request_created'][0]['id']
        sys.exit(0)

if __name__ == "__main__":
        main()
