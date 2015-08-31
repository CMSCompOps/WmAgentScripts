#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse
from dbs.apis.dbsClient import DbsApi
from das_client import get_data

dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
das_host='https://cmsweb.cern.ch'

def getSizeAtSizeDAS(dataset, site):
        query = "site dataset="+dataset
        das_data = get_data(das_host,query,0,0,0)
        myStatus = ''
        if isinstance(das_data, basestring):
           result = json.loads(das_data)
        else:
           result = das_data
           if result['status'] == 'fail' :
              print 'ERROR: DAS query failed with reason:',result['reason']
              sys.exit(0)
           else:
              preresult = result['data']
              for key in preresult:
                 if key['site'][0]['name'] == site:
                    return key['site'][0]['dataset_fraction']
        return 'Unknown'


def getSize(dataset):
       # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve dataset summary
        reply = dbsapi.listBlocks(dataset=dataset,detail=True)
        sum = 0
        for block in reply:
           sum = sum + block['block_size']
        return sum

def getSizeAtSite(url, dataset, site):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset+'&node='+site)
        r2=conn.getresponse()
        result = json.loads(r2.read())
        request=result['phedex']
        if 'block' not in request.keys():
                return -1
        if len(request['block'])==0:
                return -1
        sum = 0
        for replica in request['block']:
           sum = sum + replica['bytes']
        return sum

def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-f', '--filename', help='Filename',dest='filename')
	parser.add_option('-d', '--dataset', help='Dataset',dest='userDataset')
	parser.add_option('-w', '--workflow', help='Workflow',dest='userWorkflow')
	parser.add_option('-s', '--site', help='Site',dest='userSite')
	(options,args) = parser.parse_args()
	if not options.filename and not options.userDataset:
		print "A filename or dataset is required"
		sys.exit(0)
        userSite = options.userSite

        filename=options.filename

        if options.filename:
           f=open(filename,'r')
        else:
           f=[options.userDataset]

        for line in f:
           pieces = line.split()
           sizeAtSiteDAS = getSizeAtSizeDAS(pieces[1], userSite)
           #sizeAtSite = getSizeAtSite(url, pieces[1], userSite)
           #sizeTotal = getSize(pieces[1])
           #percent = 100.0*sizeAtSite/sizeTotal
           print pieces[0], pieces[1], sizeAtSiteDAS

	sys.exit(0)

if __name__ == "__main__":
	main()
