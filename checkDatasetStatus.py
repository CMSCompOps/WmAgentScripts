#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse
from dbs.apis.dbsClient import DbsApi

dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

def getDatasetStatus(dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve dataset summary
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*',detail=True)
        return reply[0]['dataset_access_type']

def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-f', '--filename', help='Filename',dest='filename')
	parser.add_option('-d', '--dataset', help='Dataset',dest='userDataset')
	(options,args) = parser.parse_args()
	if not options.filename and not options.userDataset:
		print "A filename or dataset is required"
		sys.exit(0)

        filename=options.filename

        if options.filename:
           f=open(filename,'r')
        else:
           f=[options.userDataset]

        for dataset in f:
           dataset = dataset.rstrip('\n')
           status = getDatasetStatus(dataset)
           print dataset, status

	sys.exit(0)

if __name__ == "__main__":
	main()
