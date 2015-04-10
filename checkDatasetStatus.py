#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse
import dbs3Client as dbs3

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
           status = dbs3.getDatasetStatus(dataset)
           print dataset, status

	sys.exit(0)

if __name__ == "__main__":
	main()
