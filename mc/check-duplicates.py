#!/usr/bin/env python
import simplejson as json
import urllib2,urllib, httplib, sys, re, os 
from xml.dom.minidom import getDOMImplementation
try:
    import json
except ImportError:
    import simplejson as json

def duplicateLumi(dataset):
        querry="/afs/cern.ch/user/s/spinoso/public/dbssql --limit=10000000 --input='find file, lumi where dataset="+dataset+"'| grep store| awk '{print $2}' | sort | uniq -c | awk '{print $1}' | sort | uniq | awk '{if ($1>1) print $1}'"
        output=os.popen(querry).read()
        if output:
		return True
        else:
               	return False

def main():
	if len(sys.argv)<2:
		print "Usage: python check-duplicates.py <dataset>"
		sys.exit(1)
	dataset = sys.argv[1]
	if duplicateLumi(dataset):
		print "%s has duplicated events" % dataset
	else:
		print "%s does not have duplicated events" % dataset
		
        sys.exit(0)

if __name__ == "__main__":
        main()
