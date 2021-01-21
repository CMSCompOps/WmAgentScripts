#!/usr/bin/env python
"""
Creates a list of files to transfer from FNAL /User (tape) to EOS
This feeds ftsuser-transfer-submit-list-luis script

Usage: python transferFiles_FNAL.py [dbslocal] [ticket] [dataset]
"""
import sys
from dbs.apis.dbsClient import DbsApi

#Variables
dbsUrl = 'https://cmsweb.cern.ch/dbs/prod/'+sys.argv[1]+'/DBSReader/'

#Init DBS connection
dbsApi = DbsApi(url = dbsUrl)

#Get files and write output file
files = dbsApi.listFiles( dataset = sys.argv[3] )
outputFile = open(sys.argv[2]+'.txt', 'w+')

for lfn in files:
    outputFile.write("%s %s\n" % (lfn['logical_file_name'],lfn['logical_file_name']))
    
outputFile.close()