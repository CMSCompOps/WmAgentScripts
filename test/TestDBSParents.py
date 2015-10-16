'''
Created on Mar 25, 2015

@author: sryu
'''
from WMCore.Services.DBS.DBS3Reader import DBS3Reader

url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"
dbs = DBS3Reader(url)
print dbs.listFilesInBlockWithParents("/Cosmics/Commissioning2015-6Mar2015-v1/RECO#d6ff8a0a-c6e2-11e4-b23a-a0369f23d008")