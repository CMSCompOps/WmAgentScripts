from WMCore.Services.Requests import JSONRequests
jsonSender = JSONRequests("https://cmsweb.cern.ch/dbs/prod/global/DBSReader")
result = jsonSender.get("/serverinfo")
print result
result = jsonSender.get("/datasetparents?dataset=/Cosmics/Commissioning2015-6Mar2015-v1/RECO")
print result
result = jsonSender.get("/datasetparents?dataset=/Cosmics/Commissioning2015-v1/RAW")
print result
result = jsonSender.get("/datasets?dataset=/Cosmics/Commissioning2015-v1/RAW")
print result
result = jsonSender.get("/datasets?dataset_access_type=*&dataset=/Cosmics/Commissioning2015-6Mar2015-v1/RECO")
print result