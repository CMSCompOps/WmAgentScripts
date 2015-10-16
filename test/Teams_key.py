from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from pprint import pprint

url = "https://cmsweb-testbed.cern.ch/reqmgr2"
reqMgr = ReqMgr(url)
result = reqMgr.getRequestByStatus("assigned")
#pprint(result)
for requestName, value in result.items():
    if "Teams" not in value:
        print requestName
    else:
        print value["Teams"]
        print value["Team"]
