from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from pprint import pprint

base_url = "https://cmsweb.cern.ch"
reqmgr_url = "%s/reqmgr2" % base_url

req_list = ['alahiff_6Mar2015_MinimumBias_732p5_150309_145708_5216',
 'alahiff_6Mar2015_MinimumBias_732p5_150323_205621_7090',
 'franzoni_04Jun2015_MinimumBias_743p1_150604_160223_1556',
 'franzoni_04Jun292015_Cosmics_743p1_150604_160201_8618',
 'franzoni_6Mar2015_Cosmics_732p5_150309_175340_221',
 'pdmvserv_EXO-RunIISpring15DR74-00311_00040_v0__150511_195553_6963',
 'pdmvserv_EXO-RunIISpring15DR74-00313_00040_v0__150511_195600_7142',
 'pdmvserv_EXO-RunIISpring15DR74-00318_00040_v0__150511_195614_5584',
 'pdmvserv_EXO-RunIISpring15DR74-00321_00040_v0__150511_195623_1707',
 'pdmvserv_EXO-RunIISpring15DR74-00332_00042_v0__150511_200001_1733',
 'pdmvserv_EXO-RunIISpring15DR74-00337_00042_v0__150511_200018_6209',
 'pdmvserv_EXO-RunIISpring15DR74-00342_00042_v0__150511_200036_5694',
 'pdmvserv_EXO-RunIISpring15DR74-00348_00091_v2__150526_192156_1832',
 'pdmvserv_EXO-RunIISpring15DR74-00834_00080_v0__150526_105702_1354',
 'pdmvserv_EXO-RunIISpring15DR74-00836_00080_v0__150526_105852_2969']
reqmgr = ReqMgr(reqmgr_url)
print len(req_list)
inputs = set()
for req in req_list:
    result = reqmgr.getRequestByNames(req)

    for k, v in result.items():
        print "%s : %s" % (k, v['InputDataset'])

        inputs.add(v['InputDataset'])

print len(inputs)
