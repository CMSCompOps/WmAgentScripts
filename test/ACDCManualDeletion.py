from WMCore.ACDC.CouchService import CouchService
acdcService = CouchService(url = "https://cmsweb.cern.ch/couchdb", database = "acdcserver")

reportND = ""
reportD = ""
total = 0
deletedReq = 0
safeToDelete = ["alahiff_EWK-Summer12DR53X-00157_T1_US_FNAL_MSS_00121_v0__140314_154105_6109_5d7b6966-1e35-11e4-b090-00155dff86bf",
                "alahiff_EWK-Summer12DR53X-00157_T1_US_FNAL_MSS_00121_v0__140314_154105_6109_7a20b8fa-1d68-11e4-811f-00155dff86bf",
                "alahiff_EWK-Summer12DR53X-00157_T1_US_FNAL_MSS_00121_v0__140314_154105_6109_9b2aa22a-1d6f-11e4-811f-00155dff86bf",
                "alahiff_EWK-Summer12DR53X-00157_T1_US_FNAL_MSS_00121_v0__140314_154105_6109_abf63806-1e2f-11e4-b090-00155dff86bf"]
safeToDelete = ["pdmvserv_FSQ-ppSpring2014-00003_00003_v0_castor_140404_150335_2560"]
safeToDelete = ["pdmvserv_TOP-Fall13dr-00015_T0_CH_CERN_MSS_00238_v0__140602_181237_9559",
                "pdmvserv_TOP-Fall13dr-00015_T0_CH_CERN_MSS_00250_v2__140614_162239_9238"]
safeToDelete = ["pdmvserv_TOP-Fall13dr-00017_T0_CH_CERN_MSS_00240_v0__140602_181244_5250",
                "pdmvserv_TOP-Fall13dr-00017_T0_CH_CERN_MSS_00252_v2__140614_163443_6723",
                "pdmvserv_TOP-Fall13dr-00016_T0_CH_CERN_MSS_00239_v0__140602_181241_2611"]
for req in safeToDelete:
    deleted = acdcService.removeFilesetsByCollectionName(req)
    if deleted == None:
        reportND += "%s\n" % req
    else:
        num = len(deleted)
        reportD +=  "%s :%s\n" % (req, num)
        total += num
        deletedReq += 1

print "Aleady Gone"
print reportND

print "DELETED"
print reportD