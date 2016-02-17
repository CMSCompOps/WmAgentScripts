#!/usr/bin/env python
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
import pprint 

phedexReader = PhEDEx()

blockDict = {'/HIExpressPhysics/HIRun2015-Express-v1/FEVT#0d57ac36-9d1b-11e5-9720-001e67abf228': 
             set(['/store/express/HIRun2015/HIExpressPhysics/FEVT/Express-v1/000/263/406/00000/64E6B5AF-169D-E511-9E46-02163E0142A6.root',
                  '/store/express/HIRun2015/HIExpressPhysics/FEVT/Express-v1/000/263/410/00000/2882B9C9-169D-E511-A6D6-02163E0133E4.root',
                  '/store/express/HIRun2015/HIExpressPhysics/FEVT/Express-v1/000/263/410/00000/505C0B76-F89C-E511-AE64-02163E014594.root',
                  '/store/express/HIRun2015/HIExpressPhysics/FEVT/Express-v1/000/263/410/00000/58F73D10-DE9C-E511-9A35-02163E0141AE.root',
                  '/store/express/HIRun2015/HIExpressPhysics/FEVT/Express-v1/000/263/410/00000/705A6904-F89C-E511-B805-02163E014594.root',
                  '/store/express/HIRun2015/HIExpressPhysics/FEVT/Express-v1/000/263/410/00000/881BFCCA-159D-E511-ACA2-02163E01470B.root',
                  '/store/express/HIRun2015/HIExpressPhysics/FEVT/Express-v1/000/263/410/00000/A8CD0A76-169D-E511-A2E6-02163E0144E7.root',
                  '/store/express/HIRun2015/HIExpressPhysics/FEVT/Express-v1/000/263/410/00000/BE257665-DA9C-E511-8BD2-02163E0141AE.root',
                  '/store/express/HIRun2015/HIExpressPhysics/FEVT/Express-v1/000/263/412/00000/840514C6-F49C-E511-816F-02163E0139D0.root'])}

'/HIOniaL1DoubleMu0/HIRun2015-v1/RAW#e1f36958-a54f-11e5-9720-001e67abf228'

blockDict = {'/SingleMuHighPt/Run2015E-ZMM-PromptReco-v1/RAW-RECO#5193d382-a6a0-11e5-9720-001e67abf228': 
set(['/store/data/Run2015E/SingleMuHighPt/RAW-RECO/ZMM-PromptReco-v1/000/262/326/00000/208DADBD-9FA6-E511-8F75-02163E011B0C.root',
     '/store/data/Run2015E/SingleMuHighPt/RAW-RECO/ZMM-PromptReco-v1/000/262/327/00000/36AB2E12-D2A6-E511-AF57-02163E0135B8.root',
     '/store/data/Run2015E/SingleMuHighPt/RAW-RECO/ZMM-PromptReco-v1/000/262/327/00000/7623BE10-D2A6-E511-862A-02163E012808.root'])}
#pprint.pprint(phedexReader.getInjectedFiles(blockDict))

blockDict = {'/HIHardProbesPhotons/HIRun2015-PromptReco-v1/AOD#479ecbda-ad07-11e5-9720-001e67abf228':
set(['/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/042B59D7-2EAD-E511-9FDF-02163E011826.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/047C56B5-34AD-E511-A7E1-02163E0126A8.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/06145055-47AD-E511-86D7-02163E0144D3.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/08092349-3EAD-E511-9EC1-02163E013787.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/0E961FA7-32AD-E511-9090-02163E01384D.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/1490DF68-38AD-E511-8785-02163E0143F7.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/1CD5ECAF-32AD-E511-828B-02163E011C4D.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/281C3F55-33AD-E511-88C2-02163E0134D6.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/2A9D1BD2-2EAD-E511-919D-02163E013948.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/32844AD7-33AD-E511-8F23-02163E011C4D.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/462315D7-2EAD-E511-B1F0-02163E01344C.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/4AC55959-35AD-E511-AB8F-02163E01353A.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/52BE06E3-2FAD-E511-AF9A-02163E0127A9.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/58A5A4A2-30AD-E511-9F9D-02163E0137C0.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/6A8226D1-31AD-E511-AC25-02163E0134A5.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/6E5A0CD6-2EAD-E511-97B8-02163E01451A.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/6EB1CC44-30AD-E511-BD0F-02163E0145C3.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/7A996266-2FAD-E511-ABCC-02163E012A56.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/8C0A4694-31AD-E511-B78F-02163E011972.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/8EF1FD45-2FAD-E511-AF41-02163E013930.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/9032E876-30AD-E511-BBFD-02163E011826.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/9E2EB64B-33AD-E511-8776-02163E0135B3.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/A0806FC8-30AD-E511-BCF8-02163E012A56.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/ACF6DBC1-34AD-E511-B655-02163E0126A8.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/B4009A8B-31AD-E511-842F-02163E0144DB.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/BC2DAA02-34AD-E511-B2DB-02163E014623.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/D610A55B-33AD-E511-9773-02163E01353A.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/D89CBB6F-34AD-E511-AC7A-02163E01384D.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/DC86B4C8-30AD-E511-A8E0-02163E0145BD.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/E0A1E929-30AD-E511-B222-02163E01358E.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/E4DDF364-32AD-E511-91EE-02163E0133F0.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/E66E6753-2EAD-E511-8B64-02163E0138C9.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/F2305663-34AD-E511-AEEB-02163E011826.root',
     '/store/hidata/HIRun2015/HIHardProbesPhotons/AOD/PromptReco-v1/000/263/410/00000/F6BDA8E8-2FAD-E511-8A9C-02163E011D44.root'])}

blockDict = {'/HIOniaTnP/HIRun2015-PromptReco-v1/AOD#289b71ce-b852-11e5-9720-001e67abf228':
             set(['/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/689/00000/1EDF3445-57B8-E511-995B-02163E0145DA.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/689/00000/82470F7E-4FB8-E511-899A-02163E013762.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/689/00000/847F8CF2-50B8-E511-A129-02163E011A75.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/689/00000/B8570FEA-51B8-E511-8801-02163E0128FA.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/0A656E54-51B8-E511-9B62-02163E01195C.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/14FE6E3C-5CB8-E511-8F87-02163E013509.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/2608B54A-5AB8-E511-8907-02163E0145C6.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/2AF3489E-5BB8-E511-B7A7-02163E0126E0.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/2CBA8C8E-54B8-E511-9999-02163E01451C.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/2E49543F-55B8-E511-ACAB-02163E011B52.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/320418DF-5AB8-E511-814C-02163E01356A.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/32833C2D-5AB8-E511-B4C6-02163E014372.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/38722124-57B8-E511-AB74-02163E012AFA.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/3A3D358F-5BB8-E511-B63F-02163E01412C.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/3C72055D-59B8-E511-AB17-02163E011A5C.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/40F9D3CA-5AB8-E511-AB47-02163E011B52.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/486A2F47-5AB8-E511-ADD9-02163E013769.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/50208398-5BB8-E511-A3D9-02163E01420D.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/54C253C0-53B8-E511-BC72-02163E012380.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/5685AC01-54B8-E511-A1B9-02163E01443C.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/5CEF1A00-59B8-E511-9C1A-02163E014178.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/5E5349A4-57B8-E511-B0CE-02163E011B52.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/60E432DD-51B8-E511-AD2B-02163E0135A5.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/640FE155-5BB8-E511-9D5E-02163E0137DA.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/68A7AF73-55B8-E511-96C3-02163E0126B9.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/68D065C1-55B8-E511-A685-02163E01194C.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/700BE334-54B8-E511-AD5D-02163E014348.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/764AC6D8-5AB8-E511-B267-02163E01443C.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/7829E975-5AB8-E511-914A-02163E011FBA.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/8420560B-5AB8-E511-9CA2-02163E0133C0.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/88218403-57B8-E511-80F2-02163E011C7E.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/8A09B1F7-55B8-E511-8C56-02163E0141D2.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/8A6FE56C-55B8-E511-96B0-02163E014434.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/92D4633E-52B8-E511-BCFD-02163E014348.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/96999EDC-50B8-E511-A186-02163E011F6F.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/98204CF7-59B8-E511-AD66-02163E011F93.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/9A44307A-59B8-E511-AE1D-02163E0142B1.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/9C6F6EFF-54B8-E511-8675-02163E011846.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/AAACE27A-56B8-E511-B018-02163E0141EF.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/BAAA1082-54B8-E511-B580-02163E01456D.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/C02BE168-57B8-E511-B8EB-02163E0118D6.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/C0C71FAD-5BB8-E511-A552-02163E011CA3.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/C27A48E8-5BB8-E511-8911-02163E011F62.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/CAE86DC7-59B8-E511-B3B5-02163E01431B.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/D6D75EE6-58B8-E511-A64B-02163E014389.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/D81613A8-5BB8-E511-9125-02163E01247F.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/D828FC9B-53B8-E511-BDA9-02163E0146F5.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/E42264FC-52B8-E511-A942-02163E014636.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/F039E198-52B8-E511-BC6B-02163E014506.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/F6796DD9-58B8-E511-B5BD-02163E014280.root',
     '/store/hidata/HIRun2015/HIOniaTnP/AOD/PromptReco-v1/000/263/718/00000/FAA95F33-56B8-E511-A233-02163E013742.root'])}

blockDict = {'/MinimumBias/HIRun2015-SiStripCalZeroBias-PromptReco-v1/ALCARECO#99913b74-b402-11e5-9720-001e67abf228':
             set(['/store/hidata/HIRun2015/MinimumBias/ALCARECO/SiStripCalZeroBias-PromptReco-v1/000/263/524/00000/42A55A76-01B4-E511-92C7-02163E0143B3.root'])}

blockDict = {'/HIPhoton40AndZ/HIRun2015-ZEE-PromptReco-v1/AOD#d541de70-b859-11e5-9720-001e67abf228':
set(['/store/hidata/HIRun2015/HIPhoton40AndZ/AOD/ZEE-PromptReco-v1/000/263/689/00000/4602425C-BFB8-E511-AB8C-02163E0142B1.root',
     '/store/hidata/HIRun2015/HIPhoton40AndZ/AOD/ZEE-PromptReco-v1/000/263/718/00000/3A7C80B4-BFB8-E511-B445-02163E014528.root'])}

blockDict = {'/HIMinimumBias3/HIRun2015-PromptReco-v1/AOD#6de573de-b52c-11e5-9720-001e67abf228':
             
result = phedexReader.getInjectedFiles(blockDict)
pprint.pprint(result)
pprint.pprint(len(result))

#dataset = '/ADDGravToGG_MS-3500_NED-4_KK-1_M-1000To2000_13TeV-sherpa/RunIIFall15MiniAODv1-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/MINIAODSIM'
#node = 'T1_US_FNAL_Disk'
#existingRequests = phedexReader.getRequestList(dataset = dataset,
#                                                        node = node,
#                                                        decision = 'pending')['phedex']['request']
#pprint.pprint(existingRequests)