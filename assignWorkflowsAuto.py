#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse

def getStatusDataSet(dataset):
        output=os.popen("./dbssql --input='find dataset.status where dataset="+dataset+" and dataset.status=*'"+ "|awk '{print $2}' ").read()
        try:
                myStatus = output[output.find("'")+1:output.find("'",output.find("'")+1)]
                return myStatus
        except ValueError:
                return "Unknown"

def outputdatasetsWorkflow(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=' + workflow)
        r2=conn.getresponse()
        datasets = json.read(r2.read())
        if len(datasets)==0:
                print "ERROR: No output datasets for this workflow"
                sys.exit(0)
        return datasets

def getDatasetVersion(url, workflow, era, partialProcVersion):
        versionNum = 1
        outputs = outputdatasetsWorkflow(url, workflow)
        for output in outputs:
           if 'None-v0' not in output:
              print 'ERROR: Problem checking output datasets'
              sys.exit(0)
           bits = output.split('/')
           lastbit = bits[len(bits)-1]
           outputCheck = re.sub(r'None-v0', era+'-'+partialProcVersion+'*', output)
           output=os.popen("./dbssql --input='find dataset where dataset="+outputCheck+" and dataset.status=*' | grep "+lastbit).read()
           lines = output.split('\n')
           for line in lines:
              matchObj = re.match(r".*-v(\d+)/.*", line)
              if matchObj:
                 currentVersionNum = int(matchObj.group(1))
                 if versionNum <= currentVersionNum:
                    versionNum=versionNum+1

        return versionNum

def getScenario(ps):
        pss = 'Unknown'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59),probValue=cms.vdouble(2.56e-06,5.239e-06,1.42e-05,5.005e-05,0.0001001,0.0002705,0.001999,0.006097,0.01046,0.01383,0.01685,0.02055,0.02572,0.03262,0.04121,0.04977,0.05539,0.05725,0.05607,0.05312,0.05008,0.04763,0.04558,0.04363,0.04159,0.03933,0.03681,0.03406,0.03116,0.02818,0.02519,0.02226,0.01946,0.01682,0.01437,0.01215,0.01016,0.0084,0.006873,0.005564,0.004457,0.003533,0.002772,0.002154,0.001656,0.001261,0.0009513,0.0007107,0.0005259,0.0003856,0.0002801,0.0002017,0.0001439,0.0001017,7.126e-05,4.948e-05,3.405e-05,2.322e-05,1.57e-05,5.005e-06)),OOT_type=cms.untracked.string('Poisson'),":
           pss = 'PU_S10'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(averageNumber=cms.double(32.0)),sequential=cms.untracked.bool(False),type=cms.string('poisson'),":
           pss = 'PU_S9'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(2),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-3),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(averageNumber=cms.double(4.0)),sequential=cms.untracked.bool(False),type=cms.string('poisson'),":
           pss = 'PU_S8'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59),probValue=cms.vdouble(2.344e-05,2.344e-05,2.344e-05,2.344e-05,0.0004687,0.0004687,0.0007032,0.0009414,0.001234,0.001603,0.002464,0.00325,0.005021,0.006644,0.008502,0.01121,0.01518,0.02033,0.02608,0.03171,0.03667,0.0406,0.04338,0.0452,0.04641,0.04735,0.04816,0.04881,0.04917,0.04909,0.04842,0.04707,0.04501,0.04228,0.03896,0.03521,0.03118,0.02702,0.02287,0.01885,0.01508,0.01166,0.008673,0.00619,0.004222,0.002746,0.001698,0.0009971,0.0005549,0.0002924,0.0001457,6.864e-05,3.054e-05,1.282e-05,5.081e-06,1.898e-06,6.688e-07,2.221e-07,6.947e-08,2.047e-08)),OOT_type=cms.untracked.string('Poisson'),":
           pss = 'PU_S7'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49),probValue=cms.vdouble(0.003388501,0.010357558,0.024724258,0.042348605,0.058279812,0.068851751,0.072914824,0.071579609,0.066811668,0.060672356,0.054528356,0.04919354,0.044886042,0.041341896,0.0384679,0.035871463,0.03341952,0.030915649,0.028395374,0.025798107,0.023237445,0.020602754,0.0180688,0.015559693,0.013211063,0.010964293,0.008920993,0.007080504,0.005499239,0.004187022,0.003096474,0.002237361,0.001566428,0.001074149,0.000721755,0.000470838,0.00030268,0.000184665,0.000112883,6.74043e-05,3.82178e-05,2.22847e-05,1.20933e-05,6.96173e-06,3.4689e-06,1.96172e-06,8.49283e-07,5.02393e-07,2.15311e-07,9.56938e-08)),OOT_type=cms.untracked.string('Poisson'),":
           pss = 'PU_S6'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-2),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),seed=cms.untracked.int32(54321),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24),probValue=cms.vdouble(0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0698146584,0.0630151648,0.0526654164,0.0402754482,0.0292988928,0.0194384503,0.0122016783,0.007207042,0.004003637,0.0020278322,0.0010739954,0.0004595759,0.0002229748,0.0001028162,4.5833715281e-05)),OOT_type=cms.untracked.string('Poisson'),":
           pss = 'PU_S4'
        if ps == "process.mix=cms.EDProducer(MixingModule,mixProdStep1=cms.bool(False),mixProdStep2=cms.bool(False),maxBunch=cms.int32(0),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(0),bunchspace=cms.int32(450),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(histoFileName=cms.untracked.string('histProbFunction.root'),seed=cms.untracked.int32(54321),probFunctionVariable=cms.vint32(0,1,2,3,4,5,6,7,8,9,10),probValue=cms.vdouble(0.145168,0.251419,0.251596,0.17943,0.1,0.05,0.02,0.01,0.005,0.002,0.001)),sequential=cms.untracked.bool(False),type=cms.string('probFunction'),":
           pss = 'PU_S0'

        if ps == "process.mix=cms.EDProducer(MixingModule,digitizers=cms.PSet(hcal=cms.PSet(HFTuningParameter=cms.double(1.025),HETuningParameter=cms.double(0.9),doHFWindow=cms.bool(False),doNoise=cms.bool(True),hb=cms.PSet(siPMCells=cms.vint32(),readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(True),binOfMaximum=cms.int32(5),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.vdouble(0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305),simHitToPhotoelectrons=cms.double(2000.0),samplingFactors=cms.vdouble(125.44,125.54,125.32,125.13,124.46,125.01,125.22,125.48,124.45,125.9,125.83,127.01,126.82,129.73,131.83,143.52),syncPhase=cms.bool(True),timePhase=cms.double(6.0)),useOldHO=cms.bool(True),useOldHE=cms.bool(True),hoHamamatsu=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(False),siPMCode=cms.int32(2),timePhase=cms.double(5.0),simHitToPhotoelectrons=cms.double(4000.0),binOfMaximum=cms.int32(5),photoelectronsToAnalog=cms.vdouble(3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0),samplingFactors=cms.vdouble(231.0,231.0,231.0,231.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0),syncPhase=cms.bool(True),doPhotoStatistics=cms.bool(True),pixels=cms.int32(960)),useOldHF=cms.bool(True),injectTestHits=cms.bool(False),useOldHB=cms.bool(True),doEmpty=cms.bool(True),he=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(16),timeSmearing=cms.bool(True),binOfMaximum=cms.int32(5),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.vdouble(0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305),simHitToPhotoelectrons=cms.double(2000.0),samplingFactors=cms.vdouble(210.55,197.93,186.12,189.64,189.63,190.28,189.61,189.6,190.12,191.22,190.9,193.06,188.42,188.42),syncPhase=cms.bool(True),timePhase=cms.double(6.0)),hf1=cms.PSet(readoutFrameSize=cms.int32(5),binOfMaximum=cms.int32(3),samplingFactor=cms.double(0.383),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(2.79),simHitToPhotoelectrons=cms.double(6.0),syncPhase=cms.bool(True),timePhase=cms.double(14.0)),hf2=cms.PSet(readoutFrameSize=cms.int32(5),binOfMaximum=cms.int32(3),samplingFactor=cms.double(0.368),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(1.843),simHitToPhotoelectrons=cms.double(6.0),syncPhase=cms.bool(True),timePhase=cms.double(13.0)),HBTuningParameter=cms.double(0.875),doThermalNoise=cms.bool(True),accumulatorType=cms.string('HcalDigiProducer'),hoZecotek=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(False),siPMCode=cms.int32(2),timePhase=cms.double(5.0),simHitToPhotoelectrons=cms.double(4000.0),binOfMaximum=cms.int32(5),photoelectronsToAnalog=cms.vdouble(3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0),samplingFactors=cms.vdouble(231.0,231.0,231.0,231.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0),syncPhase=cms.bool(True),doPhotoStatistics=cms.bool(True),pixels=cms.int32(36000)),hitsProducer=cms.string('g4SimHits'),doTimeSlew=cms.bool(True),ho=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(False),siPMCode=cms.int32(2),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.vdouble(0.24,0.24,0.24,0.24,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17),binOfMaximum=cms.int32(5),simHitToPhotoelectrons=cms.double(4000.0),samplingFactors=cms.vdouble(231.0,231.0,231.0,231.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0),syncPhase=cms.bool(True),timePhase=cms.double(5.0)),HOTuningParameter=cms.double(1),doIonFeedback=cms.bool(True),makeDigiSimLinks=cms.untracked.bool(False),doHPDNoise=cms.bool(False),zdc=cms.PSet(readoutFrameSize=cms.int32(10),binOfMaximum=cms.int32(5),samplingFactor=cms.double(1.0),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(1.843),simHitToPhotoelectrons=cms.double(6.0),syncPhase=cms.bool(True),timePhase=cms.double(-4.0))),strip=cms.PSet(CouplingConstantPeakIB1=cms.vdouble(0.9006,0.0497),TOFCutForPeak=cms.double(100.0),DeltaProductionCut=cms.double(0.120425),RealPedestals=cms.bool(True),Temperature=cms.double(273.0),CouplingConstantPeakOB1=cms.vdouble(0.8542,0.0729),DepletionVoltage=cms.double(170.0),CouplingConstantDecW2b=cms.vdouble(0.888,0.05,0.006),CouplingConstantPeakIB2=cms.vdouble(0.9342,0.0328),CouplingConstantDecW2a=cms.vdouble(0.7962,0.0914,0.0104),CouplingConstantPeakW7=cms.vdouble(0.964,0.018),BaselineShift=cms.bool(True),SingleStripNoise=cms.bool(True),CouplingConstantDecOB1=cms.vdouble(0.6871,0.1222,0.0342),CouplingConstantPeakOB2=cms.vdouble(0.8719,0.064),CouplingConstantPeakW2b=cms.vdouble(0.998,0.001),Inefficiency=cms.double(0.0),CouplingConstantPeakW2a=cms.vdouble(1.0,0.0),ZeroSuppression=cms.bool(True),cmnRMStec=cms.double(2.44),CouplingConstantDecIB2=cms.vdouble(0.83,0.0756,0.0094),Noise=cms.bool(True),LorentzAngle=cms.string(''),noDiffusion=cms.bool(False),LandauFluctuations=cms.bool(True),FedAlgorithm=cms.int32(4),DigiModeList=cms.PSet(SCDigi=cms.string('ScopeMode'),ZSDigi=cms.string('ZeroSuppressed'),PRDigi=cms.string('ProcessedRaw'),VRDigi=cms.string('VirginRaw')),Gain=cms.string(''),CouplingConstantDecW1a=cms.vdouble(0.786,0.093,0.014),APVSaturationProb=cms.double(0.001),electronPerAdcPeak=cms.double(262.0),CouplingConstantDecW6=cms.vdouble(0.758,0.093,0.026),CouplingConstantDecW5=cms.vdouble(0.7565,0.0913,0.0304),CouplingConstantDecW4=cms.vdouble(0.876,0.06,0.002),CouplingConstantDecW1b=cms.vdouble(0.822,0.08,0.009),hitsProducer=cms.string('g4SimHits'),PedestalsOffset=cms.double(128),ROUList=cms.vstring('TrackerHitsTIBLowTof','TrackerHitsTIBHighTof','TrackerHitsTIDLowTof','TrackerHitsTIDHighTof','TrackerHitsTOBLowTof','TrackerHitsTOBHighTof','TrackerHitsTECLowTof','TrackerHitsTECHighTof'),CouplingConstantDecIB1=cms.vdouble(0.7748,0.0962,0.0165),APVSaturationFromHIP=cms.bool(True),GevPerElectron=cms.double(3.61e-09),CouplingConstantDecW3a=cms.vdouble(0.8164,0.09,0.0018),chargeDivisionsPerStrip=cms.int32(10),TOFCutForDeconvolution=cms.double(50.0),CouplingConstantPeakW3a=cms.vdouble(0.996,0.002),CouplingConstantDecW7=cms.vdouble(0.7828,0.0862,0.0224),cmnRMStid=cms.double(3.08),CouplingConstantPeakW3b=cms.vdouble(0.992,0.004),AppliedVoltage=cms.double(300.0),CouplingConstantPeakW1b=cms.vdouble(0.976,0.012),CouplingConstantPeakW1a=cms.vdouble(0.996,0.002),NoiseSigmaThreshold=cms.double(2.0),cmnRMStib=cms.double(5.92),CouplingConstantDecW3b=cms.vdouble(0.848,0.06,0.016),ChargeDistributionRMS=cms.double(6.5e-10),TrackerConfigurationFromDB=cms.bool(False),APVpeakmode=cms.bool(False),CosmicDelayShift=cms.untracked.double(0.0),cmnRMStob=cms.double(1.08),accumulatorType=cms.string('SiStripDigitizer'),CouplingConstantPeakW6=cms.vdouble(0.972,0.014),CouplingConstantPeakW5=cms.vdouble(0.968,0.016),CouplingConstantPeakW4=cms.vdouble(0.992,0.004),electronPerAdcDec=cms.double(247.0),GeometryType=cms.string('idealForDigi'),ChargeMobility=cms.double(310.0),CouplingConstantDecOB2=cms.vdouble(0.725,0.1102,0.0273),CommonModeNoise=cms.bool(True)),castor=cms.PSet(hitsProducer=cms.string('g4SimHits'),castor=cms.PSet(readoutFrameSize=cms.int32(6),binOfMaximum=cms.int32(5),samplingFactor=cms.double(0.060135),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(4.009),simHitToPhotoelectrons=cms.double(1000.0),syncPhase=cms.bool(True),timePhase=cms.double(-4.0)),doNoise=cms.bool(True),doTimeSlew=cms.bool(True),accumulatorType=cms.string('CastorDigiProducer'),makeDigiSimLinks=cms.untracked.bool(False)),pixel=cms.PSet(DeltaProductionCut=cms.double(0.03),FPix_SignalResponse_p2=cms.double(93.6),FPix_SignalResponse_p3=cms.double(134.6),FPix_SignalResponse_p0=cms.double(0.0043),FPix_SignalResponse_p1=cms.double(1.31),TofUpperCut=cms.double(12.5),AddNoisyPixels=cms.bool(True),TanLorentzAnglePerTesla_BPix=cms.double(0.106),AddNoise=cms.bool(True),GainSmearing=cms.double(0.0),AddThresholdSmearing=cms.bool(True),useDB=cms.bool(True),AdcFullScale=cms.int32(255),TofLowerCut=cms.double(-12.5),killModules=cms.bool(True),DeadModules=cms.VPSet(cms.PSet(Dead_detID=cms.int32(302055940),Module=cms.string('tbmB')),cms.PSet(Dead_detID=cms.int32(302059800),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302121992),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302123296),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302125060),Module=cms.string('tbmA')),cms.PSet(Dead_detID=cms.int32(302125076),Module=cms.string('tbmA')),cms.PSet(Dead_detID=cms.int32(302126364),Module=cms.string('tbmB')),cms.PSet(Dead_detID=cms.int32(302126596),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302127136),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302188552),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302188824),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302194200),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302195232),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302197252),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302197784),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453892),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453896),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453900),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453904),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454916),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454920),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454924),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454928),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455940),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455944),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455948),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455952),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454148),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454152),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454156),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455172),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455176),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455180),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352456196),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352456200),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352456204),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999748),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999752),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999756),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999760),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344014340),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344014344),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344014348),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344019460),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344019464),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344019468),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077572),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077576),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077580),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077584),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078596),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078600),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078604),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078608),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079620),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079624),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079628),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079632),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078852),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078856),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078860),Module=cms.string('whole'))),TanLorentzAnglePerTesla_FPix=cms.double(0.106),accumulatorType=cms.string('SiPixelDigitizer'),AddPixelInefficiency=cms.int32(0),LorentzAngle_DB=cms.bool(True),makeDigiSimLinks=cms.untracked.bool(True),BPix_SignalResponse_p2=cms.double(97.4),BPix_SignalResponse_p3=cms.double(126.5),BPix_SignalResponse_p0=cms.double(0.0035),Alpha2Order=cms.bool(True),hitsProducer=cms.string('g4SimHits'),ReadoutNoiseInElec=cms.double(350.0),DeadModules_DB=cms.bool(True),ROUList=cms.vstring('TrackerHitsPixelBarrelLowTof','TrackerHitsPixelBarrelHighTof','TrackerHitsPixelEndcapLowTof','TrackerHitsPixelEndcapHighTof'),OffsetSmearing=cms.double(0.0),NoiseInElectrons=cms.double(175.0),ChargeVCALSmearing=cms.bool(True),ElectronsPerVcal=cms.double(65.5),MissCalibrate=cms.bool(True),ThresholdInElectrons_BPix=cms.double(3500.0),ThresholdSmearing_BPix=cms.double(245.0),ThresholdInElectrons_FPix=cms.double(3000.0),ElectronsPerVcal_Offset=cms.double(-414.0),ThresholdSmearing_FPix=cms.double(210.0),ElectronPerAdc=cms.double(135.0),GeometryType=cms.string('idealForDigi'),BPix_SignalResponse_p1=cms.double(1.23)),ecal=cms.PSet(EEdigiCollection=cms.string(''),readoutFrameSize=cms.int32(10),EBdigiCollection=cms.string(''),apdShapeTau=cms.double(40.5),ESdigiCollection=cms.string(''),apdSimToPEHigh=cms.double(88200000.0),doNoise=cms.bool(True),apdTimeOffset=cms.double(-13.5),EBCorrNoiseMatrixG06=cms.vdouble(1.0,0.70946,0.58021,0.49846,0.45006,0.41366,0.39699,0.38478,0.37847,0.37055),EECorrNoiseMatrixG12=cms.vdouble(1.0,0.71373,0.44825,0.30152,0.21609,0.14786,0.11772,0.10165,0.09465,0.08098),doESNoise=cms.bool(True),apdSeparateDigi=cms.bool(True),EBCorrNoiseMatrixG01=cms.vdouble(1.0,0.73354,0.64442,0.58851,0.55425,0.53082,0.51916,0.51097,0.50732,0.50409),applyConstantTerm=cms.bool(True),binOfMaximum=cms.int32(6),EBs25notContainment=cms.double(0.97),accumulatorType=cms.string('EcalDigiProducer'),apdTimeOffWidth=cms.double(0.8),simHitToPhotoelectronsBarrel=cms.double(2250.0),syncPhase=cms.bool(True),doPhotostatistics=cms.bool(True),apdShapeTstart=cms.double(74.5),hitsProducer=cms.string('g4SimHits'),apdDoPEStats=cms.bool(True),ConstantTerm=cms.double(0.003),apdSimToPELow=cms.double(2450000.0),cosmicsPhase=cms.bool(False),apdNonlParms=cms.vdouble(1.48,-3.75,1.81,1.26,2.0,45,1.0),photoelectronsToAnalogEndcap=cms.double(0.000555555),photoelectronsToAnalogBarrel=cms.double(0.000444444),apdDigiTag=cms.string('APD'),EECorrNoiseMatrixG01=cms.vdouble(1.0,0.72698,0.62048,0.55691,0.51848,0.49147,0.47813,0.47007,0.46621,0.46265),apdAddToBarrel=cms.bool(False),EBCorrNoiseMatrixG12=cms.vdouble(1.0,0.71073,0.55721,0.46089,0.40449,0.35931,0.33924,0.32439,0.31581,0.30481),EECorrNoiseMatrixG06=cms.vdouble(1.0,0.71217,0.47464,0.34056,0.26282,0.20287,0.17734,0.16256,0.15618,0.14443),makeDigiSimLinks=cms.untracked.bool(False),simHitToPhotoelectronsEndcap=cms.double(1800.0),samplingFactor=cms.double(1.0),cosmicsShift=cms.double(0.0),doFast=cms.bool(True),EEs25notContainment=cms.double(0.975),timePhase=cms.double(0.0))),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-5),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(averageNumber=cms.double(50.0)),sequential=cms.untracked.bool(False),type=cms.string('poisson'),":
           pss = 'PU50'

        if ps == "process.mix=cms.EDProducer(MixingModule,digitizers=cms.PSet(hcal=cms.PSet(HFTuningParameter=cms.double(1.025),HETuningParameter=cms.double(0.9),doHFWindow=cms.bool(False),doNoise=cms.bool(True),hb=cms.PSet(siPMCells=cms.vint32(),readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(True),binOfMaximum=cms.int32(5),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.vdouble(0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305),simHitToPhotoelectrons=cms.double(2000.0),samplingFactors=cms.vdouble(125.44,125.54,125.32,125.13,124.46,125.01,125.22,125.48,124.45,125.9,125.83,127.01,126.82,129.73,131.83,143.52),syncPhase=cms.bool(True),timePhase=cms.double(6.0)),useOldHO=cms.bool(True),useOldHE=cms.bool(True),hoHamamatsu=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(False),siPMCode=cms.int32(2),timePhase=cms.double(5.0),simHitToPhotoelectrons=cms.double(4000.0),binOfMaximum=cms.int32(5),photoelectronsToAnalog=cms.vdouble(3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0),samplingFactors=cms.vdouble(231.0,231.0,231.0,231.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0),syncPhase=cms.bool(True),doPhotoStatistics=cms.bool(True),pixels=cms.int32(960)),useOldHF=cms.bool(True),injectTestHits=cms.bool(False),useOldHB=cms.bool(True),doEmpty=cms.bool(True),he=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(16),timeSmearing=cms.bool(True),binOfMaximum=cms.int32(5),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.vdouble(0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305),simHitToPhotoelectrons=cms.double(2000.0),samplingFactors=cms.vdouble(210.55,197.93,186.12,189.64,189.63,190.28,189.61,189.6,190.12,191.22,190.9,193.06,188.42,188.42),syncPhase=cms.bool(True),timePhase=cms.double(6.0)),hf1=cms.PSet(readoutFrameSize=cms.int32(5),binOfMaximum=cms.int32(3),samplingFactor=cms.double(0.383),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(2.79),simHitToPhotoelectrons=cms.double(6.0),syncPhase=cms.bool(True),timePhase=cms.double(14.0)),hf2=cms.PSet(readoutFrameSize=cms.int32(5),binOfMaximum=cms.int32(3),samplingFactor=cms.double(0.368),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(1.843),simHitToPhotoelectrons=cms.double(6.0),syncPhase=cms.bool(True),timePhase=cms.double(13.0)),HBTuningParameter=cms.double(0.875),doThermalNoise=cms.bool(True),accumulatorType=cms.string('HcalDigiProducer'),hoZecotek=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(False),siPMCode=cms.int32(2),timePhase=cms.double(5.0),simHitToPhotoelectrons=cms.double(4000.0),binOfMaximum=cms.int32(5),photoelectronsToAnalog=cms.vdouble(3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0),samplingFactors=cms.vdouble(231.0,231.0,231.0,231.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0),syncPhase=cms.bool(True),doPhotoStatistics=cms.bool(True),pixels=cms.int32(36000)),hitsProducer=cms.string('g4SimHits'),doTimeSlew=cms.bool(True),ho=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(False),siPMCode=cms.int32(2),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.vdouble(0.24,0.24,0.24,0.24,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17),binOfMaximum=cms.int32(5),simHitToPhotoelectrons=cms.double(4000.0),samplingFactors=cms.vdouble(231.0,231.0,231.0,231.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0),syncPhase=cms.bool(True),timePhase=cms.double(5.0)),HOTuningParameter=cms.double(1),doIonFeedback=cms.bool(True),makeDigiSimLinks=cms.untracked.bool(False),doHPDNoise=cms.bool(False),zdc=cms.PSet(readoutFrameSize=cms.int32(10),binOfMaximum=cms.int32(5),samplingFactor=cms.double(1.0),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(1.843),simHitToPhotoelectrons=cms.double(6.0),syncPhase=cms.bool(True),timePhase=cms.double(-4.0))),strip=cms.PSet(CouplingConstantPeakIB1=cms.vdouble(0.9006,0.0497),TOFCutForPeak=cms.double(100.0),DeltaProductionCut=cms.double(0.120425),RealPedestals=cms.bool(True),Temperature=cms.double(273.0),CouplingConstantPeakOB1=cms.vdouble(0.8542,0.0729),DepletionVoltage=cms.double(170.0),CouplingConstantDecW2b=cms.vdouble(0.888,0.05,0.006),CouplingConstantPeakIB2=cms.vdouble(0.9342,0.0328),CouplingConstantDecW2a=cms.vdouble(0.7962,0.0914,0.0104),CouplingConstantPeakW7=cms.vdouble(0.964,0.018),BaselineShift=cms.bool(True),SingleStripNoise=cms.bool(True),CouplingConstantDecOB1=cms.vdouble(0.6871,0.1222,0.0342),CouplingConstantPeakOB2=cms.vdouble(0.8719,0.064),CouplingConstantPeakW2b=cms.vdouble(0.998,0.001),Inefficiency=cms.double(0.0),CouplingConstantPeakW2a=cms.vdouble(1.0,0.0),ZeroSuppression=cms.bool(True),cmnRMStec=cms.double(2.44),CouplingConstantDecIB2=cms.vdouble(0.83,0.0756,0.0094),Noise=cms.bool(True),LorentzAngle=cms.string(''),noDiffusion=cms.bool(False),LandauFluctuations=cms.bool(True),FedAlgorithm=cms.int32(4),DigiModeList=cms.PSet(SCDigi=cms.string('ScopeMode'),ZSDigi=cms.string('ZeroSuppressed'),PRDigi=cms.string('ProcessedRaw'),VRDigi=cms.string('VirginRaw')),Gain=cms.string(''),CouplingConstantDecW1a=cms.vdouble(0.786,0.093,0.014),APVSaturationProb=cms.double(0.001),electronPerAdcPeak=cms.double(262.0),CouplingConstantDecW6=cms.vdouble(0.758,0.093,0.026),CouplingConstantDecW5=cms.vdouble(0.7565,0.0913,0.0304),CouplingConstantDecW4=cms.vdouble(0.876,0.06,0.002),CouplingConstantDecW1b=cms.vdouble(0.822,0.08,0.009),hitsProducer=cms.string('g4SimHits'),PedestalsOffset=cms.double(128),ROUList=cms.vstring('TrackerHitsTIBLowTof','TrackerHitsTIBHighTof','TrackerHitsTIDLowTof','TrackerHitsTIDHighTof','TrackerHitsTOBLowTof','TrackerHitsTOBHighTof','TrackerHitsTECLowTof','TrackerHitsTECHighTof'),CouplingConstantDecIB1=cms.vdouble(0.7748,0.0962,0.0165),APVSaturationFromHIP=cms.bool(True),GevPerElectron=cms.double(3.61e-09),CouplingConstantDecW3a=cms.vdouble(0.8164,0.09,0.0018),chargeDivisionsPerStrip=cms.int32(10),TOFCutForDeconvolution=cms.double(50.0),CouplingConstantPeakW3a=cms.vdouble(0.996,0.002),CouplingConstantDecW7=cms.vdouble(0.7828,0.0862,0.0224),cmnRMStid=cms.double(3.08),CouplingConstantPeakW3b=cms.vdouble(0.992,0.004),AppliedVoltage=cms.double(300.0),CouplingConstantPeakW1b=cms.vdouble(0.976,0.012),CouplingConstantPeakW1a=cms.vdouble(0.996,0.002),NoiseSigmaThreshold=cms.double(2.0),cmnRMStib=cms.double(5.92),CouplingConstantDecW3b=cms.vdouble(0.848,0.06,0.016),ChargeDistributionRMS=cms.double(6.5e-10),TrackerConfigurationFromDB=cms.bool(False),APVpeakmode=cms.bool(False),CosmicDelayShift=cms.untracked.double(0.0),cmnRMStob=cms.double(1.08),accumulatorType=cms.string('SiStripDigitizer'),CouplingConstantPeakW6=cms.vdouble(0.972,0.014),CouplingConstantPeakW5=cms.vdouble(0.968,0.016),CouplingConstantPeakW4=cms.vdouble(0.992,0.004),electronPerAdcDec=cms.double(247.0),GeometryType=cms.string('idealForDigi'),ChargeMobility=cms.double(310.0),CouplingConstantDecOB2=cms.vdouble(0.725,0.1102,0.0273),CommonModeNoise=cms.bool(True)),castor=cms.PSet(hitsProducer=cms.string('g4SimHits'),castor=cms.PSet(readoutFrameSize=cms.int32(6),binOfMaximum=cms.int32(5),samplingFactor=cms.double(0.060135),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(4.009),simHitToPhotoelectrons=cms.double(1000.0),syncPhase=cms.bool(True),timePhase=cms.double(-4.0)),doNoise=cms.bool(True),doTimeSlew=cms.bool(True),accumulatorType=cms.string('CastorDigiProducer'),makeDigiSimLinks=cms.untracked.bool(False)),pixel=cms.PSet(DeltaProductionCut=cms.double(0.03),FPix_SignalResponse_p2=cms.double(93.6),FPix_SignalResponse_p3=cms.double(134.6),FPix_SignalResponse_p0=cms.double(0.0043),FPix_SignalResponse_p1=cms.double(1.31),TofUpperCut=cms.double(12.5),AddNoisyPixels=cms.bool(True),TanLorentzAnglePerTesla_BPix=cms.double(0.106),AddNoise=cms.bool(True),GainSmearing=cms.double(0.0),AddThresholdSmearing=cms.bool(True),useDB=cms.bool(True),AdcFullScale=cms.int32(255),TofLowerCut=cms.double(-12.5),killModules=cms.bool(True),DeadModules=cms.VPSet(cms.PSet(Dead_detID=cms.int32(302055940),Module=cms.string('tbmB')),cms.PSet(Dead_detID=cms.int32(302059800),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302121992),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302123296),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302125060),Module=cms.string('tbmA')),cms.PSet(Dead_detID=cms.int32(302125076),Module=cms.string('tbmA')),cms.PSet(Dead_detID=cms.int32(302126364),Module=cms.string('tbmB')),cms.PSet(Dead_detID=cms.int32(302126596),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302127136),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302188552),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302188824),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302194200),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302195232),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302197252),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302197784),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453892),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453896),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453900),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453904),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454916),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454920),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454924),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454928),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455940),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455944),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455948),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455952),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454148),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454152),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454156),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455172),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455176),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455180),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352456196),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352456200),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352456204),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999748),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999752),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999756),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999760),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344014340),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344014344),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344014348),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344019460),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344019464),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344019468),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077572),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077576),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077580),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077584),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078596),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078600),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078604),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078608),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079620),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079624),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079628),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079632),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078852),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078856),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078860),Module=cms.string('whole'))),TanLorentzAnglePerTesla_FPix=cms.double(0.106),accumulatorType=cms.string('SiPixelDigitizer'),AddPixelInefficiency=cms.int32(0),LorentzAngle_DB=cms.bool(True),makeDigiSimLinks=cms.untracked.bool(True),BPix_SignalResponse_p2=cms.double(97.4),BPix_SignalResponse_p3=cms.double(126.5),BPix_SignalResponse_p0=cms.double(0.0035),Alpha2Order=cms.bool(True),hitsProducer=cms.string('g4SimHits'),ReadoutNoiseInElec=cms.double(350.0),DeadModules_DB=cms.bool(True),ROUList=cms.vstring('TrackerHitsPixelBarrelLowTof','TrackerHitsPixelBarrelHighTof','TrackerHitsPixelEndcapLowTof','TrackerHitsPixelEndcapHighTof'),OffsetSmearing=cms.double(0.0),NoiseInElectrons=cms.double(175.0),ChargeVCALSmearing=cms.bool(True),ElectronsPerVcal=cms.double(65.5),MissCalibrate=cms.bool(True),ThresholdInElectrons_BPix=cms.double(3500.0),ThresholdSmearing_BPix=cms.double(245.0),ThresholdInElectrons_FPix=cms.double(3000.0),ElectronsPerVcal_Offset=cms.double(-414.0),ThresholdSmearing_FPix=cms.double(210.0),ElectronPerAdc=cms.double(135.0),GeometryType=cms.string('idealForDigi'),BPix_SignalResponse_p1=cms.double(1.23)),ecal=cms.PSet(EEdigiCollection=cms.string(''),readoutFrameSize=cms.int32(10),EBdigiCollection=cms.string(''),apdShapeTau=cms.double(40.5),ESdigiCollection=cms.string(''),apdSimToPEHigh=cms.double(88200000.0),doNoise=cms.bool(True),apdTimeOffset=cms.double(-13.5),EBCorrNoiseMatrixG06=cms.vdouble(1.0,0.70946,0.58021,0.49846,0.45006,0.41366,0.39699,0.38478,0.37847,0.37055),EECorrNoiseMatrixG12=cms.vdouble(1.0,0.71373,0.44825,0.30152,0.21609,0.14786,0.11772,0.10165,0.09465,0.08098),doESNoise=cms.bool(True),apdSeparateDigi=cms.bool(True),EBCorrNoiseMatrixG01=cms.vdouble(1.0,0.73354,0.64442,0.58851,0.55425,0.53082,0.51916,0.51097,0.50732,0.50409),applyConstantTerm=cms.bool(True),binOfMaximum=cms.int32(6),EBs25notContainment=cms.double(0.97),accumulatorType=cms.string('EcalDigiProducer'),apdTimeOffWidth=cms.double(0.8),simHitToPhotoelectronsBarrel=cms.double(2250.0),syncPhase=cms.bool(True),doPhotostatistics=cms.bool(True),apdShapeTstart=cms.double(74.5),hitsProducer=cms.string('g4SimHits'),apdDoPEStats=cms.bool(True),ConstantTerm=cms.double(0.003),apdSimToPELow=cms.double(2450000.0),cosmicsPhase=cms.bool(False),apdNonlParms=cms.vdouble(1.48,-3.75,1.81,1.26,2.0,45,1.0),photoelectronsToAnalogEndcap=cms.double(0.000555555),photoelectronsToAnalogBarrel=cms.double(0.000444444),apdDigiTag=cms.string('APD'),EECorrNoiseMatrixG01=cms.vdouble(1.0,0.72698,0.62048,0.55691,0.51848,0.49147,0.47813,0.47007,0.46621,0.46265),apdAddToBarrel=cms.bool(False),EBCorrNoiseMatrixG12=cms.vdouble(1.0,0.71073,0.55721,0.46089,0.40449,0.35931,0.33924,0.32439,0.31581,0.30481),EECorrNoiseMatrixG06=cms.vdouble(1.0,0.71217,0.47464,0.34056,0.26282,0.20287,0.17734,0.16256,0.15618,0.14443),makeDigiSimLinks=cms.untracked.bool(False),simHitToPhotoelectronsEndcap=cms.double(1800.0),samplingFactor=cms.double(1.0),cosmicsShift=cms.double(0.0),doFast=cms.bool(True),EEs25notContainment=cms.double(0.975),timePhase=cms.double(0.0))),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-5),bunchspace=cms.int32(25),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(averageNumber=cms.double(50.0)),sequential=cms.untracked.bool(False),type=cms.string('poisson'),":
           pss = 'PU50bx25'

        if ps == "process.mix=cms.EDProducer(MixingModule,digitizers=cms.PSet(hcal=cms.PSet(HFTuningParameter=cms.double(1.025),HETuningParameter=cms.double(0.9),doHFWindow=cms.bool(False),doNoise=cms.bool(True),hb=cms.PSet(siPMCells=cms.vint32(),readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(True),binOfMaximum=cms.int32(5),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.vdouble(0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305),simHitToPhotoelectrons=cms.double(2000.0),samplingFactors=cms.vdouble(125.44,125.54,125.32,125.13,124.46,125.01,125.22,125.48,124.45,125.9,125.83,127.01,126.82,129.73,131.83,143.52),syncPhase=cms.bool(True),timePhase=cms.double(6.0)),useOldHO=cms.bool(True),useOldHE=cms.bool(True),hoHamamatsu=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(False),siPMCode=cms.int32(2),timePhase=cms.double(5.0),simHitToPhotoelectrons=cms.double(4000.0),binOfMaximum=cms.int32(5),photoelectronsToAnalog=cms.vdouble(3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0),samplingFactors=cms.vdouble(231.0,231.0,231.0,231.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0),syncPhase=cms.bool(True),doPhotoStatistics=cms.bool(True),pixels=cms.int32(960)),useOldHF=cms.bool(True),injectTestHits=cms.bool(False),useOldHB=cms.bool(True),doEmpty=cms.bool(True),he=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(16),timeSmearing=cms.bool(True),binOfMaximum=cms.int32(5),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.vdouble(0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305,0.3305),simHitToPhotoelectrons=cms.double(2000.0),samplingFactors=cms.vdouble(210.55,197.93,186.12,189.64,189.63,190.28,189.61,189.6,190.12,191.22,190.9,193.06,188.42,188.42),syncPhase=cms.bool(True),timePhase=cms.double(6.0)),hf1=cms.PSet(readoutFrameSize=cms.int32(5),binOfMaximum=cms.int32(3),samplingFactor=cms.double(0.383),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(2.79),simHitToPhotoelectrons=cms.double(6.0),syncPhase=cms.bool(True),timePhase=cms.double(14.0)),hf2=cms.PSet(readoutFrameSize=cms.int32(5),binOfMaximum=cms.int32(3),samplingFactor=cms.double(0.368),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(1.843),simHitToPhotoelectrons=cms.double(6.0),syncPhase=cms.bool(True),timePhase=cms.double(13.0)),HBTuningParameter=cms.double(0.875),doThermalNoise=cms.bool(True),accumulatorType=cms.string('HcalDigiProducer'),hoZecotek=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(False),siPMCode=cms.int32(2),timePhase=cms.double(5.0),simHitToPhotoelectrons=cms.double(4000.0),binOfMaximum=cms.int32(5),photoelectronsToAnalog=cms.vdouble(3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0,3.0),samplingFactors=cms.vdouble(231.0,231.0,231.0,231.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0),syncPhase=cms.bool(True),doPhotoStatistics=cms.bool(True),pixels=cms.int32(36000)),hitsProducer=cms.string('g4SimHits'),doTimeSlew=cms.bool(True),ho=cms.PSet(readoutFrameSize=cms.int32(10),firstRing=cms.int32(1),timeSmearing=cms.bool(False),siPMCode=cms.int32(2),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.vdouble(0.24,0.24,0.24,0.24,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17,0.17),binOfMaximum=cms.int32(5),simHitToPhotoelectrons=cms.double(4000.0),samplingFactors=cms.vdouble(231.0,231.0,231.0,231.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0,360.0),syncPhase=cms.bool(True),timePhase=cms.double(5.0)),HOTuningParameter=cms.double(1),doIonFeedback=cms.bool(True),makeDigiSimLinks=cms.untracked.bool(False),doHPDNoise=cms.bool(False),zdc=cms.PSet(readoutFrameSize=cms.int32(10),binOfMaximum=cms.int32(5),samplingFactor=cms.double(1.0),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(1.843),simHitToPhotoelectrons=cms.double(6.0),syncPhase=cms.bool(True),timePhase=cms.double(-4.0))),strip=cms.PSet(CouplingConstantPeakIB1=cms.vdouble(0.9006,0.0497),TOFCutForPeak=cms.double(100.0),DeltaProductionCut=cms.double(0.120425),RealPedestals=cms.bool(True),Temperature=cms.double(273.0),CouplingConstantPeakOB1=cms.vdouble(0.8542,0.0729),DepletionVoltage=cms.double(170.0),CouplingConstantDecW2b=cms.vdouble(0.888,0.05,0.006),CouplingConstantPeakIB2=cms.vdouble(0.9342,0.0328),CouplingConstantDecW2a=cms.vdouble(0.7962,0.0914,0.0104),CouplingConstantPeakW7=cms.vdouble(0.964,0.018),BaselineShift=cms.bool(True),SingleStripNoise=cms.bool(True),CouplingConstantDecOB1=cms.vdouble(0.6871,0.1222,0.0342),CouplingConstantPeakOB2=cms.vdouble(0.8719,0.064),CouplingConstantPeakW2b=cms.vdouble(0.998,0.001),Inefficiency=cms.double(0.0),CouplingConstantPeakW2a=cms.vdouble(1.0,0.0),ZeroSuppression=cms.bool(True),cmnRMStec=cms.double(2.44),CouplingConstantDecIB2=cms.vdouble(0.83,0.0756,0.0094),Noise=cms.bool(True),LorentzAngle=cms.string(''),noDiffusion=cms.bool(False),LandauFluctuations=cms.bool(True),FedAlgorithm=cms.int32(4),DigiModeList=cms.PSet(SCDigi=cms.string('ScopeMode'),ZSDigi=cms.string('ZeroSuppressed'),PRDigi=cms.string('ProcessedRaw'),VRDigi=cms.string('VirginRaw')),Gain=cms.string(''),CouplingConstantDecW1a=cms.vdouble(0.786,0.093,0.014),APVSaturationProb=cms.double(0.001),electronPerAdcPeak=cms.double(262.0),CouplingConstantDecW6=cms.vdouble(0.758,0.093,0.026),CouplingConstantDecW5=cms.vdouble(0.7565,0.0913,0.0304),CouplingConstantDecW4=cms.vdouble(0.876,0.06,0.002),CouplingConstantDecW1b=cms.vdouble(0.822,0.08,0.009),hitsProducer=cms.string('g4SimHits'),PedestalsOffset=cms.double(128),ROUList=cms.vstring('TrackerHitsTIBLowTof','TrackerHitsTIBHighTof','TrackerHitsTIDLowTof','TrackerHitsTIDHighTof','TrackerHitsTOBLowTof','TrackerHitsTOBHighTof','TrackerHitsTECLowTof','TrackerHitsTECHighTof'),CouplingConstantDecIB1=cms.vdouble(0.7748,0.0962,0.0165),APVSaturationFromHIP=cms.bool(True),GevPerElectron=cms.double(3.61e-09),CouplingConstantDecW3a=cms.vdouble(0.8164,0.09,0.0018),chargeDivisionsPerStrip=cms.int32(10),TOFCutForDeconvolution=cms.double(50.0),CouplingConstantPeakW3a=cms.vdouble(0.996,0.002),CouplingConstantDecW7=cms.vdouble(0.7828,0.0862,0.0224),cmnRMStid=cms.double(3.08),CouplingConstantPeakW3b=cms.vdouble(0.992,0.004),AppliedVoltage=cms.double(300.0),CouplingConstantPeakW1b=cms.vdouble(0.976,0.012),CouplingConstantPeakW1a=cms.vdouble(0.996,0.002),NoiseSigmaThreshold=cms.double(2.0),cmnRMStib=cms.double(5.92),CouplingConstantDecW3b=cms.vdouble(0.848,0.06,0.016),ChargeDistributionRMS=cms.double(6.5e-10),TrackerConfigurationFromDB=cms.bool(False),APVpeakmode=cms.bool(False),CosmicDelayShift=cms.untracked.double(0.0),cmnRMStob=cms.double(1.08),accumulatorType=cms.string('SiStripDigitizer'),CouplingConstantPeakW6=cms.vdouble(0.972,0.014),CouplingConstantPeakW5=cms.vdouble(0.968,0.016),CouplingConstantPeakW4=cms.vdouble(0.992,0.004),electronPerAdcDec=cms.double(247.0),GeometryType=cms.string('idealForDigi'),ChargeMobility=cms.double(310.0),CouplingConstantDecOB2=cms.vdouble(0.725,0.1102,0.0273),CommonModeNoise=cms.bool(True)),castor=cms.PSet(hitsProducer=cms.string('g4SimHits'),castor=cms.PSet(readoutFrameSize=cms.int32(6),binOfMaximum=cms.int32(5),samplingFactor=cms.double(0.060135),doPhotoStatistics=cms.bool(True),photoelectronsToAnalog=cms.double(4.009),simHitToPhotoelectrons=cms.double(1000.0),syncPhase=cms.bool(True),timePhase=cms.double(-4.0)),doNoise=cms.bool(True),doTimeSlew=cms.bool(True),accumulatorType=cms.string('CastorDigiProducer'),makeDigiSimLinks=cms.untracked.bool(False)),pixel=cms.PSet(DeltaProductionCut=cms.double(0.03),FPix_SignalResponse_p2=cms.double(93.6),FPix_SignalResponse_p3=cms.double(134.6),FPix_SignalResponse_p0=cms.double(0.0043),FPix_SignalResponse_p1=cms.double(1.31),TofUpperCut=cms.double(12.5),AddNoisyPixels=cms.bool(True),TanLorentzAnglePerTesla_BPix=cms.double(0.106),AddNoise=cms.bool(True),GainSmearing=cms.double(0.0),AddThresholdSmearing=cms.bool(True),useDB=cms.bool(True),AdcFullScale=cms.int32(255),TofLowerCut=cms.double(-12.5),killModules=cms.bool(True),DeadModules=cms.VPSet(cms.PSet(Dead_detID=cms.int32(302055940),Module=cms.string('tbmB')),cms.PSet(Dead_detID=cms.int32(302059800),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302121992),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302123296),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302125060),Module=cms.string('tbmA')),cms.PSet(Dead_detID=cms.int32(302125076),Module=cms.string('tbmA')),cms.PSet(Dead_detID=cms.int32(302126364),Module=cms.string('tbmB')),cms.PSet(Dead_detID=cms.int32(302126596),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302127136),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302188552),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302188824),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302194200),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302195232),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302197252),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(302197784),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453892),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453896),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453900),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352453904),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454916),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454920),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454924),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454928),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455940),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455944),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455948),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455952),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454148),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454152),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352454156),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455172),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455176),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352455180),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352456196),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352456200),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(352456204),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999748),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999752),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999756),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(343999760),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344014340),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344014344),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344014348),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344019460),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344019464),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344019468),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077572),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077576),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077580),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344077584),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078596),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078600),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078604),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078608),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079620),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079624),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079628),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344079632),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078852),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078856),Module=cms.string('whole')),cms.PSet(Dead_detID=cms.int32(344078860),Module=cms.string('whole'))),TanLorentzAnglePerTesla_FPix=cms.double(0.106),accumulatorType=cms.string('SiPixelDigitizer'),AddPixelInefficiency=cms.int32(0),LorentzAngle_DB=cms.bool(True),makeDigiSimLinks=cms.untracked.bool(True),BPix_SignalResponse_p2=cms.double(97.4),BPix_SignalResponse_p3=cms.double(126.5),BPix_SignalResponse_p0=cms.double(0.0035),Alpha2Order=cms.bool(True),hitsProducer=cms.string('g4SimHits'),ReadoutNoiseInElec=cms.double(350.0),DeadModules_DB=cms.bool(True),ROUList=cms.vstring('TrackerHitsPixelBarrelLowTof','TrackerHitsPixelBarrelHighTof','TrackerHitsPixelEndcapLowTof','TrackerHitsPixelEndcapHighTof'),OffsetSmearing=cms.double(0.0),NoiseInElectrons=cms.double(175.0),ChargeVCALSmearing=cms.bool(True),ElectronsPerVcal=cms.double(65.5),MissCalibrate=cms.bool(True),ThresholdInElectrons_BPix=cms.double(3500.0),ThresholdSmearing_BPix=cms.double(245.0),ThresholdInElectrons_FPix=cms.double(3000.0),ElectronsPerVcal_Offset=cms.double(-414.0),ThresholdSmearing_FPix=cms.double(210.0),ElectronPerAdc=cms.double(135.0),GeometryType=cms.string('idealForDigi'),BPix_SignalResponse_p1=cms.double(1.23)),ecal=cms.PSet(EEdigiCollection=cms.string(''),readoutFrameSize=cms.int32(10),EBdigiCollection=cms.string(''),apdShapeTau=cms.double(40.5),ESdigiCollection=cms.string(''),apdSimToPEHigh=cms.double(88200000.0),doNoise=cms.bool(True),apdTimeOffset=cms.double(-13.5),EBCorrNoiseMatrixG06=cms.vdouble(1.0,0.70946,0.58021,0.49846,0.45006,0.41366,0.39699,0.38478,0.37847,0.37055),EECorrNoiseMatrixG12=cms.vdouble(1.0,0.71373,0.44825,0.30152,0.21609,0.14786,0.11772,0.10165,0.09465,0.08098),doESNoise=cms.bool(True),apdSeparateDigi=cms.bool(True),EBCorrNoiseMatrixG01=cms.vdouble(1.0,0.73354,0.64442,0.58851,0.55425,0.53082,0.51916,0.51097,0.50732,0.50409),applyConstantTerm=cms.bool(True),binOfMaximum=cms.int32(6),EBs25notContainment=cms.double(0.97),accumulatorType=cms.string('EcalDigiProducer'),apdTimeOffWidth=cms.double(0.8),simHitToPhotoelectronsBarrel=cms.double(2250.0),syncPhase=cms.bool(True),doPhotostatistics=cms.bool(True),apdShapeTstart=cms.double(74.5),hitsProducer=cms.string('g4SimHits'),apdDoPEStats=cms.bool(True),ConstantTerm=cms.double(0.003),apdSimToPELow=cms.double(2450000.0),cosmicsPhase=cms.bool(False),apdNonlParms=cms.vdouble(1.48,-3.75,1.81,1.26,2.0,45,1.0),photoelectronsToAnalogEndcap=cms.double(0.000555555),photoelectronsToAnalogBarrel=cms.double(0.000444444),apdDigiTag=cms.string('APD'),EECorrNoiseMatrixG01=cms.vdouble(1.0,0.72698,0.62048,0.55691,0.51848,0.49147,0.47813,0.47007,0.46621,0.46265),apdAddToBarrel=cms.bool(False),EBCorrNoiseMatrixG12=cms.vdouble(1.0,0.71073,0.55721,0.46089,0.40449,0.35931,0.33924,0.32439,0.31581,0.30481),EECorrNoiseMatrixG06=cms.vdouble(1.0,0.71217,0.47464,0.34056,0.26282,0.20287,0.17734,0.16256,0.15618,0.14443),makeDigiSimLinks=cms.untracked.bool(False),simHitToPhotoelectronsEndcap=cms.double(1800.0),samplingFactor=cms.double(1.0),cosmicsShift=cms.double(0.0),doFast=cms.bool(True),EEs25notContainment=cms.double(0.975),timePhase=cms.double(0.0))),mixProdStep2=cms.bool(False),maxBunch=cms.int32(3),useCurrentProcessOnly=cms.bool(False),LabelPlayback=cms.string(''),minBunch=cms.int32(-5),bunchspace=cms.int32(50),playback=cms.untracked.bool(False),input=cms.SecSource(PoolSource,nbPileupEvents=cms.PSet(averageNumber=cms.double(35.0)),sequential=cms.untracked.bool(False),type=cms.string('poisson'),":
           pss = 'PU35'

        return pss

def getPileupDataset(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/view/showWorkload?requestName='+workflow)
        r2=conn.getresponse()
        workload=r2.read()
        list = workload.split('\n')

        pileupDataset = 'None'

        for line in list:
           if 'request.schema.MCPileup' in line:
              pileupDataset = line[line.find("[")+1:line.find("]")]

        return pileupDataset


def getPriority(url, workflow): 
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/view/showWorkload?requestName='+workflow)
        r2=conn.getresponse()
        workload=r2.read()
        list = workload.split('\n')
              
        priority = -1 
        
        for line in list:
           if 'request.schema.RequestPriority' in line:
              priority = line[line.find("=")+1:line.find("<br/")]

        priority = priority.strip()
        priority = re.sub(r'\'', '', priority)
        return priority

def getInputDataSet(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.read(r2.read())
        inputDataSets=request['InputDataset']
        if len(inputDataSets)<1:
                print "ERROR: No InputDataSet for workflow"
        else:   
                return inputDataSets

def findCustodialLocation(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
        r2=conn.getresponse()
        result = json.read(r2.read())
        request=result['phedex']
        if 'block' not in request.keys():
                return "No Site"
        if len(request['block'])==0:
                return "No Site"
        for replica in request['block'][0]['replica']:
                if replica['custodial']=="y" and replica['node']!="T0_CH_CERN_MSS":
                        return replica['node']
        return "None"

def getPrepID(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.read(r2.read())
        prepID=request['PrepID']
        return prepID

def getGlobalTag(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.read(r2.read())
        globalTag=request['GlobalTag']
        return globalTag

def getPileupScenario(url, workflow):
        cacheID = getCacheID(url, workflow)
        config = getConfig(url, cacheID)
        pileup = getPileup(config)
        scenario = getScenario(pileup)
        return scenario

def getPileup(config):
        lines = config.split('\n')
        want = ''
        i=0
        for line in lines:
           if 'MixingModule' in line and 'EDProducer' in line:
              i=1
           if 'fileNames' in line:
              i=0
           if i == 1:
              want = want + line

        want = re.sub(r'\s+', '', want)
        want = re.sub(r'\"', '', want)
        want = re.sub(r'\n', '', want)
           
        pu = want
        return pu

def getCacheID(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.read(r2.read())
        cacheID=request['StepOneConfigCacheID']
        return cacheID

def getConfig(url, cacheID):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/couchdb/reqmgr_config_cache/'+cacheID+'/configFile')
        r2=conn.getresponse()
        config = r2.read()
        return config

def assignRequest(url,workflow,team,site,era,procversion, activity, lfn, maxmergeevents, maxRSS, maxVSize):
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "CustodialSites": site,
              "Priority" : "Normal",
              #"Memory": 2300.0,
              #"SizePerEvent": 342110,
              #"TimePerEvent": 17.5,
              "SoftTimeout": 167000,
              "GracePeriod": 300,
              "MaxMergeEvents": maxmergeevents,
	      "maxRSS": maxRSS,
              "maxVSize": maxVSize,
              "AcquisitionEra": era,
	      "dashboard": activity,
              "ProcessingVersion": procversion,
              "checkbox"+workflow: "checked"}

    encodedParams = urllib.urlencode(params, True)

    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("POST",  "/reqmgr/assign/handleAssignmentPage", encodedParams, headers)
    response = conn.getresponse()
    if response.status != 200:
        print 'could not assign request with following parameters:'
        for item in params.keys():
            print item + ": " + str(params[item])
        print 'Response from http call:'
        print 'Status:',response.status,'Reason:',response.reason
        print 'Explanation:'
        data = response.read()
        print data
        print "Exiting!"
  	sys.exit(1)
    conn.close()
    print 'Assigned workflow:',workflow,'to site:',site,'acquisition era:',era,'team',team,'processing version:',procversion,'lfn:',lfn,'maxmergeevents:',maxmergeevents,'maxRSS:',maxRSS,'maxVSize:',maxVSize
    return



def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-f', '--filename', help='Filename',dest='filename')
	parser.add_option('-t', '--team', help='Type of Requests',dest='team')
	parser.add_option('-s', '--site', help='Force workflow to run at this site',dest='site')
	parser.add_option('-p', '--procversion', help='Processing Version',dest='procversion')
	parser.add_option('-n', '--procstring', help='Process String',dest='procstring')
	parser.add_option('-e', '--execute', help='Actually assign workflows',action="store_true",dest='execute')
	parser.add_option('-x', '--restrict', help='Only assign workflows for this site',dest='restrict')
	parser.add_option('-r', '--rssmax', help='Max RSS',dest='maxRSS')
	parser.add_option('-v', '--vsizemax', help='Max VMem',dest='maxVSize')
	parser.add_option('-a', '--extension', help='Use _ext special name',action="store_true",dest='extension')
	(options,args) = parser.parse_args()
	if not options.filename:
		print "A filename is required"
		sys.exit(0)
	activity='reprocessing'
        if not options.restrict:
                restrict='None'
        else:
                restrict=options.restrict
        maxRSS = 2300000
        if not options.maxRSS:
                maxRSS = 2300000
        else:
                maxRSS=options.maxRSS
        maxVSize = 4100000000
        if not options.maxVSize:
                maxVSize = 4100000000
        else:
                maxVSize=options.maxVSize
	filename=options.filename

        # Valid Tier-1 sites
        sites = ['T1_DE_KIT', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_ES_PIC', 'T1_TW_ASGC', 'T1_UK_RAL', 'T1_US_FNAL']

        f=open(filename,'r')
        for workflow in f:
           workflow = workflow.rstrip('\n')
           siteUse=options.site
           team=options.team
           procversion=options.procversion

           inputDataset = getInputDataSet(url, workflow)

           # Check status of input dataset
           inputDatasetStatus = getStatusDataSet(inputDataset)
           if inputDatasetStatus != 'VALID' and inputDatasetStatus != 'PRODUCTION':
              print 'ERROR: Input dataset is not PRODUCTION or VALID'
              sys.exit(0)

           if not siteUse or siteUse == 'None':
              # Determine site where workflow should be run
              count=0
              for site in sites:
                 if site in workflow:
                    count=count+1
                    siteUse = site

              # Find custodial location of input dataset if workflow name contains no T1 site or multiple T1 sites
              if count==0 or count>1:
                 siteUse = findCustodialLocation(url, inputDataset)
                 if siteUse == 'None':
                    print 'ERROR: No custodial site found'
                    sys.exit(0)
                 siteUse = siteUse[:-4]

           # Extract required part of global tag
           gtRaw = getGlobalTag(url, workflow)
           gtBits = gtRaw.split('::')
           globalTag = gtBits[0]
         
           # Determine pileup scenario
           # - Fall11_R2 & Fall11_R4 don't add pileup so extract pileup scenario from input
           pileupDataset = getPileupDataset(url, workflow)
           pileupScenario = getPileupScenario(url, workflow)
           if pileupScenario == 'Unknown' and 'MinBias' in pileupDataset:
              print 'ERROR: unable to determine pileup scenario'
              sys.exit(0)
           elif 'Fall11_R2' in workflow or 'Fall11_R4' in workflow:
              inDataSet = getInputDataSet(url, workflow)
              matchObj = re.match(r".*Fall11-(.*)_START.*", inDataSet)
              if matchObj:
                 pileupScenario = matchObj.group(1)
              else:
                 pileupScenario == 'Unknown'
           elif pileupScenario == 'Unknown' and 'MinBias' not in pileupDataset:
              pileupScenario = 'NoPileUp'

           # Decide which team to use if not already defined
           if not team:
              priority = int(getPriority(url, workflow))
              if priority < 100000:
                 team = 'processing'
              else:
                 team = 't1'

           specialName = ''

           # Set era, lfn and campaign-dependent part of name if necessary
           if 'Summer12_DR51X' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'

           if 'Summer12_DR52X' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'

           if 'Summer12_DR53X' in workflow:
              era = 'Summer12_DR53X'
              lfn = '/store/mc'

           if 'Fall11_R' in workflow:
              era = 'Fall11'
              lfn = '/store/mc'

           if 'Summer11_R' in workflow:
              era = 'Summer11'
              lfn = '/store/mc'

           if 'LowPU2010_DR42' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'
              specialName = 'LowPU2010_DR42_'

           if 'UpgradeL1TDR_DR6X' in workflow:
              era = 'Summer12'
              lfn = '/store/mc'

           # Construct processed dataset version
           if options.procstring:
              specialName = options.procstring + '_'
           extTag = ''
           if options.extension:
              extTag = '_ext'
           if not procversion:
              procversion = specialName+pileupScenario+'_'+globalTag+extTag+'-v'
              iVersion = getDatasetVersion(url, workflow, era, procversion)
              procversion = procversion+str(iVersion)

           # Set max number of merge events
           maxmergeevents = 50000
           if 'Fall11_R1' in workflow:
              maxmergeevents = 6000

           # Checks
           if not era:
              print 'ERROR: era is not defined'
              sys.exit(0)

           if not lfn:
              print 'ERROR: lfn is not defined'
              sys.exit(0)

           if siteUse not in sites:
              print 'ERROR: invalid site'
              sys.exit(0)

           if pileupScenario == 'Unknown':
              print 'ERROR: unable to determine pileup scenario'
              sys.exit(0)

           if options.execute:
              if restrict == 'None' or restrict == siteUse:
	         assignRequest(url,workflow,team,siteUse,era,procversion, activity, lfn, maxmergeevents, maxRSS, maxVSize)
              else:
                 print 'Skipping workflow ',workflow
           else:
              if restrict == 'None' or restrict == siteUse:
                 print 'Would assign ',workflow,' with ','acquisition era:',era,'version:',procversion,'lfn:',lfn,'site:',siteUse,'team:',team,'maxmergeevents:',maxmergeevents
              else:
                 print 'Would skip workflow ',workflow

	sys.exit(0)

if __name__ == "__main__":
	main()
