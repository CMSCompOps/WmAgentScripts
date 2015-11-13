# Auto generated configuration file
# using: 
# Revision: 1.372.2.25 
# Source: /local/reps/CMSSW/CMSSW/Configuration/PyReleaseValidation/python/ConfigBuilder.py,v 
# with command line options: Configuration/GenProduction/python/W1JetsToLNu_8TeV_madgraph_cff.py -s LHE --conditions MC_52_V10::All --datatier LHE --eventcontent LHE -n 100000 --no_exec
import FWCore.ParameterSet.Config as cms

process = cms.Process('LHE')

# import of standard configurations
process.load('Configuration.StandardSequences.Services_cff')
process.load('SimGeneral.HepPDTESSource.pythiapdt_cfi')
process.load('FWCore.MessageService.MessageLogger_cfi')
process.load('Configuration.EventContent.EventContent_cff')
process.load('SimGeneral.MixingModule.mixNoPU_cfi')
process.load('Configuration.StandardSequences.GeometryDB_cff')
process.load('Configuration.StandardSequences.MagneticField_38T_cff')
process.load('Configuration.StandardSequences.EndOfProcess_cff')
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(100000)
)

# Input source
process.source = cms.Source("EmptySource")

process.options = cms.untracked.PSet(

)

# Production Info
process.configurationMetadata = cms.untracked.PSet(
    version = cms.untracked.string('$Revision: 1.372.2.25 $'),
    annotation = cms.untracked.string('Configuration/GenProduction/python/W1JetsToLNu_8TeV_madgraph_cff.py nevts:100000'),
    name = cms.untracked.string('PyReleaseValidation')
)

# Output definition

process.LHEoutput = cms.OutputModule("PoolOutputModule",
    splitLevel = cms.untracked.int32(0),
    eventAutoFlushCompressedSize = cms.untracked.int32(5242880),
    outputCommands = process.LHEEventContent.outputCommands,
    fileName = cms.untracked.string('W1JetsToLNu_8TeV_madgraph_cff_py_LHE.root'),
    dataset = cms.untracked.PSet(
        filterName = cms.untracked.string(''),
        dataTier = cms.untracked.string('GEN')
    )
)

# Additional output definition

# Other statements
process.GlobalTag.globaltag = 'MC_52_V10::All'

process.externalLHEProducer = cms.EDProducer("ExternalLHEProducer",
    nEvents = cms.uint32(100000),
    args = cms.vstring('slc5_ia32_gcc434/madgraph/V5_1.3.30/8TeV_Summer12/W1JetsToLNu_8TeV-madgraph/v2', 
        'W1JetsToLNu_8TeV-madgraph', 
        'false', 
        'true', 
        'wjets', 
        '5', 
        '20', 
        'true', 
        '1', 
        '99'),
    scriptName = cms.FileInPath('GeneratorInterface/LHEInterface/data/run_madgraph_gridpack.sh'),
    outputFile = cms.string('W1JetsToLNu_8TeV-madgraph_final.lhe')
)


# Path and EndPath definitions
process.lhe_step = cms.Path(process.externalLHEProducer)
process.endjob_step = cms.EndPath(process.endOfProcess)
process.LHEoutput_step = cms.EndPath(process.LHEoutput)

# Schedule definition
process.schedule = cms.Schedule(process.lhe_step,process.endjob_step,process.LHEoutput_step)


