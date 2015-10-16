#!/usr/bin/env python
jobSlots = {'T1_IT_CNAF': 1200.0, 'T2_CH_CERN_T0': 180.0, 'T2_BR_UERJ': 60.0, 'T2_DE_DESY': 420.0, 'T2_AT_Vienna': 50.0, 
            'T2_CH_CSCS': 168.0, 'T2_UA_KIPT': 50.0, 'T2_US_Purdue': 619.0, 'T2_FI_HIP': 50.0, 'T2_UK_SGrid_RALPP': 50.0, 
            'T2_FR_GRIF_LLR': 180.0, 'T2_BE_UCL': 50.0, 'T2_ES_IFCA': 64.0, 'T2_DE_RWTH': 144.0, 'T2_UK_SGrid_Bristol': 50.0, 
            'T2_FR_IPHC': 72.0, 'T2_US_Caltech': 150.0, 'T2_KR_KNU': 50.0, 'T1_ES_PIC': 144.0, 'T1_UK_RAL': 204.0, 
            'T1_US_FNAL': 1356.0, 'T2_IT_Legnaro': 144.0, 'T2_CH_CERN_AI': 1500.0, 'T2_UK_London_Brunel': 72.0, 
            'T3_US_Colorado': 144.0, 'T2_RU_JINR': 50.0, 'T2_IT_Pisa': 129.0, 'T2_US_Vanderbilt': 79.0, 'T2_GR_Ioannina': 50.0, 
            'T2_IN_TIFR': 50.0, 'T1_DE_KIT': 240.0, 'T2_CN_Beijing': 50.0, 'T1_FR_CCIN2P3': 360.0, 'T2_US_Florida': 300.0, 
            'T2_TH_CUNSTDA': 50.0, 'T2_FR_GRIF_IRFU': 180.0, 'T2_EE_Estonia': 120.0, 'T2_UK_London_IC': 144.0, 'T2_IT_Bari': 136.0, 
            'T2_US_Nebraska': 300.0, 'T2_IT_Rome': 129.0, 'T2_FR_CCIN2P3': 96.0, 'T2_US_UCSD': 660.0, 'T2_CH_CERN_HLT': 960.0, 
            'T2_ES_CIEMAT': 96.0, 'T2_RU_IHEP': 84.0, 'T2_US_Wisconsin': 318.0, 'T2_HU_Budapest': 50.0, 'T2_US_MIT': 384.0, 
            'T2_BE_IIHE': 72.0, 'T2_CH_CERN': 240.0, 'T2_PT_NCG_Lisbon': 78.0, 'T2_PL_Swierk': 50.0, 'T1_RU_JINR': 144.0, 
            'T2_BR_SPRACE': 54.0}
siteJobCounts = {'T1_IT_CNAF': {85000: 0, 0: 0}, 'T2_CH_CERN_T0': {0: 0}, 'T2_BR_UERJ': {0: 0}, 'T2_DE_DESY': {0: 0}, 
                'T2_AT_Vienna': {0: 0}, 'T2_CH_CSCS': {0: 0}, 'T2_UA_KIPT': {0: 0}, 'T2_US_Purdue': {0: 0}, 
                'T2_FI_HIP': {0: 0}, 'T2_UK_SGrid_RALPP': {0: 0}, 'T2_FR_GRIF_LLR': {0: 0}, 
                'T2_BE_UCL': {0: 0}, 'T2_ES_IFCA': {0: 0}, 'T2_DE_RWTH': {90000: 0, 0: 0}, 
                'T2_UK_SGrid_Bristol': {0: 0}, 'T2_FR_IPHC': {0: 0}, 'T2_US_Caltech': {90000: 0, 0: 0}, 
                'T2_KR_KNU': {0: 0}, 'T1_ES_PIC': {0: 0}, 'T1_UK_RAL': {80000: 0, 0: 0}, 'T1_US_FNAL': {0: 0}, 
                'T2_IT_Legnaro': {0: 0}, 'T2_CH_CERN_AI': {0: 0}, 'T2_UK_London_Brunel': {0: 0}, 'T3_US_Colorado': {0: 0}, 
                'T2_RU_JINR': {0: 0}, 'T2_IT_Pisa': {90000: 0, 80000: 0, 85000: 0, 0: 0}, 'T2_US_Vanderbilt': {85000: 0, 0: 0}, 
                'T2_GR_Ioannina': {0: 0}, 'T2_IN_TIFR': {0: 0}, 'T1_DE_KIT': {0: 0}, 'T2_CN_Beijing': {0: 0}, 
                'T1_FR_CCIN2P3': {80000: 0, 0: 0}, 'T2_US_Florida': {90000: 0, 0: 0}, 'T2_TH_CUNSTDA': {0: 0}, 
                'T2_FR_GRIF_IRFU': {0: 0}, 'T2_EE_Estonia': {0: 0}, 'T2_UK_London_IC': {0: 0}, 'T2_IT_Bari': {0: 0}, 
                'T2_US_Nebraska': {0: 0}, 'T2_IT_Rome': {0: 0}, 'T2_FR_CCIN2P3': {0: 0}, 'T2_US_UCSD': {0: 0}, 
                'T2_CH_CERN_HLT': {0: 0}, 'T2_ES_CIEMAT': {0: 0}, 'T2_RU_IHEP': {0: 0}, 'T2_US_Wisconsin': {0: 0}, 
                'T2_HU_Budapest': {0: 0}, 'T2_US_MIT': {85000: 0, 0: 0}, 'T2_BE_IIHE': {0: 0}, 'T2_CH_CERN': {85000: 0, 0: 0}, 
                'T2_PT_NCG_Lisbon': {0: 0},'T2_PL_Swierk': {0: 0}, 'T1_RU_JINR': {0: 0}, 'T2_BR_SPRACE': {80000: 0, 0: 0}}

import os
from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
from WMCore.Configuration import loadConfigurationFile
from pprint import pprint

def createWorkQueue(config):
    """Create a workqueue from wmagent config"""
    # if config has a db sction instantiate a dbi
    if hasattr(config, "CoreDatabase"):
        from WMCore.WMInit import WMInit
        wmInit = WMInit()
        (dialect, junk) = config.CoreDatabase.connectUrl.split(":", 1)
        socket = getattr(config.CoreDatabase, "socket", None)
        wmInit.setDatabaseConnection(dbConfig = config.CoreDatabase.connectUrl,
                                     dialect = dialect,
                                     socketLoc = socket)
    return queueFromConfig(config)

if __name__ == "__main__":
    
    cfgObject = loadConfigurationFile(os.environ.get("WMAGENT_CONFIG", None))

    workqueue = createWorkQueue(cfgObject)
    pprint workqueue.backend.availableWork(jobSlots, siteJobCounts)
    