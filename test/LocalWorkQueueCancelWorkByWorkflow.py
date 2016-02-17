#!/usr/bin/env python
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
    #pprint(workqueue.status("Running"))
    #pdb.set_trace()
    elements = workqueue.cancelWork(WorkflowName="dmason_BoogaBoogaBooga_151219_064215_1624")
    pprint(elements)
    results = workqueue.backend.updateElements(*elements, Status = "Canceled")
    pprint(results)
    #workqueue.performQueueCleanupActions()
~                                             