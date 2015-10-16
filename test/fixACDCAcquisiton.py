from __future__ import print_function
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

def getWorkloadFromSpec(baseUrl, db, request):
    wh = WMWorkloadHelper()
    reqmgrSpecUrl = "%s/%s/%s/spec" % (baseUrl, db, request)
    wh.load(reqmgrSpecUrl)
    print (wh.name())
    #for task in wh.getTopLevelTask():
    for taskPath in wh.listAllTaskPathNames():
        #print (taskPath) 
        task = wh.getTaskByPath(taskPath)
        #print ("%s: %s : %s :%s :%s" % (task.name(), task.taskType(), task.getAcquisitionEra(), 
        #                        task.getProcessingVersion(), task.getProcessingString()))
        
    return wh

if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch/couchdb"
    for db in ["reqmgr_workload_cache"]:
        #wh = getWorkloadFromSpec(baseUrl, db, "jbadillo_ACDC_task_B2G-RunIIWinter15wmLHE-00388__v1_T_150728_121213_4152")
        #print(wh.listAllTaskPathNames())
        wh = getWorkloadFromSpec(baseUrl, db, "pdmvserv_task_B2G-RunIIWinter15wmLHE-00388__v1_T_150721_114715_2170")
        #task = wh.getTaskByPath("B2G-RunIIWinter15wmLHE-00388_0MergeLHEoutput")
        #print ("%s: %s : %s" % (task.name(), task.taskType(), task.getAcquisitionEra()))
        #print(wh.listAllTaskPathNames())
        count = 0
        for taskPath in wh.listAllTaskPathNames():
            count += 1
        print(count)
        print(wh.getAcquisitionEra())
        wh.truncate("test_merge", 
                    "/pdmvserv_task_B2G-RunIIWinter15wmLHE-00388__v1_T_150721_114715_2170/B2G-RunIIWinter15wmLHE-00388_0/B2G-RunIIWinter15wmLHE-00388_0MergeLHEoutput",
                    "fake", "fake")
        count = 0
        for taskPath in wh.listAllTaskPathNames():
            print (taskPath) 
            task = wh.getTaskByPath(taskPath)
            print ("%s: %s : %s :%s :%s" % (task.name(), task.taskType(), task.getAcquisitionEra(), 
                                    task.getProcessingVersion(), task.getProcessingString()))
            count += 1
        print(count)
        print(wh.getAcquisitionEra())
        #for taskName in wh.listAllTaskNames():
        #    task = wh.getTask(taskName)
        #    print ("%s: %s : %s :%s :%s" % (task.name(), task.taskType(), task.getAcquisitionEra(), 
        #                        task.getProcessingVersion(), task.getProcessingString()))
        