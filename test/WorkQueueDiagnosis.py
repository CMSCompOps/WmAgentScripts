from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
import time
from WMCore.WorkQueue.Policy.Start import startPolicy


params = {}
params.setdefault('SplittingMapping', {})
params['SplittingMapping'].setdefault('DatasetBlock',
                                                   {'name': 'Block',
                                                    'args': {}}
                                                  )
params['SplittingMapping'].setdefault('MonteCarlo',
                                           {'name': 'MonteCarlo',
                                            'args':{}}
                                           )
params['SplittingMapping'].setdefault('Dataset',
                                           {'name': 'Dataset',
                                            'args': {}}
                                          )
params['SplittingMapping'].setdefault('Block',
                                           {'name': 'Block',
                                            'args': {}}
                                          )
params['SplittingMapping'].setdefault('ResubmitBlock',
                                           {'name': 'ResubmitBlock',
                                            'args': {}}
                                          )

couchurl = "https://cmsweb.cern.ch/couchdb"
dbname = "workqueue"
inboxname = "workqueue_inbox"
backend = WorkQueueBackend(couchurl, dbname, inboxname)
#data = backend.getInboxElements(OpenForNewData = True) 
# for item in data:
#     if (item['RequestName'] == 'fabozzi_RVCMSSW_7_2_0_pre3ADDMonoJet_d3MD3_13__CondDBv2_140731_132018_1268'):
#     #if (item['RequestName'] == 'jbadillo_ACDC_HIG-Summer12-02187_00158_v0__140728_102627_3527'):
#         workflowsToCheck = [item]
#         for key, value in item.items():
#             print "%s: %s" % (key, value)
print backend.isAvailable()

workflowsToCheck = backend.getInboxElements(OpenForNewData = True)
print "workflows to check"
print len(workflowsToCheck)
workflowsToClose = []
currentTime = time.time()
workflowsToCloseTemp = []
#import pdb
#pdb.set_trace()
#workflowsToCheck = ["franzoni_RVCMSSW_7_2_0TTbar_13_PU50ns__Phys14-TPfix-pess_141217_190520_4888"]
for element in workflowsToCheck:
    # Easy check, close elements with no defined OpenRunningTimeout
    policy = element.get('StartPolicy', {})
    openRunningTimeout = policy.get('OpenRunningTimeout', 0)
    if not openRunningTimeout:
        # Closing, no valid OpenRunningTimeout available
        workflowsToClose.append(element.id)
        continue
  
    # Check if new data is currently available
    skipElement = False
    wqdb = backend.db.name
    #backend.db.name = "reqmgr_workload_cache"
    try:
        spec = backend.getWMSpec(element.id)
    except Exception, ex:
        print "%s" % str(ex)
        print "no spec in the backend: %s" % element.id
        continue
    #backend.db.name = wqdb
    for topLevelTask in spec.taskIterator():
        policyName = spec.startPolicy()
        if not policyName:
            raise RuntimeError("WMSpec doesn't define policyName, current value: '%s'" % policyName)
  
        policyInstance = startPolicy(policyName, params['SplittingMapping'])
        if not policyInstance.supportsWorkAddition():
            continue
        #if policyInstance.newDataAvailable(topLevelTask, element):
            #skipElement = True
            #backend.updateInboxElements(element.id, TimestampFoundNewData = currentTime)
            break
    if skipElement:
        continue
     
    # Check if the delay has passed
    newDataFoundTime = element.get('TimestampFoundNewData', 0)
    childrenElements = backend.getElementsForParent(element)
    try:
        lastUpdate = float(max(childrenElements, key = lambda x: x.timestamp).timestamp)
    except Exception, ex:
        print element["RequestName"]
        raise ex
    if (currentTime - max(newDataFoundTime, lastUpdate)) > openRunningTimeout:
        workflowsToClose.append(element.id)
print "Workflow to close"        
print len(workflowsToClose)