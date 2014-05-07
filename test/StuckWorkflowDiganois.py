from __future__ import print_function
from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection
import time

class RequestsReport(object):
    
    def __init__(self, group = ""):
        self.requests = []
        self.group = group
    
    def numOfRequests(self):
        return len(self.requests)
    
    def add(self, requestInfo):
        self.requests.append(requestInfo)
    
    def sortByStatusTime(self, reverse = False):
        
        return sorted(self.requests, 
                      key = lambda requestInfo: requestInfo.getRequestStatus(True)['update_time'], 
                      reverse = reverse)
    
    def printFormat(self, detail = True):
        fileName = "stuckReportForAll.txt"
        requests = self.sortByStatusTime()
        report =  "**** %s : %s ****\n" % (self.group, self.numOfRequests())
        print(report)
        for requestInfo in requests:
            requestName = requestInfo.requestName
            reqStatusWithTime = requestInfo.getRequestStatus(True)
            reqStatus = requestInfo.getRequestStatus(True)['status']
            if requestInfo.data.has_key("AgentJobInfo"):
                agents = requestInfo.data["AgentJobInfo"].keys()
            else:
                agents = "No agent Info"
            localTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(reqStatusWithTime['update_time']))
            report += "%s : %s :%s :%s\n" % (requestName, reqStatus, agents, localTime)
            if detail:
                for taskName, taskInfo in requestInfo.getTasks().items():
                    jobSummary = taskInfo.jobSummary
                    report += "  %s : %s\n" % (taskInfo.taskType, taskName.split('/')[-1])
                    report += "    job info: %s\n" % jobSummary.getJSONStatus()
            report += "\n\n"
        report += "*******\n\n\n"
        f = open(fileName, "a")
        print(report, file = f)
        
if __name__ == "__main__":
    url = "https://cmsweb.cern.ch/couchdb/wmstats"
    testbedWMStats = WMStatsClient(url)
    print("start to getting job information from %s" % url)
    print("will take a while\n")
    requestNames = set()
    with open('stuckWorkflows.txt') as fp:
        for line in fp:
            if line.strip().startswith('#') or not line.strip():
                continue
            requestNames.add(line.strip())
    
    
    #requests = testbedWMStats.getRequestByNames(list(requestNames), True)
    requests = testbedWMStats.getRequestByStatus(None, True)
    print("There are %s requests to dignose" % len(requests))
    
    requestCollection = RequestInfoCollection(requests)
    
    results = requestCollection.getData()
    status = {}
    requests = {}
    
    mismatchWrong = RequestsReport("Something very wrong")
    mismatch = RequestsReport("completd but in different state")
    mismatchWithNoJobs = RequestsReport("completd but in different state with No jobs")
    completed = RequestsReport("completed waiting for closeout")
    stillRunning = RequestsReport("jobs still running")
    noJob = RequestsReport("Log collect job is not created")
    i = 0
    requestsResult = set()
    for requestName, requestInfo in results.items():
        reqStatusWithTime = requestInfo.getRequestStatus(True)
        reqStatus = reqStatusWithTime['status']
        status.setdefault(reqStatus, {})
        status[reqStatus].setdefault("num", 0) 
        status[reqStatus]['num'] += 1
        i += 1
        requestsResult.add(requestName)
        completedFlag = True
        stillRunningFlag = False
        noJobFlag = False
        
        if len(requestInfo.getTasks()) == 0:
            noJobFlag = True
        
        for taskName, taskInfo in requestInfo.getTasks().items():
            jobSummary = taskInfo.jobSummary
            if jobSummary.getTotalJobs() == 0:
                completedFlag = False
                noJobFlag = True
            elif jobSummary.getTotalJobs() != jobSummary.getCompleted():
                completedFlag = False
                stillRunningFlag = True
        
        if completedFlag:
            if reqStatus in ["new", "assigned", "assignment-approved"]:
                continue
            if reqStatus in ["acquired", "running-open"]:
                # something wrong - need to investigate
                continue
            if reqStatus not in ['completed', 'closed-out', 'announced', 'normal-archived', 'aborted', "failed"]:
                if noJobFlag:
                    if requestInfo.getJobSummary().getTotalJobs() > 0:
                        mismatchWrong.add(requestInfo)
                    else:
                        mismatchWithNoJobs.add(requestInfo)
                else:
                    mismatch.add(requestInfo)
            else:
                completed.add(requestInfo)
        elif stillRunningFlag:
            stillRunning.add(requestInfo)
        elif noJobFlag:
            noJob.add(requestInfo)
                      
    #print requestNames.difference(requestsResult)
    mismatchWrong.printFormat(False)
    mismatchWithNoJobs.printFormat(False)
    mismatch.printFormat(False)
    noJob.printFormat()
    stillRunning.printFormat()
    completed.printFormat(False)
    
    print("done")