from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection, RequestInfo
import sys


url = "https://cmsweb.cern.ch/couchdb/wmstats"

def main():
    #a list of workflows
    workflows = sys.argv[1:]
  
    
    wMStats = WMStatsClient(url)
    print "start to getting job information from %s" % url
    #retrieve job information
    workflowsWithData = wMStats.getRequestByNames(workflows, jobInfoFlag = True)
    print '-'*120
    #print workflowsWithData
    requestCol = RequestInfoCollection(workflowsWithData)
    #print summary for each workflow
    for wf, info in requestCol.getData().items():
        print '-'*120
        print wf
        #a table
        print '\n'.join( "%10s %10s"%(t,n) 
            for t,n in  info.getJobSummary().getJSONStatus().items())


if __name__ == "__main__":
    main()
