"""
ReqMgr request handling.

"""
from WMCoreService.CouchClient import CouchServer

def splitCouchServiceURL(serviceURL):
    """
    split service URL to couchURL and couchdb name
    serviceURL should be couchURL/dbname format.
    """

    splitedURL = serviceURL.rstrip('/').rsplit('/', 1)
    return splitedURL[0], splitedURL[1]

class WMStatsClient(object):
    
    ACTIVE_STATUS = ["new",
                    "assignment-approved",
                    "assigned",
                    "ops-hold",
                    "negotiating",
                    "acquired",
                    "running",
                    "running-open",
                    "running-closed",
                    "failed",
                    "completed",
                    "closed-out",
                    "announced",
                    "aborted",
                    "rejected"]
    
    def __init__(self, url):
        # main CouchDB database where requests/workloads are stored
        url = url or "https://cmsweb.cern.ch/couchdb/wmstats"
        couchUrl, dbName = splitCouchServiceURL(url)
        self.server = CouchServer(couchUrl)
        self.couchdb = self.server.connectDatabase(dbName)
        self.couchapp = "WMStats"
    
    def getRequestByNames(self, requestNames, jobInfoFlag = False):
        data = self._getRequestByNames(requestNames, True)
        requestInfo = self._formatCouchData(data)
        if jobInfoFlag:
            # get request and agent info
            self._updateReuestInfoWithJobInfo(requestInfo)
        return requestInfo
    
    def getActiveData(self, jobInfoFlag = False):
        
        return self.getRequestByStatus(WMStatsClient.ACTIVE_STATUS, jobInfoFlag)
    
    def getRequestByStatus(self, statusList, jobInfoFlag = False):
        
        data = self._getRequestByStatus(statusList, True)
        requestInfo = self._formatCouchData(data)

        if jobInfoFlag:
            # get request and agent info
            self._updateReuestInfoWithJobInfo(requestInfo)
        return requestInfo
    
    def _updateReuestInfoWithJobInfo(self, requestInfo):
        if len(requestInfo.keys()) != 0:
            requestAndAgentKey = self._getRequestAndAgent(requestInfo.keys())
            jobDocIds = self._getLatestJobInfo(requestAndAgentKey)
            jobInfoByRequestAndAgent = self._getAllDocsByIDs(jobDocIds)
            self._combineRequestAndJobData(requestInfo, jobInfoByRequestAndAgent)
            
    def _getCouchView(self, view, options, keys = []):
        
        if not options:
            options = {}
        options.setdefault("stale", "update_after")
        if keys and type(keys) == str:
            keys = [keys]
        return self.couchdb.loadView(self.couchapp, view, options, keys)
            
        
    def _formatCouchData(self, data, key = "id"):
        result = {}
        for row in data['rows']:
            result[row[key]] = row["doc"]
        return result
    
    def _combineRequestAndJobData(self, requestData, jobData):
        """
        update the request data with job info
        requestData['AgentJobInfo'] = {'vocms234.cern.ch:9999': {"_id":"d1d11dfcb30e0ab47db42007cb6fb847",
        "_rev":"1-8abfaa2de822ed081cb8d174e3e2c003",
        "status":{"inWMBS":334,"success":381,"submitted":{"retry":2,"pending":2},"failure":{"exception":3}},
        "agent_team":"testbed-integration","workflow":"amaltaro_OracleUpgrade_TEST_HG1401_140220_090116_6731",
        "timestamp":1394738860,"sites":{"T2_CH_CERN_AI":{"submitted":{"retry":1,"pending":1}},
        "T2_CH_CERN":{"success":6,"submitted":{"retry":1,"pending":1}},
        "T2_DE_DESY":{"failure":{"exception":3},"success":375}},
        "agent":"WMAgentCommissioning",
        "tasks":
           {"/amaltaro_OracleUpgrade_TEST_HG1401_140220_090116_6731/Production":
            {"status":{"failure":{"exception":3},"success":331},
             "sites":{"T2_DE_DESY": {"success":325,"wrappedTotalJobTime":11305908,
                                     "dataset":{},"failure":{"exception":3},
                                     "cmsRunCPUPerformance":{"totalJobCPU":10869688.8,
                                                             "totalEventCPU":10832426.7,
                                                             "totalJobTime":11255865.9},
                                     "inputEvents":0},
                      "T2_CH_CERN":{"success":6,"wrappedTotalJobTime":176573,
                                    "dataset":{},
                                    "cmsRunCPUPerformance":{"totalJobCPU":167324.8,
                                                            "totalEventCPU":166652.1,
                                                            "totalJobTime":174975.7},
                                    "inputEvents":0}},
             "subscription_status":{"updated":1393108089, "finished":2, "total":2,"open":0},
             "jobtype":"Production"},
            "/amaltaro_OracleUpgrade_TEST_HG1401_140220_090116_6731/Production/ProductionMergeRAWSIMoutput/ProductionRAWSIMoutputMergeLogCollect":
             {"jobtype":"LogCollect",
              "subscription_status":{"updated":1392885768,
              "finished":0,
              "total":1,"open":1}},
            "/amaltaro_OracleUpgrade_TEST_HG1401_140220_090116_6731/Production/ProductionMergeRAWSIMoutput":
              {"status":{"success":41,"submitted":{"retry":1,"pending":1}},
                "sites":{"T2_DE_DESY":{"datasetStat":{"totalLumis":973,"events":97300,"size":105698406915},
                                       "success":41,"wrappedTotalJobTime":9190,
                                       "dataset":{"/GluGluToHTohhTo4B_mH-350_mh-125_8TeV-pythia6-tauola/Summer12-OracleUpgrade_TEST_ALAN_HG1401-v1/GEN-SIM":
                                                   {"totalLumis":973,"events":97300,"size":105698406915}},
                                       "cmsRunCPUPerformance":{"totalJobCPU":548.92532,"totalEventCPU":27.449808,"totalJobTime":2909.92125},
                                    "inputEvents":97300},
                         "T2_CH_CERN":{"submitted":{"retry":1,"pending":1}}},
                "subscription_status":{"updated":1392885768,"finished":0,"total":1,"open":1},
                "jobtype":"Merge"},
           "agent_url":"vocms231.cern.ch:9999",
           "type":"agent_request"}}
        """
        for row in jobData["rows"]:
            jobInfo = requestData[row["doc"]["workflow"]]
            jobInfo["AgentJobInfo"]  = {} 
            jobInfo["AgentJobInfo"][row["doc"]["agent_url"]] = row["doc"]
    
            
    def _getRequestByNames(self, requestNames, detail = True):
        """
        'status': list of the status
        """
        options = {}
        options["include_docs"] = detail
        result = self.couchdb.allDocs(options, requestNames)
        return result
        
    def _getRequestByStatus(self, statusList, detail = True):
        """
        'status': list of the status
        """
        options = {}
        options["include_docs"] = detail
        keys = statusList or WMStatsClient.ACTIVE_STATUS
        return self._getCouchView("requestByStatus", options, keys)
    
    def _getRequestAndAgent(self, filterRequest = None):
        """
        returns the [['request_name', 'agent_url'], ....]
        """
        options = {}
        options["reduce"] = True
        options["group"] = True
        result = self._getCouchView("requestAgentUrl", options)
        
        if filterRequest == None:
            keys = [row['key'] for row in result["rows"]]
        else:
            keys = [row['key'] for row in result["rows"] if row['key'][0] in filterRequest]
        return keys
    
    def _getLatestJobInfo(self, keys):
        """
        keys is [['request_name', 'agent_url'], ....]
        returns ids
        """
        options = {}
        options["reduce"] = True
        options["group"] = True
        result = self._getCouchView("latestRequest", options, keys)
        ids = [row['value']['id'] for row in result["rows"]]
        return ids
    
    def _getAllDocsByIDs(self, ids):
        """
        keys is [id, ....]
        returns document
        """
        options = {}
        options["include_docs"] = True
        result = self.couchdb.allDocs(options, ids)
        
        return result