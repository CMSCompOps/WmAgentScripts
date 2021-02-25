"""
File       : Workflow.py
Author     : Hasan Ozturk <haozturk AT cern dot com>

Description: Workflow class provides all the information needed for the filtering of workflows in assistance manual.

"""
from utils.WebTools import getResponse
from utils.SearchTools import findKeys

class Workflow:

    def __init__(self, workflowName, url='cmsweb.cern.ch'):
        """
        Initialize the Workflow class
        :param str workflowName: is the name of the workflow
        :param str url: is the url to fetch information from
        """

        self.workflowName = workflowName
        self.url = url
        self.workflowParams = self.getWorkflowParams()

    def getWorkflowParams(self):

        """
        Get the workflow parameters from ReqMgr2.
        See the `ReqMgr 2 wiki <https://github.com/dmwm/WMCore/wiki/reqmgr2-apis>`_
        for more details.
        :returns: Parameters for the workflow from ReqMgr2.
        :rtype: dict
        """

        try:
            result = getResponse(url=self.url,
                                 endpoint='/reqmgr2/data/request/',
                                 param=self.workflowName)

            for params in result['result']:
                for key, item in params.items():
                    if key == self.workflowName:
                        self.workflowParams = item
                        return item

        except Exception as error:
            print 'Failed to get workflow params from reqmgr for %s '% self.workflowName
            print str(error) 

    def getPrepID(self):
        """
        :param: None
        :returns: PrepID
        :rtype: string
        """
        return self.workflowParams.get('PrepID')

    def getNumberOfEvents(self):
        """
        :param: None
        :returns: Number of events requested
        :rtype: int
        """
        return self.workflowParams.get('TotalInputEvents')

    def getRequestType(self):
        """
        :param: None
        :returns: Request Type
        :rtype: string
        """

        return self.workflowParams.get('RequestType')

    def getSiteWhitelist(self):
        """
        :param: None
        :returns: SiteWhitelist
        :rtype: string
        """

        return self.workflowParams.get('SiteWhitelist')


    def getCampaigns(self):
        """
        Function to get the list of campaigns that this workflow belongs to

        :param: None
        :returns: list of campaigns that this workflow belongs to
        :rtype: list of strings
        """
        
        return findKeys('Campaign',self.workflowParams)


    ## Get runtime related values

    def getAge():
        """
        Number of days since the creation of the workflow

        :param: None
        :returns: Age of the workflow
        :rtype: float
        """
        pass

    def getLabels():
        """
        A workflow can have multiple labels. These labels are also going to be present on JIRA
        Current labels:
          1. Blocker: Workflow is waiting an input from the requestors
        Possible labels:
          1. FileIssue: Workflow is suffering from a file issue

        :param: None
        :returns: list of labels
        :rtype: list
        """
        pass

    def getErrors():
    	"""
        :param None
        :returns: a dictionary containing error codes and number of failed jobs for each task/step in the following format::
                  {task: {errorcode: {site: failed_job_count}}}
        :rtype: dict
        """
    	pass

    def getFailureRate():
        """
        :param None
        :returns: a dictionary containing failure rates for each task/step in the following format::
                  {task: failure_rate}
        :rtype: dict
        """
        pass

    ## Get request related values

    def getPrimaryDataset(self):
        """
        :assumption: every production workflow reads just one PD

        :param: None
        :returns: the name of the PD that this workflow reads
        :rtype: string 
        """
        
        return findKeys('InputDataset',self.workflowParams)

    def getPrimaryDatasetLocation():
        """
        :assumption: every production workflow reads just one PD

        :param: None
        :returns: list of RSEs hosting the PD
        :rtype: list of strings
        """
        pass

    def getSecondaryDatasets(self):
        """
        :info: a workflow can read more than one secondary datasets

        :param: None
        :returns: list of the names of PUs that this workflow reads
        :rtype: list of strings
        """
        
        return findKeys('MCPileup',self.workflowParams)

    def getSecondaryDatasetsLocation():
        """
        :info: a workflow can read more than one secondary datasets

        :param: None
        :returns: dictionary containing PU name and location pairs
        :rtype: dict
        """
        pass

    def getPrimaryAAA():
        """
        Function to get the primaryAAA/TrustSitelists value of the request (Either True or False)

        :param: None
        :returns: the primaryAAA/TrustSitelists value of the request (Either True or False)
        :rtype: boolean
        """
        pass

    def getSecondaryAAA():
        """
        Function to get the secondaryAAA/TrustPUSitelists value of the request (Either True or False)

        :param: None
        :returns: the secondaryAAA/TrustPUSitelists value of the request (Either True or False)
        :rtype: boolean
        """
        pass




    


