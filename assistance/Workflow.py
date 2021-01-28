"""
File       : Workflow.py
Author     : Hasan Ozturk <haozturk AT cern dot com>

Description: Workflow class provides all the information needed for the filtering of workflows in assistance manual.

"""

class Workflow:

    def _init_(self, workflow, url='cmsweb.cern.ch'):
        """
        Initialize the Workflow class
        :param str workflow: is the name of the workflow
        :param str url: is the url to fetch information from
        """

        super(Workflow, self)._init_()
        self.workflow = workflow
        self.url = url

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

    def getPrimaryDataset():
        """
        :assumption: every production workflow reads just one PD

        :param: None
        :returns: the name of the PD that this workflow reads
        :rtype: string 
        """
        pass

    def getPrimaryDatasetLocation():
        """
        :assumption: every production workflow reads just one PD

        :param: None
        :returns: list of RSEs hosting the PD
        :rtype: list of strings
        """
        pass

    def getSecondaryDatasets():
        """
        :info: a workflow can read more than one secondary datasets

        :param: None
        :returns: list of the names of PUs that this workflow reads
        :rtype: list of strings
        """
        pass

    def getSecondaryDatasetsLocation():
        """
        :info: a workflow can read more than one secondary datasets

        :param: None
        :returns: dictionary containing PU name and location pairs
        :rtype: dict
        """
        pass

    def getCampaigns():
        """
        Function to get the list of campaigns that this workflow belongs to

        :param: None
        :returns: list of campaigns that this workflow belongs to
        :rtype: list of strings
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

    def getSecondaryAAA():
        """
        Function to get the secondaryAAA/TrustPUSitelists value of the request (Either True or False)

        :param: None
        :returns: the secondaryAAA/TrustPUSitelists value of the request (Either True or False)
        :rtype: boolean
        """
        pass



    


