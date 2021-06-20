"""
File       : ReqMgrReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from ReqMgr
"""

import logging
import os
from Utils.WebTools import getResponse
from Utils.ConfigurationHandler import ConfigurationHandler


class ReqMgrReader(object):
    """
    _ReqMgrReader_
    General API for reading data from ReqMgr
    """

    def __init__(self, url=None, logger=None, **contact):

        try:
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv('REQMGR_URL', configurationHandler.get('reqmgr_url'))
            self.logger = logger or logging.getLogger(self.__class__.__name__)
        except Exception as e:
            msg = "Error initializing ReqMgrReader\n"
            msg += "%s\n" % format(e)
            raise Exception(msg)

    def getWorkflowByCampaign(self, campaign, details=False):
        """
        The function to get the list of workflows for a given campaign
        :param campaign: campaign name
        :param details: if True, it returns details for each workflow, o/w, just workflow names
        :return: lis
        """

        try:
            result = getResponse(url=self.reqmgrUrl,
                                 endpoint='/reqmgr2/data/request/',
                                 param={"campaign": campaign, "detail": str(details)})


            data = result['result']
            print("PRINT")
            print(data)
            print("LOGGER")
            self.logger.info(data)
            if details:
                ## list of dict
                r = []
                for it in data:
                    r.extend(it.values())
                return r
            else:
                return data


        except Exception as error:
            print('Failed to get workflows from reqmgr for campaign %s ' % campaign)
            print(str(error))
