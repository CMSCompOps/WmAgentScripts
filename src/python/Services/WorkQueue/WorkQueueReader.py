import os
import logging
from logging import Logger

from Utilities.WebTools import getResponse
from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Decorators import runWithRetries

from typing import Optional


class WorkQueueReader(object):
    """
    _WorkQueueReader_
    General API for reading data from WorkQueue
    """

    def __init__(self, logger: Optional[Logger] = None, **contact):
        try:
            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.wqEndpoint = {
                "elementsByParent": "/couchdb/workqueue/_design/WorkQueue/_view/elementsByParent",
            }  # TODO: check endpoint, call against couchdb

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing WorkQueueReader\n{str(error)}")

    @runWithRetries(tries=5, wait=0, default=[])
    def getWorkQueue(self, wf: str):
        """
        The function to get the work queue for a given workflow
        :param wf: workflow name
        :return: workload specification
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl,
                endpoint=self.wqEndpoint["elementsByParent"],
                param={"key": wf, "include_docs": True},
            )
            return [item["doc"] for item in result["rows"] if item["doc"] is not None]

        except Exception as error:
            print("Failed to get the work queue")
            print(str(error))
