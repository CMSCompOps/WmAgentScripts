import os
from logging import Logger

from Utilities.Logging import getLogger
from Utilities.WebTools import getResponse
from Utilities.ConfigurationHandler import ConfigurationHandler
from Utilities.Decorators import runWithRetries

from typing import Optional


class WorkQueueReader(object):
    """
    _WorkQueueReader_
    General API for reading data from WorkQueue
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.reqmgrUrl = os.getenv("REQMGR_URL", configurationHandler.get("reqmgr_url"))
            self.workqueueEndpoint = "/couchdb/workqueue/_design/WorkQueue/_view/"            

        except Exception as error:
            raise Exception(f"Error initializing WorkQueueReader\n{str(error)}")

    @runWithRetries(tries=5, wait=0, default=[])
    def getWorkQueue(self, wf: str) -> list:
        """
        The function to get the work queue for a given workflow
        :param wf: workflow name
        :return: work queue
        """
        try:
            result = getResponse(
                url=self.reqmgrUrl, endpoint=self.workqueueEndpoint + f'elementsByParent?key="{wf}"&include_docs=true'
            )
            return [item["doc"] for item in result["rows"] if item["doc"] is not None]

        except Exception as error:
            print("Failed to get the work queue")
            print(str(error))
