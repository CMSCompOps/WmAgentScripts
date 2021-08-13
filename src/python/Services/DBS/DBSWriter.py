"""
File       : DBSReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for writing data to DBS
"""


import os
from logging import Logger
from dbs.apis.dbsClient import DbsApi

from Utilities.Logging import getLogger
from Utilities.ConfigurationHandler import ConfigurationHandler

from typing import Optional, List


class DBSWriter(object):
    """
    _DBSWriter_
    General API for writing data to DBS
    """

    def __init__(self, url: Optional[str] = None, logger: Optional[Logger] = None, **contact):
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            if url:
                self.dbsURL = url.replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
            else:
                configurationHandler = ConfigurationHandler()
                self.dbsURL = os.getenv("DBS_WRITER_URL", configurationHandler.get("dbs_url_writer"))
            self.dbs = DbsApi(self.dbsURL, **contact)

        except Exception as error:
            raise Exception(f"Error initializing DBSWriter\n{str(error)}")

    def setFileStatus(self, files: List[dict], validate: bool = True) -> bool:
        """
        The function to set files validation status to True or False
        :param files: list of files
        :param validate: True if to validate files, False o/w
        :return: True in case the all files status were updated, False o/w
        """
        try:
            for file in files:
                if file["is_file_valid"] != validate:
                    filename = file["logical_file_name"]
                    self.logger.info(f"Turning {filename} status to {validate}")
                    self.dbs.updateFileStatus(
                        logical_file_name=filename,
                        is_file_valid=int(validate),
                    )
            return True

        except Exception as error:
            self.logger.error("Failed to update files status")
            self.logger.error(str(error))
            return False

    def setDatasetStatus(self, dataset: str, actualStatus: str, newStatus: str, withFiles: bool = True) -> bool:
        """
        The function to set dataset status to True or False
        :param dataset: dataset name
        :param actualStatus: actual dataset status
        :param newStatus: new dataset status
        :param withFiles: if True, files status are also updated
        :return: True in case the dataset status was updated, False o/w
        """
        try:
            if actualStatus is None:
                self.logger.info(
                    "Setting dataset status to inexistent dataset %s, considered succeeded",
                    dataset,
                )
                return True

            self.dbs.updateDatasetType(dataset=dataset, dataset_access_type=newStatus)
            if withFiles:
                files = self.dbs.listFiles(dataset=dataset)
                fileStatus = newStatus not in ["DELETED", "DEPRECATED", "INVALID"]
                self.setFileStatus(files, fileStatus)
            return True

        except Exception as error:
            self.logger.error("Failed to update dataset status")
            self.logger.error(str(error))
            return False
