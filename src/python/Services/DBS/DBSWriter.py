"""
File       : DBSReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for writing data to DBS
"""


# from dbs.apis.dbsClient import DbsApi
import logging
import os
from Utils.ConfigurationHandler import ConfigurationHandler

from typing import Optional, List


class DBSWriter(object):
    """
    _DBSWriter_
    General API for writing data to DBS
    """

    def __init__(
        self,
        url: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        **contact,
    ):
        # instantiate dbs api object
        try:
            if url:
                self.dbsURL = url.replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
            else:

                configurationHandler = ConfigurationHandler()
                self.dbsURL = os.getenv(
                    "DBS_WRITER_URL", configurationHandler.get("dbs_url_writer")
                )
            # self.dbs = DbsApi(self.dbsURL, **contact)
            self.dbs = None
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as e:
            msg = "Error in DBSWriter with DbsApi\n"
            msg += f"{e}\n"
            raise Exception(msg)

    def setFileStatus(self, files: List[dict], validate: bool = True) -> bool:
        """
        The function to set files status to True or False
        :param files: list of files
        :return: True in case the alls files' status were updated, o/w False
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

    def setDatasetStatus(
        self, dataset: str, actualStatus: str, newStatus: str, withFiles: bool = True
    ):
        try:
            if actualStatus is None:
                self.logger.info(
                    f"Setting dataset status to inexistent dataset {dataset}, considered succeeded"
                )
                return True

            fileStatus = 0 if newStatus in ["DELETED", "DEPRECATED", "INVALID"] else 1
            self.dbs.updateDatasetType(dataset=dataset, dataset_access_type=newStatus)
            if withFiles:
                files = self.dbs.listFiles(dataset=dataset)
                for file in files:
                    self.dbs.updateFileStatus(
                        logical_file_name=file["logical_file_name"],
                        is_file_valid=fileStatus,
                    )
            return True

        except Exception as error:
            self.logger.error("Failed to update dataset status")
            self.logger.error(str(error))
            return False
