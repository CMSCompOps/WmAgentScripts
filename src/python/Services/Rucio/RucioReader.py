import json
from logging import Logger

from rucio.client import Client
from WMCore.Services.CRIC.CRIC import CRIC

from Utilities.Logging import getLogger

from typing import List, Optional


class RucioReader(object):
    """
    __RucioReader__
    General API for reading data from Rucio
    """

    def __init__(self, logger: Optional[Logger] = None, **config) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.scope = "cms"

            with open("config/rucioConfiguration.json", "r") as file:
                defaultConfig = json.loads(file.read())
            config = {**defaultConfig, **config}
            self.rucio = Client(**config)

        except Exception as error:
            raise Exception(f"Error initializing RucioClient\n{str(error)}")

    def countDatasetFiles(self, dataset: str) -> int:
        """
        The function to get the number of files for a given dataset
        :param dataset: dataset name
        :return: number of files
        """
        try:
            return len([*self.rucio.list_files(self.scope, dataset)])

        except Exception as error:
            self.logger.error("Failed to get number of files for the dataset %s", dataset)
            self.logger.error(str(error))
            return 0

    def countDatasetFilesPerBlock(self, dataset: str) -> List[tuple]:
        """
        The function to get the number of files by block for a given dataset
        :param dataset: dataset name
        :return: list of (block name, number of files)
        """
        try:
            return [(block, self.countBlockFiles(block)) for block in self.getDatasetBlockNames(dataset)]

        except Exception as error:
            self.logger.error("Failed to get number of files per block for the dataset %s", dataset)
            self.logger.error(str(error))
            return []

    def countBlockFiles(self, block: str) -> int:
        """
        The function to get the number of files for a given block
        :param dataset: block name
        :return: number of files
        """
        try:
            numFiles = self.rucio.get_metadata(self.scope, block)["length"]
            if not numFiles:
                raise Exception("Block lenght in rucio is None")

            return numFiles

        except Exception as error:
            self.logger.error("Failed to get number of files for the block %s", block)
            self.logger.error(str(error))

    def getDatasetFileNames(self, dataset: str) -> List[str]:
        """
        The function to get the file names for a given dataset
        :param dataset: dataset name
        :return: file names
        """
        try:
            files = self.rucio.list_files(self.scope, dataset)
            return [file["name"] for file in files]

        except Exception as error:
            self.logger.error("Failed to get the file names for the dataset %s", dataset)
            self.logger.error(str(error))
            return []

    def getDatasetBlockNames(self, dataset: str) -> List[str]:
        """
        The function to get the block names for a given dataset
        :param dataset: dataset name
        :return: block names
        """
        try:
            blocks = self.rucio.list_content(self.scope, dataset)
            return [block["name"] for block in blocks]

        except Exception as error:
            self.logger.error("Failed to get the block names for the dataset %s", dataset)
            self.logger.error(str(error))

    def getDatasetLocationsByAccount(self, dataset: str, account: str) -> list:
        """
        The function to get the locations for a given dataset and for a given account in terms of computing element (not RSE name).
        This function assumes that the returned RSE expression includes only one RSE .
        :param dataset: dataset name
        :param account: account name
        :return: list of locations by account
        """
        try:
            RSEs = []
            for rule in self.rucio.list_did_rules(self.scope, dataset):
                if rule["account"] == account:
                    RSEs.append(rule["rse_expression"])

            return CRIC().PNNstoPSNs(RSEs)

        except Exception as error:
            self.logger.error("Failed to get the dataset %s locations for the account %s", dataset, account)
            self.logger.error(str(error))
            return []
