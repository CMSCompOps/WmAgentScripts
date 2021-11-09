import os
import json
from time import struct_time, gmtime, mktime, asctime
from logging import Logger
from pymongo.collection import Collection

from Utilities.ConfigurationHandler import ConfigurationHandler
from Services.EOS.EOSReader import EOSReader
from Databases.Mongo.MongoClient import MongoClient

from typing import Collection, Optional


class RemainingDatasetController(MongoClient):
    """
    __RemainingDatasetController__
    General API for monitoring the remaining dataset info
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__(logger=logger)
            configurationHandler = ConfigurationHandler()
            self.monitorEOSDirectory = configurationHandler.get("monitor_eos_dir")

        except Exception as error:
            raise Exception(f"Error initializing RemainingDatasetController\n{str(error)}")

    def __del__(self) -> None:
        self.purge(60)

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.remainingDatasetInfo

    def _buildMongoDocument(self, site: str, dataset: str, data: dict, now: struct_time = gmtime()) -> dict:
        return {
            "site": site,
            "dataset": dataset,
            "reasons": data.get("reasons", []),
            "size": data.get("size", 0),
            "time": int(mktime(now)),
            "date": asctime(now),
        }

    def set(self, site: str, data: dict) -> None:
        """
        The function to set new data in the remaining dataset info of a given site.
        It also deletes existing datasets not to be updated with the new data.
        :param site: site name
        :param data: datasets data
        """
        try:
            self.clean(site=site, dataset={"$in": sorted(self.get(site).keys() - data.keys())})
            for dataset, datasetData in data.items():
                super()._set(site, dataset, datasetData, site=site, dataset=dataset)

        except Exception as error:
            self.logger.error("Failed to set remaining dataset info for site %s", site)
            self.logger.error(str(error))

    def sync(self) -> None:
        """
        The function to sync all remaining dataset info for all sites
        """
        try:
            self.logger.info("Synching with all possible sites")
            sites = [*EOSReader(f"{self.monitorEOSDirectory}/remaining.json", self.logger).read().keys()]
            if not sites:
                for file in filter(
                    None, os.popen(f"ls -1 {self.monitorEOSDirectory}/remaining_*.json | sort").read().split("\n")
                ):
                    site = file.split("_", 1)[-1].split(".")[0]
                    if all(not site.endswith(suffix) for suffix in ["_MSS", "_Export"]):
                        sites.append(site)

            for site in sites:
                self.syncSite(site)

        except Exception as error:
            self.logger.error("Failed to sync all remaining dataset info")
            self.logger.error(str(error))

    def syncSite(self, site: str) -> None:
        """
        The function to sync remaining dataset info of a given site
        :param site: site name
        """
        try:
            self.logger.info("Synching on site %s", site)
            remainingReasons = EOSReader(f"{self.monitorEOSDirectory}/remaining_{site}.json", self.logger).read()
            self.set(site, remainingReasons)

        except Exception as error:
            self.logger.error("Failed to sync remaining dataset info for site %s", site)
            self.logger.error(str(error))

    def get(self, site: str) -> dict:
        """
        The function to get the remaining dataset info for a given site
        :param site: site name
        :return: remaining dataset info
        """
        try:
            content = super()._get("dataset", details=True, site=site)
            return dict(
                (dataset, {"size": doc.get("size", 0), "reasons": doc.get("reasons", [])})
                for dataset, doc in content.items()
            )

        except Exception as error:
            self.logger.error("Failed to get remaining dataset info")
            self.logger.error(str(error))

    def getSites(self) -> list:
        """
        The function to get all the sites names with remaining dataset info
        :return: sites names
        """
        try:
            return list(sorted(super()._get("site")))

        except Exception as error:
            self.logger.error("Failed to get sites with remaining dataset info")
            self.logger.error(str(error))

    def clean(self, **query) -> None:
        """
        The function to delete all remaining dataset info
        :param query: optional query params
        """
        try:
            super()._clean(**query)

        except Exception as error:
            self.logger.error("Failed to clean remaining dataset info")
            self.logger.error(str(error))

    def purge(self, expiredDays: int = 30) -> None:
        """
        The function to delete all remaining dataset info if it is expired
        :param expiredDays: passed days from expiration time so that data can be deleted
        """
        try:
            super()._purge("time", expiredDays)

        except Exception as error:
            self.logger.error("Failed to purge remaining dataset info expired for more than %s days", expiredDays)
            self.logger.error(str(error))

    def tell(self, site: str) -> None:
        """
        The function to tell the remaining dataset info for a given site
        :param site: site name
        """
        self.logger.info(json.dumps(self.get(site), indent=2))
