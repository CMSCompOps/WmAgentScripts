import os
import json
import csv
import random
import logging
from logging import Logger
from abc import ABC, abstractmethod

from typing import Optional, Any

class BaseDocumentCache(ABC):

    """
    __BaseDocumentCache__
    General Abstract Base Class for building caching documents
    """

    def __init__(self, defaultValue: Any = {}, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

            self.defaultValue = defaultValue
            self.lifeTimeMinutes = int(20 + random.random() * 10)

        except Exception as error:
            raise Exception(f"Error initializing BaseDocumentCache\n{str(error)}")

    @abstractmethod
    def get(self) -> Any:
        """
        The function to get the caching data
        """
        pass


class SSBProdStatus(BaseDocumentCache):
    """
    __SSBProdStatus__
    General API for building the chaching document of key ssb_prod_status
    """

    def get(self) -> dict:
        return None  # TODO: implement get_dashbssb() or drop this


class SSBCoreMaxUsed(BaseDocumentCache):
    """
    __SSBCoreMaxUsed__
    General API for building the chaching document of key ssb_core_max_used
    """

    def get(self) -> list:
        return None  # TODO: implement get_dashbssb() or drop this


class SSBCoreProduction(BaseDocumentCache):
    """
    __SSBCoreProduction__
    General API for building the chaching document of key ssb_core_production
    """

    def get(self) -> list:
        return None  # TODO: implement get_dashbssb() or drop this


class SSBCoreCpuIntensive(BaseDocumentCache):
    """
    __SSBCoreCpuIntensive__
    General API for building the chaching document of key ssb_core_cpu_intensive
    """

    def get(self) -> list:
        return None  # TODO: implement get_dashbssb() or drop this


class GWMSMonTotals(BaseDocumentCache):
    """
    __GWMSMonTotals__
    General API for building the chaching document of key gwmsmon_totals
    """

    def get(self) -> dict:
        with os.popen("curl --retry 5 -s https://cms-gwmsmon.cern.ch/poolview/json/totals") as file:
            data = json.loads(file.read())
        return data


class GWMSMonProdSiteSummary(BaseDocumentCache):
    """
    __GWMSMonProdSiteSummary__
    General API for building the chaching document of key gwmsmon_prod_site_summary
    """

    def get(self) -> dict:
        with os.popen("curl --retry 5 -s https://cms-gwmsmon.cern.ch/prodview//json/site_summary") as file:
            data = json.loads(file.read())
        return data


class GWMSMonProdMaxUsed(BaseDocumentCache):
    """
    __GWMSMonProdMaxUsed__
    General API for building the chaching document of key gwmsmon_prod_maxused
    """

    def get(self) -> dict:
        with os.popen("curl --retry 5 -s https://cms-gwmsmon.cern.ch/prodview//json/maxusedcpus") as file:
            data = json.loads(file.read())
        return data


class MCoreReady(BaseDocumentCache):
    """
    __MCoreReady__
    General API for building the chaching document of key mcore_ready
    """

    def get(self) -> dict:
        with os.popen(
            "curl --retry 5 -s http://cmsgwms-frontend-global.cern.ch/vofrontend/stage/mcore_siteinfo.json"
        ) as file:
            data = json.loads(file.read())
        return data


class DetoxSites(BaseDocumentCache):
    """
    __DetoxSites__
    General API for building the chaching document of key detox_sites
    """

    def get(self) -> list:
        with os.popen("curl --retry 5 -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt") as file:
            data = file.read().split("\n")
        return data


class SiteQueues(BaseDocumentCache):
    """
    __SiteQueues__
    General API for building the chaching document of key site_queues
    """

    def get(self) -> dict:
        return None  # TODO: implement getNodesQueue() or drop this


class SiteStorage(BaseDocumentCache):
    """
    __SiteStorage__
    General API for building the chaching document of key site_storage
    """

    def get(self) -> dict:
        return None  # TODO: implement getSiteStorage() or drop this


class FileInvalidation(BaseDocumentCache):
    """
    __FileInvalidation__
    General API for building the chaching document of key file_invalidation
    """

    def get(self) -> dict:
        with os.popen(
            'curl -s "https://docs.google.com/spreadsheets/d/11fFsDOTLTtRcI4Q3gXw0GNj4ZS8IoXMoQDC3CbOo_2o/export?format=csv"'
        ) as file:
            data = csv.reader(file)
        return list(set(row[3].split(":")[-1] for row in data))


class WMStats(BaseDocumentCache):
    """
    __WMStats__
    General API for building the chaching document of key wmstats
    """

    def get(self) -> dict:
        return None  # TODO: implement getWMStats() or drop this