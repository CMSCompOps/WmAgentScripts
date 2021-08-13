import os
import csv
import random
from logging import Logger
from abc import ABC, abstractmethod

from Services.CRIC.CRICReader import CRICReader
from Services.GWMSMon.GWMSMonReader import GWMSMonReader
from Services.MONIT.MONITReader import MONITReader
from Services.WMStats.WMStatsReader import WMStatsReader
from Utilities.Logging import getLogger

from typing import Optional, Any


class BaseDocumentCache(ABC):

    """
    __BaseDocumentCache__
    General Abstract Base Class for building caching documents
    """

    def __init__(self, defaultValue: Any = {}, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.defaultValue = defaultValue
            self.lifeTimeMinutes = int(20 + random.random() * 10)

        except Exception as error:
            raise Exception(f"Error initializing BaseDocumentCache\n{str(error)}")

    @abstractmethod
    def get(self) -> Any:
        """
        The function to get the cached data
        :return: cached data
        """
        pass


class SSBProdStatus(BaseDocumentCache):
    """
    __SSBProdStatus__
    General API for building the chaching document of key ssb_prod_status
    """

    def get(self) -> dict:
        monitReader = MONITReader()
        return monitReader.getDashbssb("sts15min", "prod_status")


class SSBCoreMaxUsed(BaseDocumentCache):
    """
    __SSBCoreMaxUsed__
    General API for building the chaching document of key ssb_core_max_used
    """

    def get(self) -> list:
        monitReader = MONITReader()
        return monitReader.getDashbssb("scap15min", "core_max_used")


class SSBCoreProduction(BaseDocumentCache):
    """
    __SSBCoreProduction__
    General API for building the chaching document of key ssb_core_production
    """

    def get(self) -> list:
        monitReader = MONITReader()
        return monitReader.getDashbssb("scap15min", "core_production")


class SSBCoreCpuIntensive(BaseDocumentCache):
    """
    __SSBCoreCpuIntensive__
    General API for building the chaching document of key ssb_core_cpu_intensive
    """

    def get(self) -> list:
        monitReader = MONITReader()
        return monitReader.getDashbssb("scap15min", "core_cpu_intensive")


class GWMSMonTotals(BaseDocumentCache):
    """
    __GWMSMonTotals__
    General API for building the chaching document of key gwmsmon_totals
    """

    def get(self) -> dict:
        gwmsmonReader = GWMSMonReader()
        return gwmsmonReader.getViewByKey("pool", "totals") or {}


class GWMSMonProdSiteSummary(BaseDocumentCache):
    """
    __GWMSMonProdSiteSummary__
    General API for building the chaching document of key gwmsmon_prod_site_summary
    """

    def get(self) -> dict:
        gwmsmonReader = GWMSMonReader()
        return gwmsmonReader.getViewByKey("prod", "site_summary") or {}


class GWMSMonProdMaxUsed(BaseDocumentCache):
    """
    __GWMSMonProdMaxUsed__
    General API for building the chaching document of key gwmsmon_prod_maxused
    """

    def get(self) -> dict:
        gwmsmonReader = GWMSMonReader()
        return gwmsmonReader.getViewByKey("prod", "maxusedcpus") or {}


class MCoreReady(BaseDocumentCache):
    """
    __MCoreReady__
    General API for building the chaching document of key mcore_ready
    """

    def get(self) -> dict:
        gwmsmonReader = GWMSMonReader()
        return gwmsmonReader.getMCoreReady() or {}


class DetoxSites(BaseDocumentCache):
    """
    __DetoxSites__
    General API for building the chaching document of key detox_sites
    """

    def get(self) -> list:
        with os.popen("curl --retry 5 -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt") as file:
            data = file.read().split("\n")
        return data


class SiteStorage(BaseDocumentCache):
    """
    __SiteStorage__
    General API for building the chaching document of key site_storage
    """

    def get(self) -> dict:
        cricReader = CRICReader()
        return cricReader.getSiteStorage() or []


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
        wmstatsReader = WMStatsReader()
        return wmstatsReader.getCachedWMStats() or {}
