from logging import Logger

from Cache import DocumentCache
from Cache.CacheManager import CacheManager
from Utilities.Logging import getLogger

from typing import Optional, Any


class DataCacheLoader(object):
    """
    __DataCacheLoader__
    General API for loading cached data
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.cacheManager = CacheManager()
            self.cache = {}

        except Exception as error:
            raise Exception(f"Error initializing DataCacheLoader\n{str(error)}")

    def load(self, key: str) -> bool:
        """
        The function to load the cached data for a given key
        :param key: key name
        :return: True if succeeded, False o/w
        """
        try:
            if key == "ssb_prod_status":
                self.cache[key] = DocumentCache.SSBProdStatus()
            elif key == "ssb_core_max_used":
                self.cache[key] = DocumentCache.SSBCoreMaxUsed([])
            elif key == "ssb_core_production":
                self.cache[key] = DocumentCache.SSBCoreProduction([])
            elif key == "ssb_core_cpu_intensive":
                self.cache[key] = DocumentCache.SSBCoreCpuIntensive([])
            elif key == "gwmsmon_totals":
                self.cache[key] = DocumentCache.GWMSMonTotals()
            elif key == "gwmsmon_prod_site_summary":
                self.cache[key] = DocumentCache.GWMSMonProdSiteSummary()
            elif key == "gwmsmon_prod_maxused":
                self.cache[key] = DocumentCache.GWMSMonProdMaxUsed()
            elif key == "mcore_ready":
                self.cache[key] = DocumentCache.MCoreReady()
            elif key == "detox_sites":
                self.cache[key] = DocumentCache.DetoxSites([])
            elif key == "site_storage":
                self.cache[key] = DocumentCache.SiteStorage()
            elif key == "file_invalidation":
                self.cache[key] = DocumentCache.FileInvalidation([])
            elif key == "wmstats":
                self.cache[key] = DocumentCache.WMStats()
            else:
                raise ValueError("Unknown cache doc key %s", key)
            return True

        except Exception as error:
            self.logger.error("Failed to build doc cache for key %s", key)
            self.logger.error(str(error))
            return False

    def get(self, key: str, fresh: bool = False) -> Any:
        """
        The function to get the cached data for a given key
        :param key: key name
        :param fresh: if True build data from scratch, o/w get already cached data
        :return: data
        """
        try:
            if not self.load(key):
                return None

            cached = self.cacheManager.get(key) if not fresh else None
            if cached:
                return cached

            data = self.cache[key].defaultValue
            try:
                data = self.cache[key].get()

            except Exception as error:
                self.logger.critical("Failed to get cache doc for key %s", key)
                self.logger.critical(str(error))
                cached = self.cacheManager.get(key, noExpire=True)
                if cached:
                    self.logger.info("Returning the last doc in cache")
                    return cached
                else:
                    self.logger.info("Returning the default value")

            self.cacheManager.set(key, data, lifeTimeMinutes=self.cache[key].lifeTimeMinutes)
            return data

        except Exception as error:
            self.logger.error("Failed to get cache data for key %s", key)
            self.logger.error(str(error))
