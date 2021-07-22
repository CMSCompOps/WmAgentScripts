import logging
from logging import Logger

from typing import Optional, Any

from Cache import DocumentCache
from Cache.CacheManager import CacheManager


class DataCacheLoader(object):
    """
    __DataCacheLoader__
    General API for loading caching data
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.cacheManager = CacheManager()
            self.cache = {}

            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing DataCacheLoader\n{str(error)}")

    def load(self, key: str) -> bool:
        """
        The function to load the caching data for a given key
        :param key: key name
        :return: True if succeeded, False o/w
        """
        try:
            if key == "ssb_prod_status":
                self.cache[key] = DocumentCache.SSBProdStatusDocCache()
            elif key == "ssb_core_max_used":
                self.cache[key] = DocumentCache.SSBCoreMaxUsedDocCache([])
            elif key == "ssb_core_production":
                self.cache[key] = DocumentCache.SSBCoreProductionDocCache([])
            elif key == "ssb_core_cpu_intensive":
                self.cache[key] = DocumentCache.SSBCoreCpuIntensiveDocCache([])
            elif key == "gwmsmon_totals":
                self.cache[key] = DocumentCache.GWMSMONTotalsDocCache()
            elif key == "gwmsmon_prod_site_summary":
                self.cache[key] = DocumentCache.GWMSMONProdSiteSummaryDocCache()
            elif key == "gwmsmon_prod_maxused":
                self.cache[key] = DocumentCache.GWMSMONProdMaxUsedDocCache()
            elif key == "mcore_ready":
                self.cache[key] = DocumentCache.MCoreReadyDocCache()
            elif key == "detox_sites":
                self.cache[key] = DocumentCache.DetoxSitesDocCache([])
            elif key == "site_queues":
                self.cache[key] = DocumentCache.SiteQueuesDocCache()
            elif key == "site_storage":
                self.cache[key] = DocumentCache.SiteStorageDocCache()
            elif key == "file_invalidation":
                self.cache[key] = DocumentCache.FileInvalidationDocCache([])
            elif key == "wmstats":
                self.cache[key] = DocumentCache.WMStatsDocCache()
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

            self.cacheManager.set(key, data, lifeTimeMinutes=self.cache[key].lifeTimeMinutes)
            return data

        except Exception as error:
            self.logger.error("Failed to get cache data for key %s", key)
            self.logger.error(str(error))
