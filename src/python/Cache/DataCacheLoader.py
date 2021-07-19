import logging
from logging import Logger

from typing import Optional, Any

from Cache import DocCache
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

        except Exception as e:
            msg = "Error initializing DataCacheLoader\n"
            msg += str(e)
            raise Exception(msg)

    def load(self, key: str) -> bool:
        """
        The function to load the caching data for a given key
        :param key: key name
        :return: True if succeeded, False o/w
        """
        try:
            if key is "ssb_prod_status":
                self.cache[key] = DocCache.SSBProdStatusDocCache()
            elif key is "ssb_core_max_used":
                self.cache[key] = DocCache.SSBCoreMaxUsedDocCache([])
            elif key is "ssb_core_production":
                self.cache[key] = DocCache.SSBCoreProductionDocCache([])
            elif key is "ssb_core_cpu_intensive":
                self.cache[key] = DocCache.SSBCoreCpuIntensiveDocCache([])
            elif key is "gwmsmon_totals":
                self.cache[key] = DocCache.GWMSMONTotalsDocCache()
            elif key is "gwmsmon_prod_site_summary":
                self.cache[key] = DocCache.GWMSMONProdSiteSummaryDocCache()
            elif key is "gwmsmon_prod_maxused":
                self.cache[key] = DocCache.GWMSMONProdMaxUsedDocCache()
            elif key is "mcore_ready":
                self.cache[key] = DocCache.MCoreReadyDocCache()
            elif key is "detox_sites":
                self.cache[key] = DocCache.DetoxSitesDocCache([])
            elif key is "site_queues":
                self.cache[key] = DocCache.SiteQueuesDocCache()
            elif key is "site_storage":
                self.cache[key] = DocCache.SiteStorageDocCache()
            elif key is "file_invalidation":
                self.cache[key] = DocCache.FileInvalidationDocCache([])
            elif key is "wmstats":
                self.cache[key] = DocCache.WMStatsDocCache()
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

            data = self.cache[key].default
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
