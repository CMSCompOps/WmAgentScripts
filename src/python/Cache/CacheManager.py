import os
from time import struct_time, gmtime, mktime
import json
from logging import Logger
from pymongo.errors import WriteError, DocumentTooLarge
from pymongo.collection import Collection

from Utilities.ConfigurationHandler import ConfigurationHandler
from Databases.Mongo.MongoClient import MongoClient

from typing import Optional


class CacheManager(MongoClient):
    """
    __CacheManager__
    General API for managing the cache info
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__(logger=logger)
            self.cacheDirectory = ConfigurationHandler().get("cache_dir")

        except Exception as error:
            raise Exception(f"Error initializing CacheManager\n{str(error)}")

    def _setMongoCollection(self) -> Collection:
        return self.client.unified.cacheInfo

    def _buildMongoDocument(
        self, key: str, data: dict, lifeTimeMinutes: int = 10, now: struct_time = gmtime()
    ) -> dict:
        document = {
            "key": key,
            "time": int(mktime(now)),
            "expire": int(mktime(now) + 60 * lifeTimeMinutes),
            "lifetime": lifeTimeMinutes,
        }
        if data:
            document.update({"data": data})
        return document

    def set(self, key: str, data: dict, lifeTimeMinutes: int = 10) -> None:
        """
        The function to set data in cache
        :param key: key name
        :param data: data to be cached
        :param lifeTimeMinutes: minutes for data expiration
        """
        try:
            super()._set(key, data, lifeTimeMinutes, key=key)

        except (WriteError, DocumentTooLarge) as error:
            self.logger.error("Failed writing in mongo, will use file instead")
            with open(self._getKeyFilePath(key), "w") as file:
                file.write(json.dumps(data))
            super()._set(key, None, lifeTimeMinutes, key=key)

        except Exception as error:
            self.logger.error("Failed to set cache for key %s", key)
            self.logger.error(str(error))

    def _getFromFile(self, key: str) -> Optional[dict]:
        """
        The function to get cache data from file
        :param key: key name
        :return: cached data if any, None o/w
        """
        try:
            filePath = self._getKeyFilePath(key)
            if os.path.isfile(filePath):
                self.logger.info("File cache hit %s", key)
                with open(filePath, "r") as file:
                    content = json.loads(file.read())
                return content

            self.logger.info("File cache miss %s", key)
            return None

        except Exception as error:
            self.logger.error("Failed to get %s from file cache", key)
            self.logger.error(str(error))

    def _getKeyFilePath(self, key: str) -> str:
        """
        The function to get the file path for a given key
        :param key: key
        :return: file path
        """
        return f"{self.cacheDirectory}/{key.replace('/','_')}"

    def get(self, key: str, noExpire: bool = False) -> Optional[dict]:
        """
        The function to get data from cache for a given key
        :param key: key name
        :param noExpire: if True, return data regardless of expiration date
        :return: cached data if any, None o/w
        """
        try:
            now = mktime(gmtime())
            content = super()._getOne(key=key)
            if content:
                if noExpire or content["expire"] > now:
                    if "data" in content:
                        self.logger.info("Cache hit %s", key)
                        return content["data"]
                    return self._getFromFile(key)
                else:
                    self.logger.info("Expired doc %s", key)
            else:
                self.logger.info("Cache miss %s", key)
            return None

        except Exception as error:
            self.logger.error("Failed to get %s from cache", key)
            self.logger.error(str(error))

    def purge(self, expiredDays: int = 2) -> None:
        """
        The function to delete all cached data if expired
        :param expiredDays: passed days from expiration time so that data can be deleted
        """
        try:
            super()._purge("expire", expiredDays)

        except Exception as error:
            self.logger.error("Failed to purge cache info expired for more than %s days", expiredDays)
            self.logger.error(str(error))
