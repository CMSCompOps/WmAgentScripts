import os
import time
import pymongo
import json
import logging
from logging import Logger

from typing import Optional

from Utils.Authenticate import mongoClient
from Utils.ConfigurationHandler import ConfigurationHandler


class CacheInfo:
    """
    _CacheInfo_
    For reading/writing collection cacheInfo in MongoDB
    """

    def __init__(self, logger: Optional[Logger] = None):
        configurationHandler = ConfigurationHandler()
        self.mongoUrl = configurationHandler.get("mongo_db_url")
        self.cacheDir = configurationHandler.get("cache_dir")
        self.client = mongoClient(self.mongoUrl)
        self.collection = self.client.unified.cacheInfo
        logging.basicConfig(level=logging.INFO)
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def _getFromFile(self, key: str):
        """
        The function to get key data from file
        :param key: key
        :return: file data
        """
        fileKey = self._getFileKey(key)
        if os.path.isfile(fileKey):
            self.logger.info(f"File cache hit {key}")
            with open(fileKey) as file:
                data = json.loads(file.read())
            return data

        else:
            self.logger.info("File cache miss %s", key)
            return None

    def _getFileKey(self, key: str) -> str:
        """
        The function to get the file path for a given key
        :param key: key
        :return: file path
        """
        return f"{self.cacheDir}/{key.replace('/','_')}"

    def get(self, key: str, noExpire: bool = False):
        """
        The function to get data from mongo cache info given a key
        :param key: key name
        :param noExpire: if True, return data regardless of expire date
        :return: (????)
        """
        try:
            now = time.mktime(time.gmtime())
            foundKey = self.collection.find_one({"key": key})
            if foundKey:
                if noExpire or foundKey["expire"] > now:
                    if "data" not in foundKey:
                        return self._getFromFile(key)
                    else:
                        self.logger.info(f"Cache hit {key}")
                        return foundKey["data"]
                else:
                    self.logger.info(f"Expired doc {key}")
                    return None
            else:
                self.logger.info(f"Cache miss {key}")
                return None

        except Exception as error:
            self.logger.error("Failed to get %s from cache", key)
            self.logger.error(str(error))

    def store(self, key: str, data: dict, lifeTimeMinutes: int = 10) -> bool:
        """
        The function to store data in mongo CacheInfo
        :param key: key
        :param data: data
        :param lifeTimeMinutes: minutes to expire data
        :return: True if succeeded, False o/w
        """
        now = time.mktime(time.gmtime())
        content = {
            "data": data,
            "key": key,
            "time": int(now),
            "expire": int(now + 60 * lifeTimeMinutes),
            "lifetime": lifeTimeMinutes,
        }

        try:
            self.collection.update_one({"key": key}, {"$set": content}, upsert=True)
            return True

        except (pymongo.errors.WriteError, pymongo.errors.DocumentTooLarge) as error:
            self.logger.error("Failed writing in CacheInfo, will use file instead")
            with open(self._getFileKey(key), "w") as file:
                file.write(json.dumps(content.pop("data")))
            self.collection.update_one({"key": key}, {"$set": content}, upsert=True)
            return True

        except Exception as error:
            self.logger.error("Failed to store %s in CacheInfo", key)
            self.logger.error(str(error))

        return False

    def purge(self, expiredDays: int = 2) -> bool:
        """
        The function to all delete data from mongo CacheInfo if expired
        :param expiredDays: passed days from expiration time so that data can be deleted
        :param return: True if succeeded, False o/w
        """
        try:
            now = time.mktime(time.gmtime())
            expireLimit = now - expiredDays * 86400
            self.collection.delete_many({"expire": {"$lt": expireLimit}})
            return True

        except Exception as error:
            self.logger.error("Failed to purge data from cache")
            self.logger.error(str(error))
            return False
