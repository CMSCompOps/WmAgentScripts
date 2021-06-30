# TODO: this should be in Services or Utils?

import os
import time
import logging
import pymongo
import json
from abc import ABC, abstractmethod

from typing import Optional

from Utils.Authenticate import mongoClient
from Utils.ConfigurationHandler import ConfigurationHandler


class MongoInfo(ABC):
    """
    _MongoInfo_
    General abstract class for talking to Mongo collections
    """

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
    ):
        self.client = mongoClient()
        self.db = self._set_db()
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def _set_db(self):
        pass


class CacheInfo(MongoInfo):
    """
    _CacheInfo_
    Class for interaction with CacheInfo in Mongo
    """

    def _set_db(self):
        # TODO: what type does this function return ?
        return self.client.unified.cacheInfo

    def _getFromFile(self, key: str):
        # TODO: what type does this function return ?
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
            self.logger.info(f"File cache miss {key}")
            return None

    def _getFileKey(self, key: str) -> str:
        # TODO: rename ?
        """
        The function to get the file path for a given key
        :param key: key
        :return: file path
        """
        cacheDir = ConfigurationHandler().get("cache_dir")
        return f"{cacheDir}/{key.replace('/','_')}"

    def get(self, key: str, noExpire: bool = False):
        # TODO: what type does this function return ?
        """
        The function to get data from mongo cache info given a key
        :param key: key name
        :param noExpire: if True, return data regardless of expire date
        :return: (????)
        """
        try:
            now = time.mktime(time.gmtime())
            foundKey = self.db.find_one({"key": key})
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
            self.logger.error(f"Failed to get {key} from cache")
            self.logger.error(f"{str(error)}")

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
            self.db.update_one({"key": key}, {"$set": content}, upsert=True)
            return True

        except (pymongo.errors.WriteError, pymongo.errors.DocumentTooLarge) as error:
            self.logger.error("Failed writing in CacheInfo, will use file instead")
            # TODO: what is actually done here ?
            with open(self._getFileKey(key), "w") as file:
                file.write(json.dumps(content.pop("data")))
            self.db.update_one({"key": key}, {"$set": content}, upsert=True)
            return True

        except Exception as error:
            self.logger.error(f"Failed to store {key} in CacheInfo")
            self.logger.error(f"{str(error)}")

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
            self.db.delete_many({"expire": {"$lt": expireLimit}})
            return True
        except Exception as error:
            self.logger.error("Failed to purge data from cache")
            self.logger.error(f"{str(error)}")
        return False
