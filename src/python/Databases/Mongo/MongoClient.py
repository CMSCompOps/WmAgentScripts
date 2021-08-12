from time import struct_time, gmtime, mktime
import logging
from logging import Logger
import pymongo
from pymongo.collection import Collection

from abc import ABC, abstractmethod

from typing import Optional, Union

from Utilities.ConfigurationHandler import ConfigurationHandler


class MongoClient(ABC):
    """
    __MongoClient__
    General Abstract Base Class for APIs relying on a MongoDB collection
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            logging.basicConfig(level=logging.INFO)
            self.logger = logger or logging.getLogger(self.__class__.__name__)

            configurationHandler = ConfigurationHandler()
            self.mongoUrl = configurationHandler.get("mongo_db_url")
            self.client = pymongo.MongoClient(f"mongodb://{self.mongoUrl}/?ssl=true", tlsAllowInvalidCertificates=True)
            self.collection = self._setMongoCollection()

        except Exception as error:
            raise Exception(f"Error initializing MongoClient\n{str(error)}")

    @abstractmethod
    def _setMongoCollection(self) -> Collection:
        """
        The function to set the mongo collection of the class
        """
        pass

    @abstractmethod
    def _buildMongoDocument(self, *documentArgs) -> dict:
        """
        The function to build a collection document from given params
        """
        pass

    def _set(self, *documentArgs, **query) -> None:
        """
        The function to set new documents in the collection
        :param documentArgs: args to build the document
        :param query: optional query param
        """
        document = self._buildMongoDocument(*documentArgs)
        if query:
            self.collection.update_one(query, {"$set": document}, upsert=True)
        else:
            self.collection.insert_one(document)

    def _get(self, key: str, details: bool = False, **query) -> Union[dict, list]:
        """
        The function to get the collection content
        :param key: key name
        :param details: if True return doc details, o/w only a list of key values
        :param query: optional query params
        :return: collection content
        """
        if details:
            return dict((doc[key], doc) for doc in self.collection.find(query) if key in doc)
        return [doc[key] for doc in self.collection.find(query) if key in doc]

    def _getOne(self, dropParams: list = [], **query) -> Optional[dict]:
        """
        The function to get one collection document for a given query
        :param dropParams: params to drop from the document
        :param query: query params
        :return: collection content if any, None o/w
        """
        content = self.collection.find_one(query)
        if content:
            for k in dropParams:
                content.pop(k, None)
            return content
        return None

    def _pop(self, **query) -> None:
        """
        The function to delete one document from the collection
        :param query: query params
        """
        self.collection.delete_one(query)

    def _clean(self, **query) -> None:
        """
        The function to delete many documents from the collection
        :param query: query params
        """
        count = self.collection.delete_many(query)
        self.logger.info("%s documents deleted", count)

    def _purge(self, key: str, expiredDays: int, now: struct_time = gmtime()) -> None:
        """
        The function to delete all documents from collection if it is expired
        :param key: expiration time key name
        :param expiredDays: passed days from expiration time so that data can be deleted
        :param now: time now
        """
        expiredLimit = mktime(now) - expiredDays * 86400
        count = self.collection.delete_many({key: {"$lt": expiredLimit}})
        self.logger.info("%s documents deleted", count)
