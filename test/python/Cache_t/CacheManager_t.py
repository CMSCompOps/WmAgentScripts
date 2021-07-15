#!/usr/bin/env python
"""
_CacheManager_t_
Unit test for CacheManager helper class.
"""

import unittest
from unittest.mock import patch
from pymongo.collection import Collection

from typing import Optional

from Cache.CacheManager import CacheManager
from Services.Mongo.MongoClient import MongoClient


class mockMongoClient(MongoClient):
    def _setMongoCollection(self) -> Collection:
        return self.client.unified.cacheInfo

    def _buildMongoDocument(self, *documentArgs) -> dict:
        pass

    def _getOne(self, dropParams: list = [], **query) -> Optional[dict]:
        return super()._getOne(dropParams, **query)


class CacheManagerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "cacheInfo"}

    # The data in cache is always changing.
    # For now, test the get methods with a fake key and document values mocked with real random ones.
    params = {"key": "test/key", "cacheFilePath": "/data/unified-cache//test_key"}

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        cache = CacheManager()
        isCollection = isinstance(cache.collection, Collection)
        self.assertTrue(isCollection)

        rightName = cache.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = cache.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGetKeyFilePath(self):
        """_getKeyFilePath gets the cache file path"""
        cache = CacheManager()
        path = cache._getKeyFilePath(self.params.get("key"))
        isStr = isinstance(path, str)
        self.assertTrue(isStr)

        rightPath = path == self.params.get("cacheFilePath")
        self.assertTrue(rightPath)

    @patch("Cache.CacheManager.CacheManager._getFromFile")
    @patch("Services.Mongo.MongoClient.MongoClient._getOne")
    def testGet(self, mockGetOne, mockGetFromFile):
        """get gets the data in cache for a given key"""
        # Test when key does not exist
        mockGetOne.return_value = None
        cache = CacheManager()
        result = cache.get(self.params.get("key"))
        isNone = result is None
        self.assertTrue(isNone)

        # Test when key exists and data exists in Mongo document
        mockDocument = mockMongoClient()._getOne(data={"$exists": True})
        mockGetOne.return_value = mockDocument
        cache = CacheManager()
        result = cache.get(self.params.get("key"), noExpire=True)
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        # Test when key exists but data does not exists in Mongo document
        mockDocument = mockMongoClient()._getOne(data={"$exists": False})
        mockGetOne.return_value = mockDocument
        mockFile = {"test": "data"}
        mockGetFromFile.return_value = mockFile
        cache = CacheManager()
        result = cache.get(self.params.get("key"), noExpire=True)
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        fromFile = result == mockFile
        self.assertTrue(fromFile)
