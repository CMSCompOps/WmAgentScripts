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


class MockMongoClient(MongoClient):
    def _setMongoCollection(self) -> Collection:
        return self.client.unified.cacheInfo

    def _buildMongoDocument(self) -> None:
        pass

class CacheManagerTest(unittest.TestCase):
    mongoSettings = {"database": "unified", "collection": "cacheInfo"}

    # The data in cache is always changing.
    # For now, test the get methods with a fake key and real document values got randomly from mongo.
    mockMongoClient = MockMongoClient()
    params = {
        "key": "test/key",
        "cacheFilePath": "/data/unified-cache//test_key",
        "fileData": {"test": "data"},
        "docWithData": mockMongoClient._getOne(data={"$exists": True}),
        "docWithoutData": mockMongoClient._getOne(data={"$exists": False}),
        "docKeys": ["key", "time", "expire", "lifetime"],
    }

    def setUp(self) -> None:
        self.cache = CacheManager()
        super().setUp()
        return

    def tearDown(self) -> None:
        super().tearDown()
        return

    def testMongoSettings(self):
        """MongoClient gets the connection to MongoDB"""
        isCollection = isinstance(self.cache.collection, Collection)
        self.assertTrue(isCollection)

        rightName = self.cache.collection.database.name == self.mongoSettings.get("database")
        self.assertTrue(rightName)

        rightName = self.cache.collection.name == self.mongoSettings.get("collection")
        self.assertTrue(rightName)

    def testGetKeyFilePath(self):
        """_getKeyFilePath gets the cache file path"""
        path = self.cache._getKeyFilePath(self.params.get("key"))
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
        result = self.cache.get(self.params.get("key"))
        isNone = result is None
        self.assertTrue(isNone)

        # Test when key exists and data exists in Mongo document
        mockDocument = self.params.get("docWithData")
        hasAllKeys = all(k in mockDocument for k in self.params.get("docKeys"))
        self.assertTrue(hasAllKeys)

        mockGetOne.return_value = mockDocument
        result = self.cache.get(self.params.get("key"), noExpire=True)
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        # Test when key exists but data does not exists in Mongo document
        mockDocument = self.params.get("docWithoutData")
        hasAllKeys = all(k in mockDocument for k in self.params.get("docKeys"))
        self.assertTrue(hasAllKeys)

        mockGetOne.return_value = mockDocument
        mockGetFromFile.return_value = self.params.get("fileData")
        result = self.cache.get(self.params.get("key"), noExpire=True)
        isDict = isinstance(result, dict)
        self.assertTrue(isDict)

        fromFile = result == self.params.get("fileData")
        self.assertTrue(fromFile)


if __name__ == "__main__":
    unittest.main()
