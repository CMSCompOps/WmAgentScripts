import os
import time
import pymongo
import json
import logging
from logging import Logger

from typing import Optional

from Utils.ConfigurationHandler import ConfigurationHandler


class CacheManager:
    """
    _CacheManager_
    General API for managing the cache info
    """

    def __init__(self, logger: Optional[Logger] = None):
        pass

    def get(self, key: str, noExpire: bool = False):
        pass

    def set(self, key: str, data: dict, lifeTimeMinutes: int = 10) -> bool:
        pass
