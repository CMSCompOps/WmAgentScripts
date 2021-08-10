import random
from logging import Logger
from abc import ABC, abstractmethod

from Utilities.Logging import getLogger

from typing import Optional, Any


class BaseDocumentCache(ABC):
    """
    __BaseDocumentCache__
    General Abstract Base Class for building caching documents
    """

    def __init__(self, defaultValue: Any = {}, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

            self.defaultValue = defaultValue
            self.lifeTimeMinutes = int(20 + random.random() * 10)

        except Exception as error:
            raise Exception(f"Error initializing BaseDocumentCache\n{str(error)}")

    @abstractmethod
    def get(self) -> Any:
        """
        The function to get the cached data
        :return: cached data
        """
        pass
